/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.compact.Dubbo2CompactUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.rpc.service.Destroyable;
import org.apache.dubbo.rpc.service.EchoService;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_UNSUPPORTED_INVOKER;
import static org.apache.dubbo.rpc.Constants.INTERFACES;

public abstract class AbstractProxyFactory implements ProxyFactory {
    private static final Class<?>[] INTERNAL_INTERFACES = new Class<?>[] {EchoService.class, Destroyable.class};

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(AbstractProxyFactory.class);

    @Override
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    /**
     * 创建服务代理对象，组装所有需要实现的接口并处理泛化调用场景。
     * <p>
     * 该方法负责构建代理对象所需实现的接口集合，包括用户自定义接口、内部框架接口以及泛化调用所需的GenericService接口。
     * 支持从URL配置中读取额外的接口列表，并在GraalVM Native Image环境下保持接口顺序的稳定性（使用LinkedHashSet）。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>创建LinkedHashSet用于存储代理需要实现的所有接口，保证插入顺序不变</li>
     *   <li>获取Invoker的类加载器，用于后续反射加载接口类</li>
     *   <li>从URL的INTERFACES参数中解析用户配置的额外接口列表，逐个加载并添加到interfaces集合中，加载失败的接口会被忽略</li>
     *   <li>如果是泛化调用模式（generic=true）：
     *     <ul>
     *       <li>尝试从URL的INTERFACE参数中获取真实的业务接口类并添加到集合中</li>
     *       <li>如果Invoker接口是GenericService且Dubbo2兼容模式启用，则添加Dubbo2的GenericService接口</li>
     *       <li>如果Invoker接口不是GenericService，则根据兼容模式状态选择添加Dubbo2或标准版的GenericService接口</li>
     *     </ul>
     *   </li>
     *   <li>添加Invoker的主接口和INTERNAL_INTERFACES数组中的所有内部接口（如EchoService等）</li>
     *   <li>调用子类实现的getProxy()模板方法创建具体的代理对象</li>
     *   <li>异常处理：
     *     <ul>
     *       <li>如果创建代理失败且处于泛化模式，尝试移除真实接口类和Invoker主接口，重新创建仅包含基础接口的代理，提高兼容性</li>
     *       <li>如果不是泛化模式，直接抛出原始异常</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param invoker 服务调用的Invoker对象，包含服务接口、URL配置等信息
     * @param generic 是否为泛化调用模式，true表示创建泛化代理（无需接口类），false表示创建标准代理
     * @return 创建的服务代理对象，实现了所有指定的接口
     * @throws RpcException 当非泛化模式下创建代理失败时抛出
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        // when compiling with native image, ensure that the order of the interfaces remains unchanged
        LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();
        ClassLoader classLoader = getClassLoader(invoker);

        String config = invoker.getUrl().getParameter(INTERFACES);
        if (StringUtils.isNotEmpty(config)) {
            /*
             * 从URL配置中加载用户指定的额外接口列表
             */
            String[] types = COMMA_SPLIT_PATTERN.split(config);
            for (String type : types) {
                try {
                    interfaces.add(ReflectUtils.forName(classLoader, type));
                } catch (Throwable e) {
                    // ignore
                }
            }
        }

        Class<?> realInterfaceClass = null;
        if (generic) {
            try {
                // find the real interface from url
                /*
                 * 泛化调用场景：尝试加载真实的业务接口类
                 */
                String realInterface = invoker.getUrl().getParameter(Constants.INTERFACE);
                realInterfaceClass = ReflectUtils.forName(classLoader, realInterface);
                interfaces.add(realInterfaceClass);
            } catch (Throwable e) {
                // ignore
            }

            /*
             * 根据Dubbo2兼容模式状态添加对应的GenericService接口
             */
            if (GenericService.class.isAssignableFrom(invoker.getInterface())
                    && Dubbo2CompactUtils.isEnabled()
                    && Dubbo2CompactUtils.isGenericServiceClassLoaded()) {
                interfaces.add(Dubbo2CompactUtils.getGenericServiceClass());
            }
            if (!GenericService.class.isAssignableFrom(invoker.getInterface())) {
                if (Dubbo2CompactUtils.isEnabled() && Dubbo2CompactUtils.isGenericServiceClassLoaded()) {
                    interfaces.add(Dubbo2CompactUtils.getGenericServiceClass());
                } else {
                    interfaces.add(org.apache.dubbo.rpc.service.GenericService.class);
                }
            }
        }

        /*
         * 添加Invoker的主接口和框架内部接口（如EchoService）
         */
        interfaces.add(invoker.getInterface());
        interfaces.addAll(Arrays.asList(INTERNAL_INTERFACES));

        try {
            return getProxy(invoker, interfaces.toArray(new Class<?>[0]));
        } catch (Throwable t) {
            if (generic) {
                /*
                 * 泛化模式下创建失败时，移除真实接口类和主接口，尝试使用最小接口集重新创建代理
                 */
                if (realInterfaceClass != null) {
                    interfaces.remove(realInterfaceClass);
                }
                interfaces.remove(invoker.getInterface());

                logger.error(
                        PROXY_UNSUPPORTED_INVOKER,
                        "",
                        "",
                        "Error occur when creating proxy. Invoker is in generic mode. Trying to create proxy without real interface class.",
                        t);
                return getProxy(invoker, interfaces.toArray(new Class<?>[0]));
            } else {
                throw t;
            }
        }
    }

    private <T> ClassLoader getClassLoader(Invoker<T> invoker) {
        ServiceModel serviceModel = invoker.getUrl().getServiceModel();
        ClassLoader classLoader = null;
        if (serviceModel != null && serviceModel.getInterfaceClassLoader() != null) {
            classLoader = serviceModel.getInterfaceClassLoader();
        }
        if (classLoader == null) {
            classLoader = ClassUtils.getClassLoader();
        }
        return classLoader;
    }

    public static Class<?>[] getInternalInterfaces() {
        return INTERNAL_INTERFACES.clone();
    }

    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);
}
