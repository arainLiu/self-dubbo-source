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
package org.apache.dubbo.rpc.proxy.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericService;

import java.lang.reflect.Constructor;

import static org.apache.dubbo.common.constants.CommonConstants.STUB_EVENT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED_EXPORT_SERVICE;
import static org.apache.dubbo.rpc.Constants.DEFAULT_STUB_EVENT;
import static org.apache.dubbo.rpc.Constants.IS_SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_METHODS_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_KEY;

public class StubProxyFactoryWrapper implements ProxyFactory {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(StubProxyFactoryWrapper.class);

    private final ProxyFactory proxyFactory;

    private Protocol protocol;

    public StubProxyFactoryWrapper(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 获取服务代理对象，并根据配置包装本地存根（Stub/Local）实现。
     * <p>
     * 该方法在创建标准代理后，检查是否配置了本地存根类。如果配置了，则加载存根类并实例化，
     * 将原始代理作为构造函数参数传入，从而实现对远程调用的本地增强（如参数校验、缓存、异常处理等）。
     * 此外，如果启用了存根事件功能，还会将存根服务作为一个本地服务导出，以便接收来自服务端的回调。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>调用底层proxyFactory.getProxy()创建标准的远程服务代理</li>
     *   <li>如果不是GenericService泛化调用场景，则尝试处理存根逻辑：
     *     <ul>
     *       <li>从URL中获取stub或local配置项</li>
     *       <li>如果配置为true或default，则根据约定生成存根类名（接口名+Stub 或 接口名+Local）</li>
     *       <li>反射加载存根类，并验证其是否实现了服务接口</li>
     *       <li>查找并接受一个以服务接口为参数的构造函数，实例化存根类并将原始代理注入</li>
     *       <li>如果启用了STUB_EVENT（存根事件），则构建事件回调URL，包含所有声明的方法名，并设置is_server=false</li>
     *       <li>调用export()方法导出存根服务，使其能够接收服务端的事件通知</li>
     *     </ul>
     *   </li>
     *   <li>如果在存根处理过程中发生异常，记录错误日志但不影响主流程，直接返回原始代理</li>
     * </ol>
     * </p>
     *
     * @param invoker 服务调用的Invoker对象，封装了远程调用逻辑
     * @param generic 是否为泛化调用模式
     * @return 经过存根包装后的代理对象，或者在未配置存根时返回原始代理
     * @throws RpcException 当代理创建或存根处理发生严重错误时抛出
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        T proxy = proxyFactory.getProxy(invoker, generic);
        if (GenericService.class != invoker.getInterface()) {
            URL url = invoker.getUrl();
            String stub = url.getParameter(STUB_KEY, url.getParameter(LOCAL_KEY));
            if (ConfigUtils.isNotEmpty(stub)) {
                Class<?> serviceType = invoker.getInterface();
                /*
                 * 如果配置为默认值，则根据约定生成存根类名
                 */
                if (ConfigUtils.isDefault(stub)) {
                    if (url.hasParameter(STUB_KEY)) {
                        stub = serviceType.getName() + "Stub";
                    } else {
                        stub = serviceType.getName() + "Local";
                    }
                }
                try {
                    Class<?> stubClass = ReflectUtils.forName(stub);
                    if (!serviceType.isAssignableFrom(stubClass)) {
                        throw new IllegalStateException("The stub implementation class " + stubClass.getName()
                                + " not implement interface " + serviceType.getName());
                    }
                    try {
                        /*
                         * 通过构造函数实例化存根类，并将原始代理注入
                         */
                        Constructor<?> constructor = ReflectUtils.findConstructor(stubClass, serviceType);
                        proxy = (T) constructor.newInstance(new Object[] {proxy});
                        // export stub service
                        /*
                         * 如果启用了存根事件功能，则导出存根服务以接收服务端回调
                         */
                        URLBuilder urlBuilder = URLBuilder.from(url);
                        if (url.getParameter(STUB_EVENT_KEY, DEFAULT_STUB_EVENT)) {
                            urlBuilder.addParameter(
                                    STUB_EVENT_METHODS_KEY,
                                    StringUtils.join(
                                            Wrapper.getWrapper(proxy.getClass()).getDeclaredMethodNames(), ","));
                            urlBuilder.addParameter(IS_SERVER_KEY, Boolean.FALSE.toString());
                            try {
                                export(proxy, invoker.getInterface(), urlBuilder.build());
                            } catch (Exception e) {
                                LOGGER.error(PROXY_FAILED_EXPORT_SERVICE, "", "", "export a stub service error.", e);
                            }
                        }
                    } catch (NoSuchMethodException e) {
                        throw new IllegalStateException(
                                "No such constructor \"public " + stubClass.getSimpleName() + "("
                                        + serviceType.getName() + ")\" in stub implementation class "
                                        + stubClass.getName(),
                                e);
                    }
                } catch (Throwable t) {
                    LOGGER.error(
                            PROXY_FAILED_EXPORT_SERVICE,
                            "",
                            "",
                            "Failed to create stub implementation class " + stub + " in consumer "
                                    + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion()
                                    + ", cause: " + t.getMessage(),
                            t);
                    // ignore
                }
            }
        }
        return proxy;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    @Override
    public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException {
        return proxyFactory.getInvoker(proxy, type, url);
    }

    private <T> Exporter<T> export(T instance, Class<T> type, URL url) {
        return protocol.export(proxyFactory.getInvoker(instance, type, url));
    }
}
