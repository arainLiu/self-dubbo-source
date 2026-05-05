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
package org.apache.dubbo.rpc.proxy.javassist;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.Proxy;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.proxy.AbstractProxyFactory;
import org.apache.dubbo.rpc.proxy.AbstractProxyInvoker;
import org.apache.dubbo.rpc.proxy.InvokerInvocationHandler;
import org.apache.dubbo.rpc.proxy.jdk.JdkProxyFactory;

import java.util.Arrays;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED;

/**
 * JavassistRpcProxyFactory
 */
public class JavassistProxyFactory extends AbstractProxyFactory {
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(JavassistProxyFactory.class);
    private final JdkProxyFactory jdkProxyFactory = new JdkProxyFactory();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Invoker<T> invoker, Class<?>[] interfaces) {
        try {
            return (T) Proxy.getProxy(interfaces).newInstance(new InvokerInvocationHandler(invoker));
        } catch (Throwable fromJavassist) {
            // try fall back to JDK proxy factory
            try {
                T proxy = jdkProxyFactory.getProxy(invoker, interfaces);
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy success. "
                                + "Interfaces: " + Arrays.toString(interfaces),
                        fromJavassist);
                return proxy;
            } catch (Throwable fromJdk) {
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + Arrays.toString(interfaces) + " Javassist Error.",
                        fromJavassist);
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + Arrays.toString(interfaces) + " JDK Error.",
                        fromJdk);
                throw fromJavassist;
            }
        }
    }

    /**
     * 获取服务代理的Invoker对象
     * <p>
     * 该方法负责将服务代理对象包装成Dubbo的Invoker，用于远程调用。主要执行以下操作：
     * 1. 尝试使用Javassist创建Wrapper包装器：
     *    - 如果类名不包含'$'（非动态代理类），直接使用proxy的Class
     *    - 如果类名包含'$'（动态代理类），使用接口类型Class
     * 2. 创建AbstractProxyInvoker匿名实现，委托给Wrapper执行方法调用
     * 3. 如果Javassist失败，降级使用JDK Proxy工厂作为后备方案
     * 4. 记录详细的错误日志，包括Javassist和JDK Proxy的失败信息
     * 5. 如果两种方案都失败，抛出原始异常
     * <p>
     * 该设计提供了双重保障机制，优先使用性能更好的Javassist字节码技术，
     * 在特殊场景下自动降级到JDK动态代理，确保服务的可用性。
     *
     * @param proxy 服务代理对象，实际的服务实现类实例
     * @param type 服务接口的Class对象
     * @param url 包含服务配置信息的URL对象
     * @param <T> 服务接口的泛型类型
     * @return 封装了服务代理的Invoker对象
     */
    @Override
    public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) {
        try {
            // TODO Wrapper cannot handle this scenario correctly: the classname contains '$'
            // 获取Wrapper包装器，处理普通类和动态代理类的不同情况
            final Wrapper wrapper =
                    Wrapper.getWrapper(proxy.getClass().getName().indexOf('$') < 0 ? proxy.getClass() : type);

            // 创建AbstractProxyInvoker，委托给Wrapper执行方法调用
            return new AbstractProxyInvoker<T>(proxy, type, url) {
                @Override
                protected Object doInvoke(T proxy, String methodName, Class<?>[] parameterTypes, Object[] arguments)
                        throws Throwable {
                    return wrapper.invokeMethod(proxy, methodName, parameterTypes, arguments);
                }
            };
        } catch (Throwable fromJavassist) {
            // try fall back to JDK proxy factory
            // Javassist失败，尝试降级使用JDK Proxy
            try {
                Invoker<T> invoker = jdkProxyFactory.getInvoker(proxy, type, url);
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate invoker by Javassist failed. Fallback to use JDK proxy success. "
                                + "Interfaces: " + type,
                        fromJavassist);
                // log out error
                return invoker;
            } catch (Throwable fromJdk) {
                // 两种方案都失败，记录详细错误日志并抛出异常
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate invoker by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + type + " Javassist Error.",
                        fromJavassist);
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate invoker by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + type + " JDK Error.",
                        fromJdk);
                throw fromJavassist;
            }
        }
    }

}
