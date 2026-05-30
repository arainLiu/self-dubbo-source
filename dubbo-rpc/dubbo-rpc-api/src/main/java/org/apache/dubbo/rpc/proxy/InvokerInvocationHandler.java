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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ServiceModel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Dubbo RPC代理调用处理器，负责将JDK动态代理的方法调用转换为RPC Invocation并执行
 * 拦截业务接口方法的调用，构建RpcInvocation上下文，并通过Invoker链发起远程调用
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);

    /** 底层RPC调用器，代表远程服务的代理入口 */
    private final Invoker<?> invoker;

    /** 服务模型，包含消费者端的配置信息和元数据 */
    private final ServiceModel serviceModel;

    /** 协议服务键，用于唯一标识一个服务（接口+版本+分组+协议） */
    private final String protocolServiceKey;

    /**
     * 构造函数，初始化调用处理器
     *
     * @param handler 底层RPC调用器，通常由ProxyFactory创建
     */
    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
        URL url = invoker.getUrl();
        this.protocolServiceKey = url.getProtocolServiceKey();
        this.serviceModel = url.getServiceModel();
    }

    /**
     * 处理代理对象的方法调用，将本地方法调用转换为RPC远程调用
     * 对于Object类的基础方法（如toString、hashCode等）直接在本地执行，不触发RPC
     *
     * @param proxy 被代理的目标对象
     * @param method 当前被调用的方法对象
     * @param args 方法调用参数数组
     * @return 方法执行的返回值，如果是void方法则返回null
     * @throws Throwable 当RPC调用失败或反射执行异常时抛出
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 如果调用的是Object类定义的方法，则直接在invoker对象上反射执行，不走RPC流程
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }

        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();

        // 处理无参的特殊方法：toString、$destroy、hashCode
        if (parameterTypes.length == 0) {
            if ("toString".equals(methodName)) {
                return invoker.toString();
            } else if ("$destroy".equals(methodName)) {
                // 执行销毁逻辑，释放RPC资源
                invoker.destroy();
                return null;
            } else if ("hashCode".equals(methodName)) {
                return invoker.hashCode();
            }
        } else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
            // 处理equals方法，委托给invoker进行对象比较
            return invoker.equals(args[0]);
        }

        // 构建RPC调用上下文，封装方法名、参数类型、参数值及服务元数据
        RpcInvocation rpcInvocation = new RpcInvocation(
                serviceModel,
                method.getName(),
                invoker.getInterface().getName(),
                protocolServiceKey,
                method.getParameterTypes(),
                args);

        // 如果是消费者模型，则将ConsumerModel和MethodModel存入附件，供后续Filter或集群策略使用
        if (serviceModel instanceof ConsumerModel) {
            rpcInvocation.put(Constants.CONSUMER_MODEL, serviceModel);
            rpcInvocation.put(Constants.METHOD_MODEL, ((ConsumerModel) serviceModel).getMethodModel(method));
        }

        // 通过InvocationUtil执行最终的RPC调用链路
        return InvocationUtil.invoke(invoker, rpcInvocation);
    }
}
