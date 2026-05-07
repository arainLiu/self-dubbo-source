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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.Method;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FILTER_VALIDATION_EXCEPTION;

/**
 * ExceptionInvokerFilter
 * <p>
 * Functions:
 * <ol>
 * <li>unexpected exception will be logged in ERROR level on provider side. Unexpected exception are unchecked
 * exception not declared on the interface</li>
 * <li>Wrap the exception not introduced in API package into RuntimeException. Framework will serialize the outer exception but stringnize its cause in order to avoid of possible serialization problem on client side</li>
 * </ol>
 */
@Activate(group = CommonConstants.PROVIDER)
public class ExceptionFilter implements Filter, Filter.Listener {
    private ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ExceptionFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    /**
     * 处理服务端响应中的异常，根据异常类型决定是否包装为RuntimeException
     * 目的是避免将非预期的检查型异常或第三方库异常直接抛给客户端，导致客户端反序列化失败
     *
     * @param appResponse RPC调用的应用层响应结果，可能包含异常信息
     * @param invoker 调用器对象，用于获取服务接口类型和URL信息
     * @param invocation 调用上下文对象，包含方法名和参数类型等信息
     */
    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 仅当响应中存在异常且目标服务不是GenericService时执行过滤逻辑
        if (appResponse.hasException() && GenericService.class != invoker.getInterface()) {
            try {
                Throwable exception = appResponse.getException();

                // 如果是检查型异常（非RuntimeException），则直接返回，允许其传递给客户端
                if (!(exception instanceof RuntimeException) && (exception instanceof Exception)) {
                    return;
                }

                // 如果异常已在方法签名的throws子句中声明，则直接返回
                try {
                    Method method = invoker.getInterface()
                            .getMethod(RpcUtils.getMethodName(invocation), invocation.getParameterTypes());
                    Class<?>[] exceptionClasses = method.getExceptionTypes();
                    for (Class<?> exceptionClass : exceptionClasses) {
                        if (exception.getClass().equals(exceptionClass)) {
                            return;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    // 如果找不到对应的方法，则跳过过滤逻辑
                    return;
                }

                // 对于未在方法签名中声明的非检查型异常，记录ERROR级别的日志
                logger.error(
                        CONFIG_FILTER_VALIDATION_EXCEPTION,
                        "",
                        "",
                        "Got unchecked and undeclared exception which called by "
                                + RpcContext.getServiceContext().getRemoteHost() + ". service: "
                                + invoker.getInterface().getName() + ", method: " + RpcUtils.getMethodName(invocation)
                                + ", exception: "
                                + exception.getClass().getName() + ": " + exception.getMessage(),
                        exception);

                // 如果异常类与服务接口类在同一个JAR包中，说明是服务内部定义的异常，直接返回
                String serviceFile = ReflectUtils.getCodeBase(invoker.getInterface());
                String exceptionFile = ReflectUtils.getCodeBase(exception.getClass());
                if (serviceFile == null || exceptionFile == null || serviceFile.equals(exceptionFile)) {
                    return;
                }

                // 如果是JDK自带的异常（java/javax/jakarta开头），直接返回
                String className = exception.getClass().getName();
                if (className.startsWith("java.")
                        || className.startsWith("javax.")
                        || className.startsWith("jakarta.")) {
                    return;
                }

                // 如果是Dubbo框架自身的异常，直接返回
                if (exception instanceof RpcException) {
                    return;
                }

                // 对于其他所有未预期的异常，包装为RuntimeException并转换为字符串形式，防止客户端反序列化失败
                appResponse.setException(new RuntimeException(StringUtils.toString(exception)));
            } catch (Throwable e) {
                // 捕获过滤器自身处理过程中的异常，避免影响主业务流程
                logger.warn(
                        CONFIG_FILTER_VALIDATION_EXCEPTION,
                        "",
                        "",
                        "Fail to ExceptionFilter when called by "
                                + RpcContext.getServiceContext().getRemoteHost() + ". service: "
                                + invoker.getInterface().getName() + ", method: " + RpcUtils.getMethodName(invocation)
                                + ", exception: "
                                + e.getClass().getName() + ": " + e.getMessage(),
                        e);
            }
        }
    }


    /**
     * 处理RPC调用过程中的异常事件
     * 当调用链中出现未被捕获的异常时，记录包含调用方、服务接口、方法及异常详情的错误日志
     *
     * @param e 捕获到的异常对象
     * @param invoker 调用器对象，用于获取服务接口名称等元数据
     * @param invocation 调用上下文对象，包含被调用的方法名等信息
     */
    @Override
    public void onError(Throwable e, Invoker<?> invoker, Invocation invocation) {
        // 记录未检查且未声明的异常信息，便于服务端排查问题
        logger.error(
                CONFIG_FILTER_VALIDATION_EXCEPTION,
                "",
                "",
                "Got unchecked and undeclared exception which called by "
                        + RpcContext.getServiceContext().getRemoteHost() + ". service: "
                        + invoker.getInterface().getName() + ", method: " + RpcUtils.getMethodName(invocation)
                        + ", exception: "
                        + e.getClass().getName() + ": " + e.getMessage(),
                e);
    }

    // For test purpose
    @DisableInject
    public void mockLogger(ErrorTypeAwareLogger logger) {
        this.logger = logger;
    }
}
