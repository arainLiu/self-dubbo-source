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
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.profiler.Profiler;
import org.apache.dubbo.common.profiler.ProfilerEntry;
import org.apache.dubbo.common.profiler.ProfilerSwitch;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncContextImpl;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_ASYNC_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_ERROR_ASYNC_RESPONSE;

/**
 * This Invoker works on provider side, delegates RPC to interface implementation.
 */
public abstract class AbstractProxyInvoker<T> implements Invoker<T> {
    ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(AbstractProxyInvoker.class);

    private final T proxy;

    private final Class<T> type;

    private final URL url;

    public AbstractProxyInvoker(T proxy, Class<T> type, URL url) {
        if (proxy == null) {
            throw new IllegalArgumentException("proxy == null");
        }
        if (type == null) {
            throw new IllegalArgumentException("interface == null");
        }
        if (!type.isInstance(proxy)) {
            throw new IllegalArgumentException(proxy.getClass().getName() + " not implement interface " + type);
        }
        this.proxy = proxy;
        this.type = type;
        this.url = url;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {}

    /**
     * 执行远程代理方法的调用，将Invocation转换为本地方法调用并处理异步结果
     * 集成性能监控逻辑，并对同步/异步异常进行统一封装
     *
     * @param invocation 调用上下文对象，包含方法名、参数类型、参数值及监控上下文
     * @return RPC调用结果，通常为AsyncRpcResult以支持异步编程模型
     * @throws RpcException 当远程代理方法调用失败或发生系统级错误时抛出
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        ProfilerEntry originEntry = null;
        try {
            // 如果开启了简易性能监控，则从invocation中提取并推进监控埋点
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                Object fromInvocation = invocation.get(Profiler.PROFILER_KEY);
                if (fromInvocation instanceof ProfilerEntry) {
                    ProfilerEntry profiler = Profiler.enter(
                            (ProfilerEntry) fromInvocation, "Receive request. Server biz impl invoke begin.");
                    invocation.put(Profiler.PROFILER_KEY, profiler);
                    originEntry = Profiler.setToBizProfiler(profiler);
                }
            }

            // 执行具体的代理方法调用（由子类实现），获取原始返回值
            Object value = doInvoke(
                    proxy, invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments());

            // 将原始返回值包装为CompletableFuture，并通过handle处理同步或异步产生的异常
            CompletableFuture<Object> future = wrapWithFuture(value, invocation);
            CompletableFuture<AppResponse> appResponseFuture = future.handle((obj, t) -> {
                AppResponse result = new AppResponse(invocation);
                if (t != null) {
                    // 如果执行过程中抛出异常，解包CompletionException并设置到结果中
                    if (t instanceof CompletionException) {
                        result.setException(t.getCause());
                    } else {
                        result.setException(t);
                    }
                } else {
                    // 正常完成，设置业务返回值
                    result.setValue(obj);
                }
                return result;
            });
            return new AsyncRpcResult(appResponseFuture, invocation);
        } catch (InvocationTargetException e) {
            // 处理目标方法执行期间抛出的检查型异常
            if (RpcContext.getServiceContext().isAsyncStarted()
                    && !RpcContext.getServiceContext().stopAsync()) {
                logger.error(
                        PROXY_ERROR_ASYNC_RESPONSE,
                        "",
                        "",
                        "Provider async started, but got an exception from the original method, cannot write the exception back to consumer because an async result may have returned the new thread.",
                        e);
            }
            return AsyncRpcResult.newDefaultAsyncResult(null, e.getTargetException(), invocation);
        } catch (Throwable e) {
            // 捕获其他所有异常（如反射调用失败、参数不匹配等），包装为RpcException抛出
            throw new RpcException(
                    "Failed to invoke remote proxy method " + invocation.getMethodName() + " to " + getUrl()
                            + ", cause: " + e.getMessage(),
                    e);
        } finally {
            // 结束性能监控埋点，并恢复之前的监控上下文状态
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                Object fromInvocation = invocation.get(Profiler.PROFILER_KEY);
                if (fromInvocation instanceof ProfilerEntry) {
                    ProfilerEntry profiler = Profiler.release((ProfilerEntry) fromInvocation);
                    invocation.put(Profiler.PROFILER_KEY, profiler);
                }
            }
            Profiler.removeBizProfiler();
            if (originEntry != null) {
                Profiler.setToBizProfiler(originEntry);
            }
        }
    }

    private CompletableFuture<Object> wrapWithFuture(Object value, Invocation invocation) {
        if (value instanceof CompletableFuture) {
            invocation.put(PROVIDER_ASYNC_KEY, Boolean.TRUE);
            return (CompletableFuture<Object>) value;
        } else if (RpcContext.getServerAttachment().isAsyncStarted()) {
            invocation.put(PROVIDER_ASYNC_KEY, Boolean.TRUE);
            return ((AsyncContextImpl) (RpcContext.getServerAttachment().getAsyncContext())).getInternalFuture();
        }
        return CompletableFuture.completedFuture(value);
    }

    protected abstract Object doInvoke(T proxy, String methodName, Class<?>[] parameterTypes, Object[] arguments)
            throws Throwable;

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? " " : getUrl().toString());
    }
}
