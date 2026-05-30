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
package org.apache.dubbo.rpc.cluster.filter;

import org.apache.dubbo.common.Experimental;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvocationProfilerUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_EXECUTE_FILTER_EXCEPTION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.extension.ExtensionScope.APPLICATION;

/**
 * 过滤器链构建器接口，负责将多个过滤器（Filter）组装成调用链
 * 通过责任链模式实现对RPC调用的拦截、增强和监控功能
 */
@SPI(value = "default", scope = APPLICATION)
public interface FilterChainBuilder {
    /**
     * 构建消费者或服务提供者的过滤器调用链
     *
     * @param invoker 原始调用器对象
     * @param key 用于筛选过滤器的配置键
     * @param group 过滤器所属的分组（consumer或provider）
     * @return 包装了过滤器链的Invoker对象
     */
    <T> Invoker<T> buildInvokerChain(final Invoker<T> invoker, String key, String group);

    /**
     * 构建消费者端的集群过滤器调用链
     *
     * @param invoker 原始集群调用器对象
     * @param key 用于筛选过滤器的配置键
     * @param group 过滤器所属的分组
     * @return 包装了过滤器链的ClusterInvoker对象
     */
    <T> ClusterInvoker<T> buildClusterInvokerChain(final ClusterInvoker<T> invoker, String key, String group);

    /**
     * 过滤器链中的单个节点实现，工作在服务端（Provider）
     * 每个节点持有一个过滤器和下一个节点的引用，形成递归调用结构
     *
     * @param <T> 服务接口类型
     * @param <TYPE> 调用器类型
     * @param <FILTER> 过滤器类型
     */
    class FilterChainNode<T, TYPE extends Invoker<T>, FILTER extends BaseFilter> implements Invoker<T> {
        TYPE originalInvoker;
        /** 链表中的下一个节点 */
        Invoker<T> nextNode;
        /** 当前节点持有的过滤器实例 */
        FILTER filter;

        public FilterChainNode(TYPE originalInvoker, Invoker<T> nextNode, FILTER filter) {
            this.originalInvoker = originalInvoker;
            this.nextNode = nextNode;
            this.filter = filter;
        }

        public TYPE getOriginalInvoker() {
            return originalInvoker;
        }

        @Override
        public Class<T> getInterface() {
            return originalInvoker.getInterface();
        }

        @Override
        public URL getUrl() {
            return originalInvoker.getUrl();
        }

        @Override
        public boolean isAvailable() {
            return originalInvoker.isAvailable();
        }

        /**
         * 执行当前节点的过滤器逻辑，并异步监听后续链路的执行结果
         * 支持在调用前后触发监听器回调，并记录详细的性能监控信息
         *
         * @param invocation 调用上下文
         * @return 异步执行结果
         * @throws RpcException 当过滤器逻辑或远程调用发生异常时抛出
         */
        @Override
        public Result invoke(Invocation invocation) throws RpcException {
            Result asyncResult;
            try {
                // 进入详细性能监控埋点，记录当前过滤器的执行耗时
                InvocationProfilerUtils.enterDetailProfiler(
                        invocation, () -> "Filter " + filter.getClass().getName() + " invoke.");
                // 执行过滤器逻辑并传递给下一个节点
                asyncResult = filter.invoke(nextNode, invocation);
            } catch (Exception e) {
                // 发生同步异常时释放监控埋点并触发错误回调
                InvocationProfilerUtils.releaseDetailProfiler(invocation);
                if (filter instanceof ListenableFilter) {
                    ListenableFilter listenableFilter = ((ListenableFilter) filter);
                    try {
                        Filter.Listener listener = listenableFilter.listener(invocation);
                        if (listener != null) {
                            listener.onError(e, originalInvoker, invocation);
                        }
                    } finally {
                        listenableFilter.removeListener(invocation);
                    }
                } else if (filter instanceof FILTER.Listener) {
                    FILTER.Listener listener = (FILTER.Listener) filter;
                    listener.onError(e, originalInvoker, invocation);
                }
                throw e;
            } finally {

            }
            // 注册异步完成回调，在远程调用真正结束后执行后置逻辑
            return asyncResult.whenCompleteWithContext((r, t) -> {
                InvocationProfilerUtils.releaseDetailProfiler(invocation);
                if (filter instanceof ListenableFilter) {
                    ListenableFilter listenableFilter = ((ListenableFilter) filter);
                    Filter.Listener listener = listenableFilter.listener(invocation);
                    try {
                        if (listener != null) {
                            if (t == null) {
                                // 调用成功，触发 onResponse 回调
                                listener.onResponse(r, originalInvoker, invocation);
                            } else {
                                // 调用失败，触发 onError 回调
                                listener.onError(t, originalInvoker, invocation);
                            }
                        }
                    } finally {
                        listenableFilter.removeListener(invocation);
                    }
                } else if (filter instanceof FILTER.Listener) {
                    FILTER.Listener listener = (FILTER.Listener) filter;
                    if (t == null) {
                        listener.onResponse(r, originalInvoker, invocation);
                    } else {
                        listener.onError(t, originalInvoker, invocation);
                    }
                }
            });
        }

        @Override
        public void destroy() {
            originalInvoker.destroy();
        }

        @Override
        public String toString() {
            return originalInvoker.toString();
        }
    }

    /**
     * 专用于消费者端（Consumer）的集群过滤器链节点
     * 继承自 FilterChainNode 并实现了 ClusterInvoker 接口，以支持集群相关的元数据获取
     *
     * @param <T> 服务接口类型
     * @param <TYPE> 集群调用器类型
     * @param <FILTER> 过滤器类型
     */
    class ClusterFilterChainNode<T, TYPE extends ClusterInvoker<T>, FILTER extends BaseFilter>
            extends FilterChainNode<T, TYPE, FILTER> implements ClusterInvoker<T> {
        public ClusterFilterChainNode(TYPE originalInvoker, Invoker<T> nextNode, FILTER filter) {
            super(originalInvoker, nextNode, filter);
        }

        @Override
        public URL getRegistryUrl() {
            return getOriginalInvoker().getRegistryUrl();
        }

        @Override
        public Directory<T> getDirectory() {
            return getOriginalInvoker().getDirectory();
        }

        @Override
        public boolean isDestroyed() {
            return getOriginalInvoker().isDestroyed();
        }
    }

    /**
     * 回调注册调用器，用于在过滤器链末端统一处理所有过滤器的异步回调
     * 采用逆向遍历的方式确保过滤器回调的执行顺序与调用顺序相反（类似栈的后进先出）
     *
     * @param <T> 服务接口类型
     * @param <FILTER> 过滤器类型
     */
    class CallbackRegistrationInvoker<T, FILTER extends BaseFilter> implements Invoker<T> {
        private static final ErrorTypeAwareLogger LOGGER =
                LoggerFactory.getErrorTypeAwareLogger(CallbackRegistrationInvoker.class);

        /** 经过所有过滤器包装后的最终调用器 */
        final Invoker<T> filterInvoker;
        /** 参与回调的所有过滤器列表 */
        final List<FILTER> filters;

        public CallbackRegistrationInvoker(Invoker<T> filterInvoker, List<FILTER> filters) {
            this.filterInvoker = filterInvoker;
            this.filters = filters;
        }

        /**
         * 执行最终的远程调用，并为列表中所有过滤器注册异步完成监听
         *
         * @param invocation 调用上下文
         * @return 异步执行结果
         * @throws RpcException 当调用或回调处理发生异常时抛出
         */
        @Override
        public Result invoke(Invocation invocation) throws RpcException {
            Result asyncResult = filterInvoker.invoke(invocation);
            asyncResult.whenCompleteWithContext((r, t) -> {
                RuntimeException filterRuntimeException = null;
                // 逆序遍历过滤器列表，执行后置回调逻辑
                for (int i = filters.size() - 1; i >= 0; i--) {
                    FILTER filter = filters.get(i);
                    try {
                        InvocationProfilerUtils.releaseDetailProfiler(invocation);
                        if (filter instanceof ListenableFilter) {
                            ListenableFilter listenableFilter = ((ListenableFilter) filter);
                            Filter.Listener listener = listenableFilter.listener(invocation);
                            try {
                                if (listener != null) {
                                    if (t == null) {
                                        listener.onResponse(r, filterInvoker, invocation);
                                    } else {
                                        listener.onError(t, filterInvoker, invocation);
                                    }
                                }
                            } finally {
                                listenableFilter.removeListener(invocation);
                            }
                        } else if (filter instanceof FILTER.Listener) {
                            FILTER.Listener listener = (FILTER.Listener) filter;
                            if (t == null) {
                                listener.onResponse(r, filterInvoker, invocation);
                            } else {
                                listener.onError(t, filterInvoker, invocation);
                            }
                        }
                    } catch (RuntimeException runtimeException) {
                        // 捕获回调过程中的异常并记录日志，防止单个过滤器故障影响整体流程
                        LOGGER.error(
                                CLUSTER_EXECUTE_FILTER_EXCEPTION,
                                "the custom filter is abnormal",
                                "",
                                String.format(
                                        "Exception occurred while executing the %s filter named %s.",
                                        i, filter.getClass().getSimpleName()));
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(String.format(
                                    "Whole filter list is: %s",
                                    filters.stream()
                                            .map(tmpFilter ->
                                                    tmpFilter.getClass().getSimpleName())
                                            .collect(Collectors.toList())));
                        }
                        filterRuntimeException = runtimeException;
                        t = runtimeException;
                    }
                }
                if (filterRuntimeException != null) {
                    throw filterRuntimeException;
                }
            });

            return asyncResult;
        }

        public Invoker<T> getFilterInvoker() {
            return filterInvoker;
        }

        @Override
        public Class<T> getInterface() {
            return filterInvoker.getInterface();
        }

        @Override
        public URL getUrl() {
            return filterInvoker.getUrl();
        }

        @Override
        public boolean isAvailable() {
            return filterInvoker.isAvailable();
        }

        @Override
        public void destroy() {
            filterInvoker.destroy();
        }
    }

    /**
     * 专用于消费者端的集群回调注册调用器
     * 继承自 CallbackRegistrationInvoker 并补充了集群相关的元数据访问能力
     *
     * @param <T> 服务接口类型
     * @param <FILTER> 过滤器类型
     */
    class ClusterCallbackRegistrationInvoker<T, FILTER extends BaseFilter>
            extends CallbackRegistrationInvoker<T, FILTER> implements ClusterInvoker<T> {
        private ClusterInvoker<T> originalInvoker;

        public ClusterCallbackRegistrationInvoker(
                ClusterInvoker<T> originalInvoker, Invoker<T> filterInvoker, List<FILTER> filters) {
            super(filterInvoker, filters);
            this.originalInvoker = originalInvoker;
        }

        public ClusterInvoker<T> getOriginalInvoker() {
            return originalInvoker;
        }

        @Override
        public URL getRegistryUrl() {
            return getOriginalInvoker().getRegistryUrl();
        }

        @Override
        public Directory<T> getDirectory() {
            return getOriginalInvoker().getDirectory();
        }

        @Override
        public boolean isDestroyed() {
            return getOriginalInvoker().isDestroyed();
        }
    }

    @Experimental(
            "Works for the same purpose as FilterChainNode, replace FilterChainNode with this one when proved stable enough")
    class CopyOfFilterChainNode<T, TYPE extends Invoker<T>, FILTER extends BaseFilter> implements Invoker<T> {
        private static final ErrorTypeAwareLogger LOGGER =
                LoggerFactory.getErrorTypeAwareLogger(CopyOfFilterChainNode.class);
        TYPE originalInvoker;
        Invoker<T> nextNode;
        FILTER filter;

        public CopyOfFilterChainNode(TYPE originalInvoker, Invoker<T> nextNode, FILTER filter) {
            this.originalInvoker = originalInvoker;
            this.nextNode = nextNode;
            this.filter = filter;
        }

        public TYPE getOriginalInvoker() {
            return originalInvoker;
        }

        @Override
        public Class<T> getInterface() {
            return originalInvoker.getInterface();
        }

        @Override
        public URL getUrl() {
            return originalInvoker.getUrl();
        }

        @Override
        public boolean isAvailable() {
            return originalInvoker.isAvailable();
        }

        @Override
        public Result invoke(Invocation invocation) throws RpcException {
            Result asyncResult;
            try {
                InvocationProfilerUtils.enterDetailProfiler(
                        invocation, () -> "Filter " + filter.getClass().getName() + " invoke.");
                asyncResult = filter.invoke(nextNode, invocation);
                if (!(asyncResult instanceof AsyncRpcResult)) {
                    String msg =
                            "The result of filter invocation must be AsyncRpcResult. (If you want to recreate a result, please use AsyncRpcResult.newDefaultAsyncResult.) "
                                    + "Filter class: " + filter.getClass().getName() + ". Result class: "
                                    + asyncResult.getClass().getName() + ".";
                    LOGGER.error(INTERNAL_ERROR, "", "", msg);
                    throw new RpcException(msg);
                }
            } catch (Exception e) {
                InvocationProfilerUtils.releaseDetailProfiler(invocation);
                if (filter instanceof ListenableFilter) {
                    ListenableFilter listenableFilter = ((ListenableFilter) filter);
                    try {
                        Filter.Listener listener = listenableFilter.listener(invocation);
                        if (listener != null) {
                            listener.onError(e, originalInvoker, invocation);
                        }
                    } finally {
                        listenableFilter.removeListener(invocation);
                    }
                } else if (filter instanceof FILTER.Listener) {
                    FILTER.Listener listener = (FILTER.Listener) filter;
                    listener.onError(e, originalInvoker, invocation);
                }
                throw e;
            } finally {

            }
            return asyncResult;
        }

        @Override
        public void destroy() {
            originalInvoker.destroy();
        }

        @Override
        public String toString() {
            return originalInvoker.toString();
        }
    }

    @Experimental(
            "Works for the same purpose as ClusterFilterChainNode, replace ClusterFilterChainNode with this one when proved stable enough")
    class CopyOfClusterFilterChainNode<T, TYPE extends ClusterInvoker<T>, FILTER extends BaseFilter>
            extends CopyOfFilterChainNode<T, TYPE, FILTER> implements ClusterInvoker<T> {
        public CopyOfClusterFilterChainNode(TYPE originalInvoker, Invoker<T> nextNode, FILTER filter) {
            super(originalInvoker, nextNode, filter);
        }

        @Override
        public URL getRegistryUrl() {
            return getOriginalInvoker().getRegistryUrl();
        }

        @Override
        public Directory<T> getDirectory() {
            return getOriginalInvoker().getDirectory();
        }

        @Override
        public boolean isDestroyed() {
            return getOriginalInvoker().isDestroyed();
        }
    }
}
