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
package org.apache.dubbo.rpc.cluster.filter.support;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.PenetrateAttachmentSelector;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_TIMEOUT_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;

/**
 * ConsumerContextFilter set current RpcContext with invoker,invocation, local host, remote host and port
 * for consumer invoker.It does it to make the requires info available to execution thread's RpcContext.
 *
 * @see Filter
 * @see RpcContext
 */
@Activate(group = CONSUMER, order = Integer.MIN_VALUE)
public class ConsumerContextFilter implements ClusterFilter, ClusterFilter.Listener {

    private Set<PenetrateAttachmentSelector> supportedSelectors;

    public ConsumerContextFilter(ApplicationModel applicationModel) {
        ExtensionLoader<PenetrateAttachmentSelector> selectorExtensionLoader =
                applicationModel.getExtensionLoader(PenetrateAttachmentSelector.class);
        supportedSelectors = selectorExtensionLoader.getSupportedExtensionInstances();
    }

        /**
     * 在执行远程调用前初始化消费者上下文，处理附件传递、超时倒计时及元数据同步。
     * <p>
     * 该过滤器是消费者端调用链的起点，负责将 {@link RpcContext} 中的配置和状态信息同步到当前的 {@link Invocation} 对象中。
     * 主要功能包括：设置当前的 Invoker 和 Invocation 到上下文；将客户端附件（Attachments）透传到服务端，支持通过选择器动态筛选或全量复制；
     * 处理分布式链路追踪等跨层级的参数传递；以及检查并传递超时倒计时对象，确保在重试或级联调用中剩余超时时间能被正确感知。
     * </p>
     *
     * @param invoker 当前调用的执行器，代表远程服务的代理
     * @param invocation 封装了方法名、参数及附件的调用对象
     * @return 远程调用的执行结果，如果超时倒计时已耗尽则直接返回异常结果
     * @throws RpcException 当调用过程中发生网络或业务异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        /*
         * 初始化 RpcContext，绑定当前的执行器和调用对象
         */
        RpcContext.getServiceContext().setInvoker(invoker).setInvocation(invocation);

        RpcContext context = RpcContext.getClientAttachment();
        context.setAttachment(REMOTE_APPLICATION_KEY, invoker.getUrl().getApplication());
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker);
        }

        /*
         * 处理附件透传逻辑：优先使用选择器提取需要下发的附件，否则全量同步服务端附件
         */
        if (CollectionUtils.isNotEmpty(supportedSelectors)) {
            for (PenetrateAttachmentSelector supportedSelector : supportedSelectors) {
                Map<String, Object> selected = supportedSelector.select(
                        invocation, RpcContext.getClientAttachment(), RpcContext.getServerAttachment());
                if (CollectionUtils.isNotEmptyMap(selected)) {
                    ((RpcInvocation) invocation).addObjectAttachments(selected);
                }
            }
        } else {
            ((RpcInvocation) invocation)
                    .addObjectAttachments(RpcContext.getServerAttachment().getObjectAttachments());
        }
        /*
         * 将客户端上下文中的附件添加到调用对象中，覆盖已有值以支持 Filter 链中的动态更新（如 traceId）
         */
        Map<String, Object> contextAttachments =
                RpcContext.getClientAttachment().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            /**
             * invocation.addAttachmentsIfAbsent(context){@link RpcInvocation#addAttachmentsIfAbsent(Map)}should not be used here,
             * because the {@link RpcContext#setAttachment(String, String)} is passed in the Filter when the call is triggered
             * by the built-in retry mechanism of the Dubbo. The attachment to update RpcContext will no longer work, which is
             * a mistake in most cases (for example, through Filter to RpcContext output traceId and spanId and other information).
             */
            ((RpcInvocation) invocation).addObjectAttachments(contextAttachments);
        }

        // pass default timeout set by end user (ReferenceConfig)
        /*
         * 处理超时倒计时逻辑：如果开启了倒计时功能，检查剩余时间并在耗尽时立即终止调用
         */
        Object countDown = RpcContext.getServerAttachment().getObjectAttachment(TIME_COUNTDOWN_KEY);
        if (countDown != null) {
            String methodName = RpcUtils.getMethodName(invocation);
            // When the client has enabled the timeout-countdown function,
            // the subsequent calls launched by the Server side will be enabled by default,
            // and support to turn off the function on a node to get rid of the timeout control.
            if (invoker.getUrl().getMethodParameter(methodName, ENABLE_TIMEOUT_COUNTDOWN_KEY, true)) {
                context.setObjectAttachment(TIME_COUNTDOWN_KEY, countDown);

                TimeoutCountDown timeoutCountDown = (TimeoutCountDown) countDown;
                if (timeoutCountDown.isExpired()) {
                    return AsyncRpcResult.newDefaultAsyncResult(
                            new RpcException(
                                    RpcException.TIMEOUT_TERMINATE,
                                    "No time left for making the following call: " + invocation.getServiceName() + "."
                                            + RpcUtils.getMethodName(invocation) + ", terminate directly."),
                            invocation);
                }
            }
        }
        RpcContext.removeClientResponseContext();
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // pass attachments to result
        Map<String, Object> map = appResponse.getObjectAttachments();
        RpcContext.getClientResponseContext().setObjectAttachments(map);
        removeContext(invocation);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        removeContext(invocation);
    }

    /**
     * 清理RPC调用后的上下文信息，根据调用模式选择性移除ServiceContext。
     * <p>
     * 该方法在远程调用完成后执行（通常在finally块中），负责回收线程局部的RpcContext资源，防止内存泄漏。
     * 清理策略根据调用模式有所不同：
     * <ul>
     *   <li><b>客户端附件</b>：无论何种模式都会立即移除，因为每次调用都会重新构建</li>
     *   <li><b>服务上下文</b>：仅在异步（ASYNC）或Future模式下移除，因为同步模式下用户可能需要在调用结束后继续访问上下文信息</li>
     *   <li><b>服务端附件</b>：刻意保留不清理，因为用户可能在回调逻辑中使用；其清理被延迟到下一次RPC调用开始时执行（见invoke()方法中的removeServerContext()调用）</li>
     * </ul>
     * </p>
     *
     * @param invocation 已完成的RPC调用对象，用于判断调用模式以决定清理策略
     */
    private void removeContext(Invocation invocation) {
        RpcContext.removeClientAttachment();
        if (invocation instanceof RpcInvocation) {
            RpcInvocation rpcInvocation = (RpcInvocation) invocation;
            if (rpcInvocation.getInvokeMode() != null) {
                // clear service context if not in sync mode
                /*
                 * 异步或Future模式下才清理服务上下文，同步模式保留供用户后续访问
                 */
                if (rpcInvocation.getInvokeMode() == InvokeMode.ASYNC
                        || rpcInvocation.getInvokeMode() == InvokeMode.FUTURE) {
                    RpcContext.removeServiceContext();
                }
            }
        }
        // server context must not be removed because user might use it on callback.
        // So the clear of is delayed til the start of the next rpc call, see RpcContext.removeServerContext(); in
        // invoke() above
        // RpcContext.removeServerContext();
    }
}
