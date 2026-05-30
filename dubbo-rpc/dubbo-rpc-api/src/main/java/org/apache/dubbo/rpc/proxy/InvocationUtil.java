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
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcServiceContext;
import org.apache.dubbo.rpc.support.RpcUtils;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_TIMEOUT_REQUEST;

public class InvocationUtil {
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(InvokerInvocationHandler.class);
    /**
     * 执行RPC调用，管理调用上下文并处理性能监控逻辑
     * 负责设置消费者URL、记录调用耗时，并在检测到潜在超时时发出警告日志
     *
     * @param invoker 调用器对象，代表服务引用的入口，包含集群、路由等逻辑
     * @param rpcInvocation RPC调用上下文，包含方法名、参数、附件及监控埋点信息
     * @return 远程方法调用的返回值，如果是void方法则返回null
     * @throws Throwable 当RPC调用失败、超时或发生其他异常时抛出
     */
    public static Object invoke(Invoker<?> invoker, RpcInvocation rpcInvocation) throws Throwable {
        // 保存当前线程的服务上下文状态，确保在调用结束后能正确恢复，防止上下文污染
        RpcContext.RestoreServiceContext originServiceContext = RpcContext.storeServiceContext();

        try {
            URL url = invoker.getUrl();
            String serviceKey = url.getServiceKey();
            // 设置目标服务的唯一名称，用于链路追踪和日志标识
            rpcInvocation.setTargetServiceUniqueName(serviceKey);

            // invoker.getUrl() returns consumer url.
            // 将当前消费者URL设置到全局上下文中，供后续的Filter或协议层使用
            RpcServiceContext.getServiceContext().setConsumerUrl(url);

            // 如果开启了简易性能监控，则记录调用耗时并进行超时预警
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                ProfilerEntry parentProfiler = Profiler.getBizProfiler();
                ProfilerEntry bizProfiler;
                if (parentProfiler != null) {
                    // 如果存在父级监控节点，则创建子节点以形成调用链
                    bizProfiler = Profiler.enter(
                            parentProfiler,
                            "Receive request. Client invoke begin. ServiceKey: " + serviceKey + " MethodName:"
                                    + rpcInvocation.getMethodName());
                } else {
                    // 否则开启一个新的监控根节点
                    bizProfiler = Profiler.start("Receive request. Client invoke begin. ServiceKey: " + serviceKey + " "
                            + "MethodName:" + rpcInvocation.getMethodName());
                }
                // 将监控节点存入invocation，以便在服务端解码时能接续监控链路
                rpcInvocation.put(Profiler.PROFILER_KEY, bizProfiler);
                try {
                    // 执行实际的RPC调用链，并通过recreate()将底层结果转换为业务对象或异常
                    return invoker.invoke(rpcInvocation).recreate();
                } finally {
                    // 结束当前节点的计时
                    Profiler.release(bizProfiler);

                    // 获取配置的超时阈值，优先从invocation附件中获取，其次从URL方法参数中获取
                    Long timeout =
                            RpcUtils.convertToNumber(rpcInvocation.getObjectAttachmentWithoutConvert(TIMEOUT_KEY));

                    if (timeout == null) {
                        timeout = (long) url.getMethodPositiveParameter(
                                rpcInvocation.getMethodName(), TIMEOUT_KEY, DEFAULT_TIMEOUT);
                    }

                    // 计算实际调用耗时（纳秒）
                    long usage = bizProfiler.getEndTime() - bizProfiler.getStartTime();

                    // 如果耗时超过了预设的警告比例（如80%），则记录详细的警告日志
                    if ((usage / (1000_000L * ProfilerSwitch.getWarnPercent())) > timeout) {
                        StringBuilder attachment = new StringBuilder();
                        // 收集所有的附件信息，便于排查问题
                        rpcInvocation.foreachAttachment((entry) -> {
                            attachment
                                    .append(entry.getKey())
                                    .append("=")
                                    .append(entry.getValue())
                                    .append(";\n");
                        });

                        logger.warn(
                                PROXY_TIMEOUT_REQUEST,
                                "",
                                "",
                                String.format(
                                        "[Dubbo-Consumer] execute service %s#%s cost %d.%06d ms, this invocation almost (maybe already) timeout. Timeout: %dms\n"
                                                + "invocation context:\n%s" + "thread info: \n%s",
                                        rpcInvocation.getProtocolServiceKey(),
                                        rpcInvocation.getMethodName(),
                                        usage / 1000_000,
                                        usage % 1000_000,
                                        timeout,
                                        attachment,
                                        Profiler.buildDetail(bizProfiler)));
                    }
                }
            }

            // 在未开启监控的情况下，直接执行调用并返回结果
            return invoker.invoke(rpcInvocation).recreate();
        } finally {
            // 无论调用成功与否，都必须恢复原始的RpcContext状态，保证线程隔离性
            RpcContext.restoreServiceContext(originServiceContext);
        }
    }

}
