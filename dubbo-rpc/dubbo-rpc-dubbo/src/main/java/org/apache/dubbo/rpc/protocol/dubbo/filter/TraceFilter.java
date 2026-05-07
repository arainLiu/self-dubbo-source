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
package org.apache.dubbo.rpc.protocol.dubbo.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_PARSE;

@Activate(group = CommonConstants.PROVIDER)
public class TraceFilter implements Filter {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(TraceFilter.class);

    private static final String TRACE_MAX = "trace.max";

    private static final String TRACE_COUNT = "trace.count";

    private static final ConcurrentMap<String, Set<Channel>> TRACERS = new ConcurrentHashMap<>();

    public static void addTracer(Class<?> type, String method, Channel channel, int max) {
        channel.setAttribute(TRACE_MAX, max);
        channel.setAttribute(TRACE_COUNT, new AtomicInteger());
        String key = StringUtils.isNotEmpty(method) ? type.getName() + "." + method : type.getName();
        Set<Channel> channels = ConcurrentHashMapUtils.computeIfAbsent(TRACERS, key, k -> new ConcurrentHashSet<>());
        channels.add(channel);
    }

    public static void removeTracer(Class<?> type, String method, Channel channel) {
        channel.removeAttribute(TRACE_MAX);
        channel.removeAttribute(TRACE_COUNT);
        String key = StringUtils.isNotEmpty(method) ? type.getName() + "." + method : type.getName();
        Set<Channel> channels = TRACERS.get(key);
        if (channels != null) {
            channels.remove(channel);
        }
    }

    /**
     * 执行服务调用并跟踪调用详情，将结果实时推送至已订阅的监控通道
     * 记录调用耗时、参数及返回值，并根据配置的最大跟踪次数自动管理跟踪通道
     *
     * @param invoker 服务调用器，用于获取接口名称及执行实际调用
     * @param invocation 调用上下文对象，包含方法名、参数等信息
     * @return RPC调用结果
     * @throws RpcException 当RPC调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 记录调用开始时间并执行实际的RPC调用
        long start = System.currentTimeMillis();
        Result result = invoker.invoke(invocation);
        long end = System.currentTimeMillis();

        // 如果存在活跃的跟踪通道，则执行跟踪逻辑
        if (TRACERS.size() > 0) {
            // 优先按“类名.方法名”匹配跟踪通道，若无则降级按“类名”匹配
            String key = invoker.getInterface().getName() + "." + RpcUtils.getMethodName(invocation);
            Set<Channel> channels = TRACERS.get(key);
            if (CollectionUtils.isEmpty(channels)) {
                key = invoker.getInterface().getName();
                channels = TRACERS.get(key);
            }

            // 遍历所有匹配的通道，发送跟踪信息
            if (CollectionUtils.isNotEmpty(channels)) {
                for (Channel channel : new ArrayList<>(channels)) {
                    if (channel.isConnected()) {
                        try {
                            // 获取该通道允许跟踪的最大次数，默认为1次
                            int max = 1;
                            Integer m = (Integer) channel.getAttribute(TRACE_MAX);
                            if (m != null) {
                                max = m;
                            }

                            // 获取或初始化当前通道的已跟踪计数
                            int count;
                            AtomicInteger c = (AtomicInteger) channel.getAttribute(TRACE_COUNT);
                            if (c == null) {
                                c = new AtomicInteger();
                                channel.setAttribute(TRACE_COUNT, c);
                            }
                            count = c.getAndIncrement();

                            // 如果未达到最大跟踪次数，则向通道发送详细的调用跟踪信息
                            if (count < max) {
                                String prompt =
                                        channel.getUrl().getParameter(Constants.PROMPT_KEY, Constants.DEFAULT_PROMPT);
                                channel.send(
                                        "\r\n" + RpcContext.getServiceContext().getRemoteAddress() + " -> "
                                                + invoker.getInterface().getName()
                                                + "." + RpcUtils.getMethodName(invocation)
                                                + "(" + JsonUtils.toJson(invocation.getArguments()) + ")" + " -> "
                                                + JsonUtils.toJson(result.getValue())
                                                + "\r\nelapsed: " + (end - start) + " ms."
                                                + "\r\n\r\n" + prompt);
                            }

                            // 如果已达到最大跟踪次数，则从跟踪列表中移除该通道
                            if (count >= max - 1) {
                                channels.remove(channel);
                            }
                        } catch (Throwable e) {
                            // 发生异常时移除通道并记录警告日志
                            channels.remove(channel);
                            logger.warn(PROTOCOL_FAILED_PARSE, "", "", e.getMessage(), e);
                        }
                    } else {
                        // 如果通道已断开连接，则直接移除
                        channels.remove(channel);
                    }
                }
            }
        }
        return result;
    }
}
