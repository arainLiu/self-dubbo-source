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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RETRIES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_MULTIPLE_RETRIES;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n times, which means at most n different invokers will be invoked)
 * Note that retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 *
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    /**
     * 执行故障转移的RPC调用，支持自动重试和负载均衡。
     * <p>
     * 该方法是FailoverCluster的核心实现，处理流程包括：
     * 1. 根据方法名计算最大调用次数（首次调用 + 重试次数）；
     * 2. 循环执行调用，每次通过负载均衡器选择一个Invoker；
     * 3. 重试前重新获取Invoker列表，避免服务提供者变化导致的不准确；
     * 4. 捕获非业务异常进行重试，业务异常直接抛出；
     * 5. 记录失败提供者的地址信息，最终抛出包含详细诊断信息的异常。
     * </p>
     *
     * @param invocation RPC调用信息，包含方法名、参数等
     * @param invokers 可用的服务提供者Invoker列表
     * @param loadbalance 负载均衡策略，用于从多个Invoker中选择一个
     * @return Result 调用成功的结果
     * @throws RpcException 当所有重试都失败或发生业务异常时抛出
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance)
            throws RpcException {
        List<Invoker<T>> copyInvokers = invokers;
        String methodName = RpcUtils.getMethodName(invocation);
        // 根据方法配置计算最大调用次数（默认1次，可配置重试次数+1）
        int len = calculateInvokeTimes(methodName);

        // 初始化重试循环所需的变量：最后一次异常、已调用的Invoker列表、失败的提供者集合
        RpcException le = null; // last exception.
        List<Invoker<T>> invoked = new ArrayList<>(copyInvokers.size()); // invoked invokers.
        Set<String> providers = new HashSet<>(len);

        // 执行最多len次的调用循环（首次 + 重试）
        for (int i = 0; i < len; i++) {
            // 重试前重新选择Invoker，避免服务提供者列表发生变化
            // 注意：如果invokers发生变化，invoked列表的准确性会受到影响
            if (i > 0) {
                checkWhetherDestroyed();
                // 重新从目录获取最新的Invoker列表
                copyInvokers = list(invocation);
                // 验证新的Invoker列表是否有效
                checkInvokers(copyInvokers, invocation);
            }

            // 通过负载均衡器选择一个Invoker，排除已调用过的
            Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);
            invoked.add(invoker);
            RpcContext.getServiceContext().setInvokers((List) invoked);

            boolean success = false;
            try {
                // 执行实际的RPC调用
                Result result = invokeWithContext(invoker, invocation);

                // 如果之前有失败记录但本次成功，输出警告日志告知用户存在不稳定的提供者
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn(
                            CLUSTER_FAILED_MULTIPLE_RETRIES,
                            "failed to retry do invoke",
                            "",
                            "Although retry the method " + methodName
                                    + " in the service " + getInterface().getName()
                                    + " was successful by the provider "
                                    + invoker.getUrl().getAddress()
                                    + ", but there have been failed providers " + providers
                                    + " (" + providers.size() + "/" + copyInvokers.size()
                                    + ") from the registry "
                                    + directory.getUrl().getAddress()
                                    + " on the consumer " + NetUtils.getLocalHost()
                                    + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                                    + le.getMessage(),
                            le);
                }
                success = true;
                return result;
            } catch (RpcException e) {
                // 业务异常直接抛出，不进行重试
                if (e.isBiz()) { // biz exception.
                    throw e;
                }
                // 非业务异常（如网络超时、连接拒绝）记录为最后一次异常，继续重试
                le = e;
            } catch (Throwable e) {
                // 其他异常包装为RpcException
                le = new RpcException(e.getMessage(), e);
            } finally {
                // 调用失败时，记录提供者的地址信息用于后续诊断
                if (!success) {
                    providers.add(invoker.getUrl().getAddress());
                }
            }
        }

        // 所有重试都失败后，抛出包含详细信息的异常（包括尝试过的提供者列表、注册中心地址等）
        throw new RpcException(
                le.getCode(),
                "Failed to invoke the method "
                        + methodName + " in the service " + getInterface().getName()
                        + ". Tried " + len + " times of the providers " + providers
                        + " (" + providers.size() + "/" + copyInvokers.size()
                        + ") from the registry " + directory.getUrl().getAddress()
                        + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                        + Version.getVersion() + ". Last error is: "
                        + le.getMessage(),
                le.getCause() != null ? le.getCause() : le);
    }

    private int calculateInvokeTimes(String methodName) {
        int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;
        RpcContext rpcContext = RpcContext.getClientAttachment();
        Object retry = rpcContext.getObjectAttachment(RETRIES_KEY);
        if (retry instanceof Number) {
            len = ((Number) retry).intValue() + 1;
            rpcContext.removeAttachment(RETRIES_KEY);
        }
        if (len <= 0) {
            len = 1;
        }

        return len;
    }
}
