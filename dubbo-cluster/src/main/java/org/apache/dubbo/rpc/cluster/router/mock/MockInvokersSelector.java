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
package org.apache.dubbo.rpc.cluster.router.mock;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotNode;
import org.apache.dubbo.rpc.cluster.router.state.AbstractStateRouter;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.RouterGroupingState;

import java.util.HashMap;
import java.util.Map;

import static org.apache.dubbo.rpc.cluster.Constants.INVOCATION_NEED_MOCK;
import static org.apache.dubbo.rpc.cluster.Constants.MOCK_PROTOCOL;

/**
 * Mock 调用者选择器路由器。
 * 该路由器根据调用上下文中的 mock 标识（invocation.need.mock）来决定路由到正常服务提供者还是 Mock 服务提供者。
 *
 * 工作原理：
 * 1. 在 notify 阶段，将所有 Invoker 分为两类：Mock 协议的和正常协议的
 * 2. 在 route 阶段，检查 Invocation 中是否设置了 invocation.need.mock 参数
 *    - 如果未设置或为 false，返回正常调用者列表
 *    - 如果设置为 true，返回 Mock 调用者列表
 *
 * 这种机制支持 Dubbo 的服务降级和 Mock 测试功能。
 *
 * @param <T> 服务类型
 */
public class MockInvokersSelector<T> extends AbstractStateRouter<T> {

    public static final String NAME = "MOCK_ROUTER";

    private volatile BitList<Invoker<T>> normalInvokers = BitList.emptyList();
    private volatile BitList<Invoker<T>> mockedInvokers = BitList.emptyList();

    /**
     * 构造 Mock 调用者选择器实例。
     *
     * @param url 消费者 URL
     */
    public MockInvokersSelector(URL url) {
        super(url);
    }

    /**
     * 执行 Mock 路由逻辑，根据调用上下文决定是否使用 Mock 服务。
     *
     * 路由决策流程：
     * 1. 如果调用者列表为空，直接返回
     * 2. 如果调用附件为空，返回正常调用者列表
     * 3. 检查 invocation.need.mock 参数：
     *    - 未设置：返回正常调用者列表
     *    - 设置为 true：返回 Mock 调用者列表
     *    - 其他值：返回原始调用者列表
     *
     * @param invokers 待路由的调用者列表
     * @param url 消费者 URL
     * @param invocation RPC 调用信息，包含方法名、参数和附件
     * @param needToPrintMessage 是否需要打印路由快照消息
     * @param nodeHolder 路由快照节点持有器
     * @param messageHolder 路由消息持有器，用于记录路由决策原因
     * @return 路由后的调用者列表（正常调用者或 Mock 调用者）
     * @throws RpcException 路由过程中发生的异常
     */
    @Override
    protected BitList<Invoker<T>> doRoute(
            BitList<Invoker<T>> invokers,
            URL url,
            Invocation invocation,
            boolean needToPrintMessage,
            Holder<RouterSnapshotNode<T>> nodeHolder,
            Holder<String> messageHolder)
            throws RpcException {
        // 如果调用者列表为空，直接返回
        if (CollectionUtils.isEmpty(invokers)) {
            if (needToPrintMessage) {
                messageHolder.set("Empty invokers. Directly return.");
            }
            return invokers;
        }

        // 如果调用附件为空，返回正常调用者列表与输入列表的交集
        if (invocation.getObjectAttachments() == null) {
            if (needToPrintMessage) {
                messageHolder.set("ObjectAttachments from invocation are null. Return normal Invokers.");
            }
            return invokers.and(normalInvokers);
        } else {
            // 获取 invocation.need.mock 参数的值
            String value = (String) invocation.getObjectAttachmentWithoutConvert(INVOCATION_NEED_MOCK);
            // 如果未设置 mock 标识，返回正常调用者列表
            if (value == null) {
                if (needToPrintMessage) {
                    messageHolder.set("invocation.need.mock not set. Return normal Invokers.");
                }
                return invokers.and(normalInvokers);
            } else if (Boolean.TRUE.toString().equalsIgnoreCase(value)) {
                // 如果 mock 标识为 true，返回 Mock 调用者列表
                if (needToPrintMessage) {
                    messageHolder.set("invocation.need.mock is true. Return mocked Invokers.");
                }
                return invokers.and(mockedInvokers);
            }
        }
        // mock 标识已设置但不为 true，返回原始调用者列表
        if (needToPrintMessage) {
            messageHolder.set("Directly Return. Reason: invocation.need.mock is set but not match true");
        }
        return invokers;
    }

    /**
     * 通知路由器调用者列表已更新。
     * 将调用者列表缓存并分类为 Mock 调用者和正常调用者。
     *
     * @param invokers 更新后的调用者列表
     */
    @Override
    public void notify(BitList<Invoker<T>> invokers) {
        // 缓存 Mock 协议的调用者
        cacheMockedInvokers(invokers);
        // 缓存正常协议的调用者
        cacheNormalInvokers(invokers);
    }

    /**
     * 缓存 Mock 协议的调用者。
     * 从完整的调用者列表中筛选出协议为 mock 的调用者。
     *
     * @param invokers 完整的调用者列表
     */
    private void cacheMockedInvokers(BitList<Invoker<T>> invokers) {
        // 克隆调用者列表，避免修改原始数据
        BitList<Invoker<T>> clonedInvokers = invokers.clone();
        // 移除所有非 Mock 协议的调用者
        clonedInvokers.removeIf((invoker) -> !invoker.getUrl().getProtocol().equals(MOCK_PROTOCOL));
        mockedInvokers = clonedInvokers;
    }

    /**
     * 缓存正常协议的调用者。
     * 从完整的调用者列表中筛选出协议不为 mock 的调用者。
     *
     * @param invokers 完整的调用者列表
     */
    @SuppressWarnings("rawtypes")
    private void cacheNormalInvokers(BitList<Invoker<T>> invokers) {
        // 克隆调用者列表，避免修改原始数据
        BitList<Invoker<T>> clonedInvokers = invokers.clone();
        // 移除所有 Mock 协议的调用者
        clonedInvokers.removeIf((invoker) -> invoker.getUrl().getProtocol().equals(MOCK_PROTOCOL));
        normalInvokers = clonedInvokers;
    }

    /**
     * 构建路由器的快照字符串表示。
     * 将 Mock 调用者和正常调用者分组展示，便于调试和监控。
     *
     * @return 包含 Mock 和正常调用者数量及分组的快照字符串
     */
    @Override
    protected String doBuildSnapshot() {
        // 创建调用者分组映射
        Map<String, BitList<Invoker<T>>> grouping = new HashMap<>();
        grouping.put("Mocked", mockedInvokers);
        grouping.put("Normal", normalInvokers);
        // 使用 RouterGroupingState 格式化输出
        return new RouterGroupingState<>(
                        this.getClass().getSimpleName(), mockedInvokers.size() + normalInvokers.size(), grouping)
                .toString();
    }
}
