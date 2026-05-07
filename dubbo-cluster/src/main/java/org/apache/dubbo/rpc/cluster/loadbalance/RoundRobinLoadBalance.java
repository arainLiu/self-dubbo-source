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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    private static final int RECYCLE_PERIOD = 60000;

    private final ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap =
            new ConcurrentHashMap<>();

    protected static class WeightedRoundRobin {
        private int weight;
        private final AtomicLong current = new AtomicLong(0);
        private long lastUpdate;

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        public long increaseCurrent() {
            return current.addAndGet(weight);
        }

        public void sel(int total) {
            current.addAndGet(-1 * total);
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     *
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + RpcUtils.getMethodName(invocation);
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    /**
     * 基于加权平滑轮询算法（Weighted Round Robin）选择服务提供者。
     * <p>
     * 该算法通过为每个Invoker维护一个当前权重（current weight），在每次调用时动态调整并选出当前权重最大的节点，
     * 从而实现流量的均匀分布并兼顾各节点的权重差异。主要执行以下操作：
     * <ol>
     *   <li>生成缓存键：由服务接口名和方法名组成，确保不同方法的负载均衡状态相互隔离</li>
     *   <li>遍历所有Invoker，获取其动态权重（考虑预热因素），并为每个Invoker维护一个WeightedRoundRobin对象</li>
     *   <li>如果检测到权重发生变化，实时更新WeightedRoundRobin中的配置权重</li>
     *   <li>调用increaseCurrent()增加当前权重，并记录最后更新时间</li>
     *   <li>找出当前权重最大的Invoker作为本次调用的目标节点</li>
     *   <li>清理过期数据：如果缓存中的Invoker数量与实际列表不一致（如有节点下线），则移除超过RECYCLE_PERIOD未更新的记录</li>
     *   <li>调用selectedWRR.sel(totalWeight)减少选中节点的当前权重，实现权重的周期性回落</li>
     * </ol>
     * </p>
     *
     * @param invokers 待选择的Invoker列表，包含所有可用的服务提供者
     * @param url 消费者URL，包含负载均衡配置和服务元数据
     * @param invocation RPC调用上下文，用于提取方法名以获取方法级权重配置
     * @return 根据加权轮询算法选出的最优Invoker对象
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + RpcUtils.getMethodName(invocation);
        ConcurrentMap<String, WeightedRoundRobin> map =
                ConcurrentHashMapUtils.computeIfAbsent(methodWeightMap, key, k -> new ConcurrentHashMap<>());
        int totalWeight = 0;
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;
        for (Invoker<T> invoker : invokers) {
            /*
             * 获取或创建当前Invoker的加权轮询状态对象，并同步最新的权重配置
             */
            String identifyString = invoker.getUrl().toIdentityString();
            int weight = getWeight(invoker, invocation);
            WeightedRoundRobin weightedRoundRobin = ConcurrentHashMapUtils.computeIfAbsent(map, identifyString, k -> {
                WeightedRoundRobin wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                return wrr;
            });

            if (weight != weightedRoundRobin.getWeight()) {
                // weight changed
                weightedRoundRobin.setWeight(weight);
            }
            /*
             * 增加当前权重并更新最后活跃时间，参与最大权重竞争
             */
            long cur = weightedRoundRobin.increaseCurrent();
            weightedRoundRobin.setLastUpdate(now);
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            totalWeight += weight;
        }
        /*
         * 清理已下线的Invoker对应的权重状态，防止内存泄漏
         */
        if (invokers.size() != map.size()) {
            map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
        }
        if (selectedInvoker != null) {
            /*
             * 选中节点后，扣减其当前权重以实现轮转效果
             */
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(0);
    }
}
