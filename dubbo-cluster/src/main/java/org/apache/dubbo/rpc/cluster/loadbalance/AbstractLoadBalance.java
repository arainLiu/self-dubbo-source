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
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WARMUP;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WEIGHT;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

public abstract class AbstractLoadBalance implements LoadBalance {
    /**
     * Calculate the weight according to the uptime proportion of warmup time
     * the new weight will be within 1(inclusive) to weight(inclusive)
     *
     * @param uptime the uptime in milliseconds
     * @param warmup the warmup time in milliseconds
     * @param weight the weight of an invoker
     * @return weight which takes warmup into account
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) (uptime / ((float) warmup / weight));
        return ww < 1 ? 1 : (Math.min(ww, weight));
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        if (invokers.size() == 1) {
            return invokers.get(0);
        }
        return doSelect(invokers, url, invocation);
    }

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

        /**
     * 获取Invoker的动态权重，综合考虑配置权重、服务预热时间和多注册中心场景。
     * <p>
     * 该方法用于负载均衡算法中计算每个服务提供者的实际权重。如果服务处于预热阶段（uptime < warmup），
     * 权重会按启动时间的比例线性增加，避免新启动的服务因尚未完全初始化而承受过高流量。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>确定使用的URL：如果是ClusterInvoker类型，则使用registryUrl以支持多注册中心负载均衡</li>
     *   <li>判断是否为注册中心内部服务引用（REGISTRY_SERVICE_REFERENCE_PATH）：
     *     <ul>
     *       <li>如果是，直接使用URL上的全局weight参数</li>
     *       <li>如果不是，使用方法级别的weight参数，支持针对不同方法进行差异化权重配置</li>
     *     </ul>
     *   </li>
     *   <li>如果配置权重大于0，检查服务是否处于预热阶段：
     *     <ul>
     *       <li>从URL中提取服务启动时间戳（timestamp），计算已运行时长（uptime）</li>
     *       <li>如果uptime为负数（时钟不同步），返回最小权重1以保证基本可用性</li>
     *       <li>如果uptime在预热时间（warmup）范围内，调用calculateWarmupWeight()计算递减后的权重</li>
     *     </ul>
     *   </li>
     *   <li>返回最终权重，确保不为负数（Math.max(weight, 0)）</li>
     * </ol>
     * </p>
     *
     * @param invoker 要计算权重的Invoker对象，包含服务配置和元数据信息
     * @param invocation 当前的RPC调用上下文，用于提取方法名以获取方法级配置
     * @return 计算后的动态权重值，已考虑预热衰减因素
     */
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        int weight;
        URL url = invoker.getUrl();
        if (invoker instanceof ClusterInvoker) {
            /*
             * 多注册中心场景：使用registryUrl进行负载均衡决策
             */
            url = ((ClusterInvoker<?>) invoker).getRegistryUrl();
        }

        // Multiple registry scenario, load balance among multiple registries.
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(url.getServiceInterface())) {
            /*
             * 注册中心内部服务：使用全局权重配置
             */
            weight = url.getParameter(WEIGHT_KEY, DEFAULT_WEIGHT);
        } else {
            /*
             * 普通业务服务：使用方法级别的权重配置
             */
            weight = url.getMethodParameter(RpcUtils.getMethodName(invocation), WEIGHT_KEY, DEFAULT_WEIGHT);
            if (weight > 0) {
                long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);
                if (timestamp > 0L) {
                    long uptime = System.currentTimeMillis() - timestamp;
                    if (uptime < 0) {
                        return 1;
                    }
                    int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP);
                    if (uptime > 0 && uptime < warmup) {
                        /*
                         * 预热阶段：按运行时间比例递减权重，避免新实例过载
                         */
                        weight = calculateWarmupWeight((int) uptime, warmup, weight);
                    }
                }
            }
        }
        return Math.max(weight, 0);
    }
}
