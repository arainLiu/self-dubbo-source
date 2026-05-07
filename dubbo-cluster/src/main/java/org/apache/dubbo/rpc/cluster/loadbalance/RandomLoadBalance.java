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
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * This class select one provider from multiple providers randomly.
 * You can define weights for each provider:
 * If the weights are all the same then it will use random.nextInt(number of invokers).
 * If the weights are different then it will use random.nextInt(w1 + w2 + ... + wn)
 * Note that if the performance of the machine is better than others, you can set a larger weight.
 * If the performance is not so good, you can set a smaller weight.
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * 基于加权随机算法（Weighted Random）从Invoker列表中选择一个服务提供者。
     * <p>
     * 该算法根据每个Invoker的权重值计算其被选中的概率，权重越高被选中的机会越大。
     * 主要执行以下操作：
     * <ol>
     *   <li>如果不需要加权负载均衡（所有权重相同或为0），则直接进行均匀随机选择</li>
     *   <li>遍历所有Invoker，获取每个节点的动态权重（考虑预热因素），并构建累积权重数组：
     *     <ul>
     *       <li>weights[i]存储的是前i+1个Invoker的权重总和，用于后续的区间映射</li>
     *       <li>在累加过程中检测所有Invoker是否具有相同权重，以便后续优化</li>
     *     </ul>
     *   </li>
     *   <li>如果总权重大于0且权重不一致，则执行加权随机选择：
     *     <ul>
     *       <li>在[0, totalWeight)范围内生成一个随机偏移量offset</li>
     *       <li>通过查找offset落在哪个累积权重区间来确定选中的Invoker</li>
     *       <li>针对小规模列表（<=4个）使用线性扫描，大规模列表使用二分查找以提升性能</li>
     *     </ul>
     *   </li>
     *   <li>如果所有权重相同或总权重为0，则回退到均匀随机选择</li>
     * </ol>
     * </p>
     *
     * @param invokers 待选择的Invoker列表，包含所有可用的服务提供者
     * @param url 消费者URL，包含负载均衡配置和服务元数据
     * @param invocation RPC调用上下文，用于提取方法名以获取方法级权重配置
     * @return 根据加权随机算法选中的Invoker对象
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();

        if (!needWeightLoadBalance(invokers, invocation)) {
            return invokers.get(ThreadLocalRandom.current().nextInt(length));
        }

        // Every invoker has the same weight?
        boolean sameWeight = true;
        // the maxWeight of every invoker, the minWeight = 0 or the maxWeight of the last invoker
        int[] weights = new int[length];
        // The sum of weights
        int totalWeight = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            // Sum
            totalWeight += weight;
            // save for later use
            weights[i] = totalWeight;
            if (sameWeight && totalWeight != weight * (i + 1)) {
                sameWeight = false;
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on
            // totalWeight.
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return an invoker based on the random value.
            /*
             * 根据随机偏移量在累积权重数组中查找对应的Invoker，小列表用线性扫描，大列表用二分查找
             */
            if (length <= 4) {
                for (int i = 0; i < length; i++) {
                    if (offset < weights[i]) {
                        return invokers.get(i);
                    }
                }
            } else {
                int i = Arrays.binarySearch(weights, offset);
                if (i < 0) {
                    i = -i - 1;
                } else {
                    while (weights[i + 1] == offset) {
                        i++;
                    }
                    i++;
                }
                return invokers.get(i);
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }


    /**
     * 判断当前场景是否需要执行加权负载均衡逻辑。
     * <p>
     * 该方法通过检查配置中是否存在权重或时间戳相关参数，来决定是否启用加权随机算法。
     * 如果没有任何权重配置且没有服务启动时间戳（用于预热计算），则可以直接使用均匀随机选择以降低开销。
     * </p>
     * <p>
     * 判断逻辑：
     * <ul>
     *   <li><b>多注册中心场景</b>：如果接口名为 REGISTRY_SERVICE_REFERENCE_PATH，仅检查 URL 上是否配置了全局 weight 参数。</li>
     *   <li><b>普通业务场景</b>：
     *     <ol>
     *       <li>优先检查是否配置了方法级的 weight 参数。</li>
     *       <li>如果没有配置权重，但存在 timestamp 参数（说明服务有启动时间，可能涉及预热逻辑），也返回 true。</li>
     *     </ol>
     *   </li>
     * </ul>
     * </p>
     *
     * @param invokers 待选择的Invoker列表，用于提取第一个节点的URL信息
     * @param invocation RPC调用上下文，用于提取方法名以获取方法级配置
     * @return 如果需要应用加权负载均衡逻辑则返回true，否则返回false
     */
    private <T> boolean needWeightLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        Invoker<T> invoker = invokers.get(0);
        URL invokerUrl = invoker.getUrl();
        if (invoker instanceof ClusterInvoker) {
            /*
             * 多注册中心场景：使用registryUrl进行配置检查
             */
            invokerUrl = ((ClusterInvoker<?>) invoker).getRegistryUrl();
        }

        // Multiple registry scenario, load balance among multiple registries.
        if (REGISTRY_SERVICE_REFERENCE_PATH.equals(invokerUrl.getServiceInterface())) {
            String weight = invokerUrl.getParameter(WEIGHT_KEY);
            return StringUtils.isNotEmpty(weight);
        } else {
            String weight = invokerUrl.getMethodParameter(RpcUtils.getMethodName(invocation), WEIGHT_KEY);
            if (StringUtils.isNotEmpty(weight)) {
                return true;
            } else {
                /*
                 * 检查是否存在时间戳，若存在则可能需要处理预热逻辑
                 */
                String timeStamp = invoker.getUrl().getParameter(TIMESTAMP_KEY);
                return StringUtils.isNotEmpty(timeStamp);
            }
        }
    }
}
