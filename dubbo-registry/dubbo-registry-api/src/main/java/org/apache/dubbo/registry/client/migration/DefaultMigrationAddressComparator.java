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
package org.apache.dubbo.registry.client.migration;

import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_PROPERTY_TYPE_MISMATCH;

public class DefaultMigrationAddressComparator implements MigrationAddressComparator {
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(DefaultMigrationAddressComparator.class);
    private static final String MIGRATION_THRESHOLD = "dubbo.application.migration.threshold";
    private static final String DEFAULT_THRESHOLD_STRING = "0.0";
    private static final float DEFAULT_THREAD = 0f;

    public static final String OLD_ADDRESS_SIZE = "OLD_ADDRESS_SIZE";
    public static final String NEW_ADDRESS_SIZE = "NEW_ADDRESS_SIZE";

    private final ConcurrentMap<String, Map<String, Integer>> serviceMigrationData = new ConcurrentHashMap<>();

    /**
     *
     * 如果应用级订阅不为空，且接口级订阅为空，则用应用级订阅
     * 如果都没有 就用接口级订阅如果都不为空 则 应用级订阅
     * 如果接口级订阅不为空且应用级订阅为空，则用接口级
     * 判断是否应该从旧Invoker迁移到新Invoker，基于地址数量的阈值比较策略。
     * <p>
     * 该方法是Dubbo3服务发现平滑迁移的核心决策逻辑，负责在APPLICATION_FIRST模式下动态选择优先使用的Invoker。
     * 通过对比应用级和接口级的提供者地址数量，结合配置的迁移阈值，决定是否切换到应用级服务发现。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>初始化统计数据</b>：从serviceMigrationData中获取当前服务的迁移数据Map，用于记录历史地址数量变化</li>
     *   <li><b>新Invoker无地址</b>：如果newInvoker（通常是应用级）没有可用的提供者，直接返回false，保持使用oldInvoker（接口级）</li>
     *   <li><b>旧Invoker无地址</b>：如果oldInvoker没有可用的提供者但newInvoker有，直接返回true，强制切换到新Invoker</li>
     *   <li><b>获取地址数量</b>：调用getAddressSize分别统计新旧Invoker的可用提供者数量，记录到migrationData中用于监控</li>
     *   <li><b>确定阈值</b>：
     *     <ul>
     *       <li>优先使用MigrationRule中配置的threshold参数（支持针对特定服务定制）</li>
     *       <li>如果规则中未配置，从系统属性或配置中心读取MIGRATION_THRESHOLD全局配置（默认1.0）</li>
     *       <li>解析失败时使用DEFAULT_THREAD（1.0）作为兜底值</li>
     *     </ul>
     *   </li>
     *   <li><b>边界条件判断</b>：
     *     <ul>
     *       <li>新地址数>0且旧地址数=0：立即返回true，说明只有应用级有提供者</li>
     *       <li>新旧地址数都为0：返回false，避免除以零错误和无意义的迁移</li>
     *     </ul>
     *   </li>
     *   <li><b>阈值比较</b>：计算 (newAddressSize / oldAddressSize) >= threshold，满足条件则迁移到应用级</li>
     * </ol>
     * </p>
     * <p>
     * 阈值说明：
     * <ul>
     *   <li>threshold=1.0（默认）：要求应用级地址数 >= 接口级地址数时才切换，保证不丢失任何提供者</li>
     *   <li>threshold=0.5：应用级地址数达到接口级的一半时就切换，适用于灰度验证阶段</li>
     *   <li>threshold=0.0：只要有应用级地址就立即切换，适用于快速迁移场景</li>
     * </ul>
     * </p>
     *
     * @param newInvoker 新的ClusterInvoker，通常是应用级服务发现的Invoker（ServiceDiscoveryRegistryDirectory）
     * @param oldInvoker 旧的ClusterInvoker，通常是接口级服务发现的Invoker（RegistryDirectory）
     * @param rule       迁移规则对象，包含针对特定服务的阈值配置，可能为null使用全局默认值
     * @return true表示应该迁移到newInvoker（应用级），false表示继续使用oldInvoker（接口级）
     */
    @Override
    public <T> boolean shouldMigrate(ClusterInvoker<T> newInvoker, ClusterInvoker<T> oldInvoker, MigrationRule rule) {
        Map<String, Integer> migrationData = ConcurrentHashMapUtils.computeIfAbsent(
                serviceMigrationData, oldInvoker.getUrl().getDisplayServiceKey(), _k -> new ConcurrentHashMap<>());

        if (!newInvoker.hasProxyInvokers()) {
            /*
             * 新Invoker无可用地址：
             * 记录旧地址数量，标记新地址为-1表示不可用，停止迁移
             */
            migrationData.put(OLD_ADDRESS_SIZE, getAddressSize(oldInvoker));
            migrationData.put(NEW_ADDRESS_SIZE, -1);
            logger.info("No " + getInvokerType(newInvoker) + " address available, stop compare.");
            return false;
        }
        if (!oldInvoker.hasProxyInvokers()) {
            /*
             * 旧Invoker无可用地址：
             * 记录新地址数量，标记旧地址为-1，强制切换到新Invoker
             */
            migrationData.put(OLD_ADDRESS_SIZE, -1);
            migrationData.put(NEW_ADDRESS_SIZE, getAddressSize(newInvoker));
            logger.info("No " + getInvokerType(oldInvoker) + " address available, stop compare.");
            return true;
        }

        int newAddressSize = getAddressSize(newInvoker);
        int oldAddressSize = getAddressSize(oldInvoker);

        migrationData.put(OLD_ADDRESS_SIZE, oldAddressSize);
        migrationData.put(NEW_ADDRESS_SIZE, newAddressSize);

        /*
         * 确定迁移阈值：
         * 优先使用规则配置，其次使用全局系统属性，最后使用默认值1.0
         */
        String rawThreshold = null;
        Float configuredThreshold = rule == null ? null : rule.getThreshold(oldInvoker.getUrl());
        if (configuredThreshold != null && configuredThreshold >= 0) {
            rawThreshold = String.valueOf(configuredThreshold);
        }
        rawThreshold = StringUtils.isNotEmpty(rawThreshold)
                ? rawThreshold
                : ConfigurationUtils.getCachedDynamicProperty(
                        newInvoker.getUrl().getScopeModel(), MIGRATION_THRESHOLD, DEFAULT_THRESHOLD_STRING);
        float threshold;
        try {
            threshold = Float.parseFloat(rawThreshold);
        } catch (Exception e) {
            logger.error(COMMON_PROPERTY_TYPE_MISMATCH, "", "", "Invalid migration threshold " + rawThreshold);
            threshold = DEFAULT_THREAD;
        }

        logger.info("serviceKey:" + oldInvoker.getUrl().getServiceKey() + " Instance address size " + newAddressSize
                + ", interface address size " + oldAddressSize + ", threshold " + threshold);

        /*
         * 边界条件快速判断：
         * 新地址存在且旧地址为空时立即迁移，两者都为空时保持现状
         */
        if (newAddressSize != 0 && oldAddressSize == 0) {
            return true;
        }
        if (newAddressSize == 0 && oldAddressSize == 0) {
            return false;
        }

        /*
         * 阈值比较：
         * 当应用级地址数与接口级地址数的比值达到阈值时，触发迁移
         */
        return ((float) newAddressSize / (float) oldAddressSize) >= threshold;
    }

    private <T> int getAddressSize(ClusterInvoker<T> invoker) {
        if (invoker == null) {
            return -1;
        }
        List<Invoker<T>> invokers = invoker.getDirectory().getAllInvokers();
        return CollectionUtils.isNotEmpty(invokers) ? invokers.size() : 0;
    }

    @Override
    public Map<String, Integer> getAddressSize(String displayServiceKey) {
        return serviceMigrationData.get(displayServiceKey);
    }

    private String getInvokerType(ClusterInvoker<?> invoker) {
        if (invoker.isServiceDiscovery()) {
            return "instance";
        }
        return "interface";
    }
}
