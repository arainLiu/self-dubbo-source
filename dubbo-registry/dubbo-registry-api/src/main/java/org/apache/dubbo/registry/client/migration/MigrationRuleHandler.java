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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.status.reporter.FrameworkStatusReportService;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.registry.client.migration.model.MigrationStep;

import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_NO_PARAMETERS_URL;

public class MigrationRuleHandler<T> {
    public static final String DUBBO_SERVICEDISCOVERY_MIGRATION = "dubbo.application.migration.step";
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(MigrationRuleHandler.class);

    private final MigrationClusterInvoker<T> migrationInvoker;
    private volatile MigrationStep currentStep;
    private volatile Float currentThreshold = 0f;
    private final URL consumerURL;
    private final ReentrantLock lock = new ReentrantLock();

    public MigrationRuleHandler(MigrationClusterInvoker<T> invoker, URL url) {
        this.migrationInvoker = invoker;
        this.consumerURL = url;
    }

    /**
     * 执行服务发现模式迁移，根据迁移规则动态切换Invoker类型。
     * <p>
     * 该方法是Dubbo3服务发现平滑迁移的核心控制逻辑，负责根据MigrationRule配置在三种模式间切换：
     * 接口级服务发现（FORCE_INTERFACE）、应用级优先（APPLICATION_FIRST）、强制应用级（FORCE_APPLICATION）。
     * 通过ReentrantLock保证线程安全，支持并发场景下的幂等性控制。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>特殊场景处理</b>：如果当前已经是ServiceDiscoveryMigrationInvoker（纯应用级），直接强制切换到FORCE_APPLICATION模式，无需进一步判断</li>
     *   <li><b>默认值初始化</b>：设置默认迁移步骤为APPLICATION_FIRST（应用级优先，同时订阅两种注册中心），阈值为-1f</li>
     *   <li><b>规则解析</b>：从MigrationRule中提取step和threshold参数，允许针对特定consumerURL定制不同的迁移策略</li>
     *   <li><b>异常容错</b>：如果规则解析失败，记录错误日志但继续执行，使用默认的APPLICATION_FIRST模式</li>
     *   <li><b>执行迁移</b>：调用refreshInvoker根据step创建对应类型的Invoker，threshold用于控制流量比例（仅在APPLICATION_FIRST模式下生效）</li>
     *   <li><b>规则更新</b>：迁移成功后调用setMigrationRule保存当前规则，供后续动态配置变更时使用</li>
     * </ol>
     * </p>
     *
     * @param rule 迁移规则对象，包含目标迁移步骤（step）、流量阈值（threshold）、作用范围等配置信息
     */
    public void doMigrate(MigrationRule rule) {
        lock.lock();
        try {
            /*
             * 纯应用级场景优化：
             * 当invoker已是ServiceDiscoveryMigrationInvoker时，说明已完成迁移，直接强制使用应用级模式
             */
            if (migrationInvoker instanceof ServiceDiscoveryMigrationInvoker) {
                refreshInvoker(MigrationStep.FORCE_APPLICATION, 1.0f, rule);
                return;
            }

            /*
             * 初始化默认迁移策略：
             * 默认采用APPLICATION_FIRST模式，同时订阅应用级和接口级注册中心，优先使用应用级结果
             */
            // initial step : APPLICATION_FIRST
            MigrationStep step = MigrationStep.APPLICATION_FIRST;
            float threshold = -1f;

            try {
                /*
                 * 解析规则配置：
                 * 从rule中提取step和threshold，允许根据不同服务接口定制不同的迁移策略
                 */
                step = rule.getStep(consumerURL);
                threshold = rule.getThreshold(consumerURL);
            } catch (Exception e) {
                logger.error(
                        REGISTRY_NO_PARAMETERS_URL,
                        "",
                        "",
                        "Failed to get step and threshold info from rule: " + rule,
                        e);
            }

            /*
             * 执行迁移并更新规则：
             * 只有迁移成功时才保存新规则，失败时保持原有状态不变
             */
            if (refreshInvoker(step, threshold, rule)) {
                // refresh success, update rule
                setMigrationRule(rule);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据迁移步骤和阈值刷新Invoker，执行实际的服务发现模式切换。
     * <p>
     * 该方法是服务发现迁移机制的核心执行单元，负责将MigrationStep枚举转换为具体的Invoker类型切换操作。
     * 支持三种迁移模式的互斥切换，并通过成功率判断决定是否保存新的迁移状态。
     * </p>
     * <p>
     * 三种迁移模式说明：
     * <ul>
     *   <li><b>APPLICATION_FIRST（应用级优先）</b>：同时订阅应用级和接口级注册中心，优先使用应用级结果，失败时自动降级到接口级。适用于迁移初期灰度验证阶段。</li>
     *   <li><b>FORCE_APPLICATION（强制应用级）</b>：仅订阅应用级注册中心，完全依赖应用级服务发现。适用于已完成迁移且确认稳定的场景。</li>
     *   <li><b>FORCE_INTERFACE（强制接口级）</b>：仅订阅接口级注册中心，回退到传统Dubbo2模式。适用于迁移失败回滚或兼容旧版本客户端的场景。</li>
     * </ul>
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>参数校验</b>：step和threshold不能为空，否则抛出IllegalStateException阻止非法迁移</li>
     *   <li><b>幂等性判断</b>：如果当前step和threshold与目标值相同，跳过迁移直接返回true，避免重复执行</li>
     *   <li><b>模式切换</b>：通过switch-case调用migrationInvoker对应的migrateToXXX方法执行实际的Invoker重建</li>
     *   <li><b>成功处理</b>：迁移成功时更新currentStep和currentThreshold，记录INFO日志并上报迁移成功指标</li>
     *   <li><b>失败处理</b>：迁移失败时不保存新状态（保持原有step和rule），记录WARN日志并上报迁移失败指标</li>
     *   <li><b>返回结果</b>：返回迁移是否成功，调用方根据返回值决定是否保存新的迁移规则</li>
     * </ol>
     * </p>
     *
     * @param step      目标迁移步骤，决定切换到哪种服务发现模式（APPLICATION_FIRST/FORCE_APPLICATION/FORCE_INTERFACE）
     * @param threshold 流量阈值，范围0.0~1.0，仅在APPLICATION_FIRST模式下生效，控制应用级流量的占比
     * @param newRule   新的迁移规则对象，包含动态配置参数和监听器信息
     * @return 迁移是否成功，true表示Invoker切换成功，false表示迁移失败（通常因为不满足阈值条件）
     * @throws IllegalStateException 当step或threshold参数为null时抛出
     */
    private boolean refreshInvoker(MigrationStep step, Float threshold, MigrationRule newRule) {
        if (step == null || threshold == null) {
            throw new IllegalStateException("Step or threshold of migration rule cannot be null");
        }
        MigrationStep originStep = currentStep;

        /*
         * 幂等性与变更检测：
         * 只有当step或threshold发生变化时才执行迁移，避免重复操作导致的资源浪费
         */
        if ((currentStep == null || currentStep != step) || !currentThreshold.equals(threshold)) {
            boolean success = true;
            switch (step) {
                case APPLICATION_FIRST:
                    /*
                     * 应用级优先模式：
                     * 创建ApplicationFirstClusterInvoker，同时订阅两种注册中心，智能路由
                     */
                    migrationInvoker.migrateToApplicationFirstInvoker(newRule);
                    break;
                case FORCE_APPLICATION:
                    /*
                     * 强制应用级模式：
                     * 创建ForceApplicationClusterInvoker，仅订阅应用级注册中心，可能失败（如提供者未升级）
                     */
                    success = migrationInvoker.migrateToForceApplicationInvoker(newRule);
                    break;
                case FORCE_INTERFACE:
                default:
                    /*
                     * 强制接口级模式：
                     * 创建ForceInterfaceClusterInvoker，仅订阅接口级注册中心，用于回滚或兼容
                     */
                    success = migrationInvoker.migrateToForceInterfaceInvoker(newRule);
            }

            if (success) {
                /*
                 * 迁移成功处理：
                 * 更新状态、记录日志、上报监控指标
                 */
                setCurrentStepAndThreshold(step, threshold);
                logger.info(
                        "Succeed Migrated to " + step + " mode. Service Name: " + consumerURL.getDisplayServiceKey());
                report(step, originStep, "true");
            } else {
                /*
                 * 迁移失败处理：
                 * 不更新状态，记录警告日志和失败指标，便于问题排查
                 */
                // migrate failed, do not save new step and rule
                logger.warn(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Migrate to " + step + " mode failed. Probably not satisfy the threshold you set " + threshold
                                + ". Please try re-publish configuration if you still after check.");
                report(step, originStep, "false");
            }

            return success;
        }
        /*
         * 幂等性快速返回：
         * step和threshold未变化时跳过迁移，但仍会覆盖rule以更新动态配置
         */
        // ignore if step is same with previous, will continue override rule for MigrationInvoker
        return true;
    }

    private void report(MigrationStep step, MigrationStep originStep, String success) {
        FrameworkStatusReportService reportService =
                consumerURL.getOrDefaultApplicationModel().getBeanFactory().getBean(FrameworkStatusReportService.class);

        if (reportService.hasReporter()) {
            reportService.reportMigrationStepStatus(reportService.createMigrationStepReport(
                    consumerURL.getServiceInterface(),
                    consumerURL.getVersion(),
                    consumerURL.getGroup(),
                    String.valueOf(originStep),
                    String.valueOf(step),
                    success));
        }
    }

    private void setMigrationRule(MigrationRule rule) {
        this.migrationInvoker.setMigrationRule(rule);
    }

    private void setCurrentStepAndThreshold(MigrationStep currentStep, Float currentThreshold) {
        if (currentThreshold != null) {
            this.currentThreshold = currentThreshold;
        }
        if (currentStep != null) {
            this.currentStep = currentStep;
            this.migrationInvoker.setMigrationStep(currentStep);
        }
    }

    // for test purpose
    public MigrationStep getMigrationStep() {
        return currentStep;
    }
}
