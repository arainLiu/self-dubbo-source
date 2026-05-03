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
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.status.reporter.FrameworkStatusReportService;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.registry.client.migration.model.MigrationStep;
import org.apache.dubbo.registry.integration.DynamicDirectory;
import org.apache.dubbo.registry.integration.RegistryProtocol;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_NOTIFY_EVENT;
import static org.apache.dubbo.registry.client.migration.model.MigrationStep.APPLICATION_FIRST;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

public class MigrationInvoker<T> implements MigrationClusterInvoker<T> {
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(MigrationInvoker.class);

    private URL url;
    private final URL consumerUrl;
    private final Cluster cluster;
    private final Registry registry;
    private final Class<T> type;
    private final RegistryProtocol registryProtocol;
    private MigrationRuleListener migrationRuleListener;
    private final ConsumerModel consumerModel;
    private final FrameworkStatusReportService reportService;

    private volatile ClusterInvoker<T> invoker;
    private volatile ClusterInvoker<T> serviceDiscoveryInvoker;
    private volatile ClusterInvoker<T> currentAvailableInvoker;
    private volatile MigrationStep step;
    private volatile MigrationRule rule;
    private volatile int promotion = 100;

    public MigrationInvoker(
            RegistryProtocol registryProtocol,
            Cluster cluster,
            Registry registry,
            Class<T> type,
            URL url,
            URL consumerUrl) {
        this(null, null, registryProtocol, cluster, registry, type, url, consumerUrl);
    }

    @SuppressWarnings("unchecked")
    public MigrationInvoker(
            ClusterInvoker<T> invoker,
            ClusterInvoker<T> serviceDiscoveryInvoker,
            RegistryProtocol registryProtocol,
            Cluster cluster,
            Registry registry,
            Class<T> type,
            URL url,
            URL consumerUrl) {
        this.invoker = invoker;
        this.serviceDiscoveryInvoker = serviceDiscoveryInvoker;
        this.registryProtocol = registryProtocol;
        this.cluster = cluster;
        this.registry = registry;
        this.type = type;
        this.url = url;
        this.consumerUrl = consumerUrl;
        this.consumerModel = (ConsumerModel) consumerUrl.getServiceModel();
        this.reportService =
                consumerUrl.getOrDefaultApplicationModel().getBeanFactory().getBean(FrameworkStatusReportService.class);

        if (consumerModel != null) {
            Object object =
                    consumerModel.getServiceMetadata().getAttribute(CommonConstants.CURRENT_CLUSTER_INVOKER_KEY);
            Map<Registry, MigrationInvoker<?>> invokerMap;
            if (object instanceof Map) {
                invokerMap = (Map<Registry, MigrationInvoker<?>>) object;
            } else {
                invokerMap = new ConcurrentHashMap<>();
            }
            invokerMap.put(registry, this);
            consumerModel.getServiceMetadata().addAttribute(CommonConstants.CURRENT_CLUSTER_INVOKER_KEY, invokerMap);
        }
    }

    public ClusterInvoker<T> getInvoker() {
        return invoker;
    }

    public void setInvoker(ClusterInvoker<T> invoker) {
        this.invoker = invoker;
    }

    public ClusterInvoker<T> getServiceDiscoveryInvoker() {
        return serviceDiscoveryInvoker;
    }

    public void setServiceDiscoveryInvoker(ClusterInvoker<T> serviceDiscoveryInvoker) {
        this.serviceDiscoveryInvoker = serviceDiscoveryInvoker;
    }

    public ClusterInvoker<T> getCurrentAvailableInvoker() {
        return currentAvailableInvoker;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public void reRefer(URL newSubscribeUrl) {
        // update url to prepare for migration refresh
        this.url = url.addParameter(REFER_KEY, StringUtils.toQueryString(newSubscribeUrl.getParameters()));

        // re-subscribe immediately
        if (invoker != null && !invoker.isDestroyed()) {
            doReSubscribe(invoker, newSubscribeUrl);
        }
        if (serviceDiscoveryInvoker != null && !serviceDiscoveryInvoker.isDestroyed()) {
            doReSubscribe(serviceDiscoveryInvoker, newSubscribeUrl);
        }
    }

    private void doReSubscribe(ClusterInvoker<T> invoker, URL newSubscribeUrl) {
        DynamicDirectory<T> directory = (DynamicDirectory<T>) invoker.getDirectory();
        URL oldSubscribeUrl = directory.getRegisteredConsumerUrl();
        Registry registry = directory.getRegistry();
        registry.unregister(directory.getRegisteredConsumerUrl());
        directory.unSubscribe(RegistryProtocol.toSubscribeUrl(oldSubscribeUrl));
        if (directory.isShouldRegister()) {
            registry.register(directory.getRegisteredConsumerUrl());
            directory.setRegisteredConsumerUrl(newSubscribeUrl);
        }
        directory.buildRouterChain(newSubscribeUrl);
        directory.subscribe(RegistryProtocol.toSubscribeUrl(newSubscribeUrl));
    }

    @Override
    public boolean migrateToForceInterfaceInvoker(MigrationRule newRule) {
        CountDownLatch latch = new CountDownLatch(1);
        refreshInterfaceInvoker(latch);

        if (serviceDiscoveryInvoker == null) {
            // serviceDiscoveryInvoker is absent, ignore threshold check
            this.currentAvailableInvoker = invoker;
            return true;
        }

        // wait and compare threshold
        waitAddressNotify(newRule, latch);

        if (newRule.getForce(consumerUrl)) {
            // force migrate, ignore threshold check
            this.currentAvailableInvoker = invoker;
            this.destroyServiceDiscoveryInvoker();
            return true;
        }

        Set<MigrationAddressComparator> detectors = ScopeModelUtil.getApplicationModel(
                        consumerUrl == null ? null : consumerUrl.getScopeModel())
                .getExtensionLoader(MigrationAddressComparator.class)
                .getSupportedExtensionInstances();
        if (CollectionUtils.isNotEmpty(detectors)) {
            if (detectors.stream()
                    .allMatch(comparator -> comparator.shouldMigrate(invoker, serviceDiscoveryInvoker, newRule))) {
                this.currentAvailableInvoker = invoker;
                this.destroyServiceDiscoveryInvoker();
                return true;
            }
        }

        // compare failed, will not change state
        if (step == MigrationStep.FORCE_APPLICATION) {
            destroyInterfaceInvoker();
        }
        return false;
    }

    @Override
    public boolean migrateToForceApplicationInvoker(MigrationRule newRule) {
        CountDownLatch latch = new CountDownLatch(1);
        refreshServiceDiscoveryInvoker(latch);

        if (invoker == null) {
            // invoker is absent, ignore threshold check
            this.currentAvailableInvoker = serviceDiscoveryInvoker;
            return true;
        }

        // wait and compare threshold
        waitAddressNotify(newRule, latch);

        if (newRule.getForce(consumerUrl)) {
            // force migrate, ignore threshold check
            this.currentAvailableInvoker = serviceDiscoveryInvoker;
            this.destroyInterfaceInvoker();
            return true;
        }

        Set<MigrationAddressComparator> detectors = ScopeModelUtil.getApplicationModel(
                        consumerUrl == null ? null : consumerUrl.getScopeModel())
                .getExtensionLoader(MigrationAddressComparator.class)
                .getSupportedExtensionInstances();
        if (CollectionUtils.isNotEmpty(detectors)) {
            if (detectors.stream()
                    .allMatch(comparator -> comparator.shouldMigrate(serviceDiscoveryInvoker, invoker, newRule))) {
                this.currentAvailableInvoker = serviceDiscoveryInvoker;
                this.destroyInterfaceInvoker();
                return true;
            }
        }

        // compare failed, will not change state
        if (step == MigrationStep.FORCE_INTERFACE) {
            destroyServiceDiscoveryInvoker();
        }
        return false;
    }

    /**
     * 迁移到应用级优先（APPLICATION_FIRST）模式，同时订阅接口级和应用级注册中心。
     * <p>
     * 该方法是Dubbo3服务发现平滑迁移的核心策略之一，通过双订阅机制实现灰度验证和自动降级能力。
     * APPLICATION_FIRST模式会同时创建并维护两个Invoker（接口级和服务发现），在运行时根据配置动态选择优先使用哪个。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>初始化CountDownLatch</b>：创建初始值为0的latch，因为APPLICATION_FIRST模式不需要等待地址通知完成</li>
     *   <li><b>刷新接口级Invoker</b>：调用refreshInterfaceInvoker创建或更新基于接口注册的Invoker（传统Dubbo2模式）</li>
     *   <li><b>刷新应用级Invoker</b>：调用refreshServiceDiscoveryInvoker创建或更新基于应用注册的Invoker（Dubbo3新模式）</li>
     *   <li><b>计算优选Invoker</b>：立即调用calcPreferredInvoker根据规则计算当前应该优先使用的Invoker，不等地址推送完成</li>
     * </ol>
     * </p>
     * <p>
     * 与其他迁移模式的区别：
     * <ul>
     *   <li><b>migrateToForceInterfaceInvoker</b>：仅使用接口级Invoker，销毁应用级Invoker，完全回退到Dubbo2模式</li>
     *   <li><b>migrateToForceApplicationInvoker</b>：仅使用应用级Invoker，销毁接口级Invoker，强制切换到Dubbo3模式</li>
     *   <li><b>migrateToApplicationFirstInvoker</b>：同时保留两个Invoker，优先使用应用级但可自动降级到接口级，适合灰度过渡期</li>
     * </ul>
     * </p>
     *
     * @param newRule 新的迁移规则，包含延迟时间、阈值、force标志等配置参数，用于控制流量分配策略
     */
    @Override
    public void migrateToApplicationFirstInvoker(MigrationRule newRule) {
        /*
         * 创建不阻塞的CountDownLatch：
         * 初始值为0表示不需要等待异步地址通知，立即返回
         */
        CountDownLatch latch = new CountDownLatch(0);
        /*
         * 刷新接口级Invoker：
         * 订阅接口级别的注册中心路径（如：/dubbo/interface:version/providers）
         */
        refreshInterfaceInvoker(latch);
        /*
         * 刷新应用级Invoker：
         * 订阅应用级别的注册中心路径（如：/dubbo/appName/providers）
         */
        refreshServiceDiscoveryInvoker(latch);

        /*
         * 立即计算优选Invoker：
         * 不等地址推送完成就根据规则计算当前应该使用哪个Invoker
         * 后续地址变更时会重新触发calcPreferredInvoker进行动态调整
         */
        // directly calculate preferred invoker, will not wait until address notify
        // calculation will re-occurred when address notify later
        calcPreferredInvoker(newRule);
    }

    @SuppressWarnings("all")
    private void waitAddressNotify(MigrationRule newRule, CountDownLatch latch) {
        // wait and compare threshold
        int delay = newRule.getDelay(consumerUrl);
        if (delay > 0) {
            try {
                Thread.sleep(delay * 1000L);
            } catch (InterruptedException e) {
                logger.error(REGISTRY_FAILED_NOTIFY_EVENT, "", "", "Interrupted when waiting for address notify!" + e);
            }
        } else {
            // do not wait address notify by default
            delay = 0;
        }
        try {
            latch.await(delay, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(REGISTRY_FAILED_NOTIFY_EVENT, "", "", "Interrupted when waiting for address notify!" + e);
        }
    }

    /**
     * 执行RPC调用，根据迁移策略和可用状态选择合适的Invoker进行服务调用。
     * <p>
     * 该方法是MigrationInvoker的核心逻辑，支持三种迁移模式：
     * 1. APPLICATION_FIRST（应用优先）：根据promotion比例在接口级和应用级之间动态选择；
     * 2. FORCE_APPLICATION（强制应用级）：只使用应用级服务发现；
     * 3. FORCE_INTERFACE（强制接口级）：只使用接口级服务发现。
     * </p>
     * <p>
     * 在APPLICATION_FIRST模式下，还会通过随机数计算调用比例，实现灰度切换：
     * - promotion值越小，越倾向于回退到接口级；
     * - promotion值为100时，全部走应用级；
     * - 每次调用都会检查当前选择的Invoker是否可用。
     * </p>
     *
     * @param invocation RPC调用信息，包含方法名、参数类型、参数值等
     * @return Result 调用结果，可能是同步或异步返回
     * @throws RpcException 当调用失败或服务不可用时抛出异常
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        // 如果已有可用的Invoker，根据迁移模式决定调用路径
        if (currentAvailableInvoker != null) {
            if (step == APPLICATION_FIRST) {
                // 在应用优先模式下，根据promotion比例计算是否回退到接口级调用
                if (promotion < 100 && ThreadLocalRandom.current().nextDouble(100) > promotion) {
                    // 回退到接口级Invoker（传统的服务发现方式）
                    return invoker.invoke(invocation);
                }
                // 检查并决定使用哪个Invoker（可能因地址变化而重新选择）
                return decideInvoker().invoke(invocation);
            }
            // 非应用优先模式，直接使用当前已选定的Invoker
            return currentAvailableInvoker.invoke(invocation);
        }

        // 首次调用或currentAvailableInvoker为空时，根据迁移模式初始化选择Invoker
        switch (step) {
            case APPLICATION_FIRST:
                // 应用优先模式：通过算法决定使用接口级还是应用级Invoker
                currentAvailableInvoker = decideInvoker();
                break;
            case FORCE_APPLICATION:
                // 强制应用级模式：直接使用基于服务发现的Invoker
                currentAvailableInvoker = serviceDiscoveryInvoker;
                break;
            case FORCE_INTERFACE:
            default:
                // 强制接口级模式（默认）：使用传统的接口级Invoker
                currentAvailableInvoker = invoker;
        }

        // 执行最终的RPC调用
        return currentAvailableInvoker.invoke(invocation);
    }

    private ClusterInvoker<T> decideInvoker() {
        if (currentAvailableInvoker == serviceDiscoveryInvoker) {
            if (checkInvokerAvailable(serviceDiscoveryInvoker)) {
                return serviceDiscoveryInvoker;
            }
            return invoker;
        } else {
            return currentAvailableInvoker;
        }
    }

    @Override
    public boolean isAvailable() {
        return currentAvailableInvoker != null
                ? currentAvailableInvoker.isAvailable()
                : (invoker != null && invoker.isAvailable())
                        || (serviceDiscoveryInvoker != null && serviceDiscoveryInvoker.isAvailable());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void destroy() {
        if (migrationRuleListener != null) {
            migrationRuleListener.removeMigrationInvoker(this);
        }
        if (invoker != null) {
            invoker.destroy();
        }
        if (serviceDiscoveryInvoker != null) {
            serviceDiscoveryInvoker.destroy();
        }
        if (consumerModel != null) {
            Object object =
                    consumerModel.getServiceMetadata().getAttribute(CommonConstants.CURRENT_CLUSTER_INVOKER_KEY);
            Map<Registry, MigrationInvoker<?>> invokerMap;
            if (object instanceof Map) {
                invokerMap = (Map<Registry, MigrationInvoker<?>>) object;
                invokerMap.remove(registry);
                if (invokerMap.isEmpty()) {
                    consumerModel
                            .getServiceMetadata()
                            .getAttributeMap()
                            .remove(CommonConstants.CURRENT_CLUSTER_INVOKER_KEY);
                }
            }
        }
    }

    @Override
    public URL getUrl() {
        if (currentAvailableInvoker != null) {
            return currentAvailableInvoker.getUrl();
        } else if (invoker != null) {
            return invoker.getUrl();
        } else if (serviceDiscoveryInvoker != null) {
            return serviceDiscoveryInvoker.getUrl();
        }

        return consumerUrl;
    }

    @Override
    public URL getRegistryUrl() {
        if (currentAvailableInvoker != null) {
            return currentAvailableInvoker.getRegistryUrl();
        } else if (invoker != null) {
            return invoker.getRegistryUrl();
        } else if (serviceDiscoveryInvoker != null) {
            return serviceDiscoveryInvoker.getRegistryUrl();
        }
        return url;
    }

    @Override
    public Directory<T> getDirectory() {
        if (currentAvailableInvoker != null) {
            return currentAvailableInvoker.getDirectory();
        } else if (invoker != null) {
            return invoker.getDirectory();
        } else if (serviceDiscoveryInvoker != null) {
            return serviceDiscoveryInvoker.getDirectory();
        }
        return null;
    }

    @Override
    public boolean isDestroyed() {
        return currentAvailableInvoker != null
                ? currentAvailableInvoker.isDestroyed()
                : (invoker == null || invoker.isDestroyed())
                        && (serviceDiscoveryInvoker == null || serviceDiscoveryInvoker.isDestroyed());
    }

    @Override
    public boolean isServiceDiscovery() {
        return false;
    }

    @Override
    public MigrationStep getMigrationStep() {
        return step;
    }

    @Override
    public void setMigrationStep(MigrationStep step) {
        this.step = step;
    }

    @Override
    public MigrationRule getMigrationRule() {
        return rule;
    }

    @Override
    public void setMigrationRule(MigrationRule rule) {
        this.rule = rule;
        promotion = rule.getProportion(consumerUrl);
    }

    protected void destroyServiceDiscoveryInvoker() {
        if (this.invoker != null) {
            this.currentAvailableInvoker = this.invoker;
        }
        if (serviceDiscoveryInvoker != null && !serviceDiscoveryInvoker.isDestroyed()) {
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Destroying instance address invokers, will not listen for address changes until re-subscribed, "
                                + type.getName());
            }
            serviceDiscoveryInvoker.destroy();
            serviceDiscoveryInvoker = null;
        }
    }

    /**
     * 刷新应用级服务发现的Invoker，重新订阅应用级别的注册中心地址。
     * <p>
     * 该方法负责创建或更新基于应用注册的Invoker（Dubbo3新模式），订阅路径格式为：
     * /dubbo/{appName}/providers。在迁移场景中，当配置变更或地址推送时会触发此方法，与refreshInterfaceInvoker形成对称操作。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>清除旧监听器</b>：调用clearListener移除之前设置的InvokersChangedListener，避免重复监听导致内存泄漏</li>
     *   <li><b>判断是否需要刷新</b>：通过needRefresh检查serviceDiscoveryInvoker是否为null、已销毁或没有可用的提供者列表</li>
     *   <li><b>销毁旧Invoker</b>：如果已有serviceDiscoveryInvoker但不是可用状态，先调用destroy释放资源（断开连接、取消订阅）</li>
     *   <li><b>创建新Invoker</b>：调用registryProtocol.getServiceDiscoveryInvoker重新创建应用级ClusterInvoker，建立与应用实例的连接</li>
     *   <li><b>设置监听器</b>：注册InvokersChangedListener，当地址变更时触发以下操作：
     *     <ul>
     *       <li>latch.countDown()：通知等待线程地址已更新（用于FORCE模式的阈值比较）</li>
     *       <li>上报消费指标：通过reportService记录应用级消费的监控数据，标识为"app"类型</li>
     *       <li>重新计算优选Invoker：当处于APPLICATION_FIRST模式时，调用calcPreferredInvoker动态调整优先使用的Invoker</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param latch 倒计时锁存器，用于异步地址通知的同步等待。在APPLICATION_FIRST模式下传入初始值为0的latch（不等待），在FORCE模式下传入初始值为1的latch（等待一次地址推送）
     */
    protected void refreshServiceDiscoveryInvoker(CountDownLatch latch) {
        /*
         * 清除旧监听器：
         * 避免在刷新Invoker时保留过期的地址变更监听逻辑
         */
        clearListener(serviceDiscoveryInvoker);
        if (needRefresh(serviceDiscoveryInvoker)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Re-subscribing instance addresses, current interface " + type.getName());
            }

            if (serviceDiscoveryInvoker != null) {
                serviceDiscoveryInvoker.destroy();
            }
            /*
             * 创建新的应用级Invoker：
             * 通过RegistryProtocol重新订阅应用级别的提供者地址
             */
            serviceDiscoveryInvoker = registryProtocol.getServiceDiscoveryInvoker(cluster, registry, type, url);
        }
        /*
         * 设置地址变更监听器：
         * 当注册中心推送新的应用实例列表时，触发回调进行后续处理
         */
        setListener(serviceDiscoveryInvoker, () -> {
            latch.countDown();
            if (reportService.hasReporter()) {
                reportService.reportConsumptionStatus(reportService.createConsumptionReport(
                        consumerUrl.getServiceInterface(), consumerUrl.getVersion(), consumerUrl.getGroup(), "app"));
            }
            /*
             * APPLICATION_FIRST模式下的动态调整：
             * 地址变更后重新计算应该优先使用哪个Invoker（应用级还是接口级）
             */
            if (step == APPLICATION_FIRST) {
                calcPreferredInvoker(rule);
            }
        });
    }

    /**
     * 刷新接口级服务发现的Invoker，重新订阅接口级别的注册中心地址。
     * <p>
     * 该方法负责创建或更新基于接口注册的Invoker（传统Dubbo2模式），订阅路径格式为：
     * /dubbo/{interfaceName}:{version}/providers。在迁移场景中，当配置变更或地址推送时会触发此方法。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>清除旧监听器</b>：调用clearListener移除之前设置的InvokersChangedListener，避免重复监听导致内存泄漏</li>
     *   <li><b>判断是否需要刷新</b>：通过needRefresh检查invoker是否为null、已销毁或没有可用的提供者列表</li>
     *   <li><b>销毁旧Invoker</b>：如果已有invoker但不是可用状态，先调用destroy释放资源（断开连接、取消订阅）</li>
     *   <li><b>创建新Invoker</b>：调用registryProtocol.getInvoker重新创建接口级ClusterInvoker，建立与提供者的连接</li>
     *   <li><b>设置监听器</b>：注册InvokersChangedListener，当地址变更时触发以下操作：
     *     <ul>
     *       <li>latch.countDown()：通知等待线程地址已更新（用于FORCE模式的阈值比较）</li>
     *       <li>上报消费指标：通过reportService记录接口级消费的监控数据</li>
     *       <li>重新计算优选Invoker：当处于APPLICATION_FIRST模式时，调用calcPreferredInvoker动态调整优先使用的Invoker</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param latch 倒计时锁存器，用于异步地址通知的同步等待。在APPLICATION_FIRST模式下传入初始值为0的latch（不等待），在FORCE模式下传入初始值为1的latch（等待一次地址推送）
     */
    protected void refreshInterfaceInvoker(CountDownLatch latch) {
        /*
         * 清除旧监听器：
         * 避免在刷新Invoker时保留过期的地址变更监听逻辑
         */
        clearListener(invoker);
        if (needRefresh(invoker)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Re-subscribing interface addresses for interface " + type.getName());
            }

            if (invoker != null) {
                invoker.destroy();
            }
            /*
             * 创建新的接口级Invoker：
             * 通过RegistryProtocol重新订阅接口级别的提供者地址
             */
            invoker = registryProtocol.getInvoker(cluster, registry, type, url);
        }
        /*
         * 设置地址变更监听器：
         * 当注册中心推送新的提供者列表时，触发回调进行后续处理
         */
        setListener(invoker, () -> {
            latch.countDown();
            if (reportService.hasReporter()) {
                reportService.reportConsumptionStatus(reportService.createConsumptionReport(
                        consumerUrl.getServiceInterface(),
                        consumerUrl.getVersion(),
                        consumerUrl.getGroup(),
                        "interface"));
            }
            /*
             * APPLICATION_FIRST模式下的动态调整：
             * 地址变更后重新计算应该优先使用哪个Invoker（应用级还是接口级）
             */
            if (step == APPLICATION_FIRST) {
                calcPreferredInvoker(rule);
            }
        });
    }

    /**
     * 计算并设置优选的Invoker，根据迁移规则动态选择应用级或接口级服务发现。
     * <p>
     * 该方法是APPLICATION_FIRST模式下的核心决策逻辑，负责在双订阅场景中智能选择当前应该优先使用的Invoker。
     * 通过SPI机制加载所有MigrationAddressComparator实现，只有当所有比较器都认为应该迁移时，才切换到应用级Invoker。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>前置校验</b>：如果serviceDiscoveryInvoker或invoker任一为空，直接返回，避免空指针异常</li>
     *   <li><b>加载比较器</b>：通过SPI获取所有MigrationAddressComparator扩展实现（如ThresholdMigrationAddressComparator）</li>
     *   <li><b>全票判断</b>：使用stream.allMatch检查所有比较器是否都返回true，即都认为应该从接口级迁移到应用级</li>
     *   <li><b>设置优选Invoker</b>：
     *     <ul>
     *       <li>全部同意迁移：currentAvailableInvoker = serviceDiscoveryInvoker（优先使用应用级）</li>
     *       <li>任意一个反对：currentAvailableInvoker = invoker（回退到接口级）</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     * <p>
     * 注意事项：
     * <ul>
     *   <li>该方法被synchronized修饰，保证多线程环境下的线程安全</li>
     *   <li>实际的调用链路由还会受到promotion参数影响，即使设置了currentAvailableInvoker，也可能按概率降级</li>
     *   <li>该方法会在地址变更通知时被重新触发，确保动态响应提供者变化</li>
     * </ul>
     * </p>
     *
     * @param migrationRule 迁移规则对象，包含阈值、force标志等配置参数，供比较器进行决策判断
     */
    private synchronized void calcPreferredInvoker(MigrationRule migrationRule) {
        if (serviceDiscoveryInvoker == null || invoker == null) {
            return;
        }
        /*
         * 加载迁移地址比较器：
         * 通过SPI机制获取所有MigrationAddressComparator实现，用于判断是否满足迁移条件
         */
        Set<MigrationAddressComparator> detectors = ScopeModelUtil.getApplicationModel(
                        consumerUrl == null ? null : consumerUrl.getScopeModel())
                .getExtensionLoader(MigrationAddressComparator.class)
                .getSupportedExtensionInstances();
        if (CollectionUtils.isNotEmpty(detectors)) {
            /*
             * 全票通过原则：
             * 只有当所有比较器都认为应该迁移到应用级时，才切换currentAvailableInvoker
             * 否则保持使用接口级Invoker作为降级方案
             */
            // pick preferred invoker
            // the real invoker choice in invocation will be affected by promotion
            if (detectors.stream()
                    .allMatch(
                            comparator -> comparator.shouldMigrate(serviceDiscoveryInvoker, invoker, migrationRule))) {
                this.currentAvailableInvoker = serviceDiscoveryInvoker;
            } else {
                this.currentAvailableInvoker = invoker;
            }
        }
    }

    protected void destroyInterfaceInvoker() {
        if (this.serviceDiscoveryInvoker != null) {
            this.currentAvailableInvoker = this.serviceDiscoveryInvoker;
        }
        if (invoker != null && !invoker.isDestroyed()) {
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Destroying interface address invokers, will not listen for address changes until re-subscribed, "
                                + type.getName());
            }
            invoker.destroy();
            invoker = null;
        }
    }

    private void clearListener(ClusterInvoker<T> invoker) {
        if (invoker == null) {
            return;
        }
        DynamicDirectory<T> directory = (DynamicDirectory<T>) invoker.getDirectory();
        directory.setInvokersChangedListener(null);
    }

    private void setListener(ClusterInvoker<T> invoker, InvokersChangedListener listener) {
        if (invoker == null) {
            return;
        }
        DynamicDirectory<T> directory = (DynamicDirectory<T>) invoker.getDirectory();
        directory.setInvokersChangedListener(listener);
    }

    private boolean needRefresh(ClusterInvoker<T> invoker) {
        return invoker == null || invoker.isDestroyed() || !invoker.hasProxyInvokers();
    }

    public boolean checkInvokerAvailable(ClusterInvoker<T> invoker) {
        return invoker != null && !invoker.isDestroyed() && invoker.isAvailable();
    }

    protected void setCurrentAvailableInvoker(ClusterInvoker<T> currentAvailableInvoker) {
        this.currentAvailableInvoker = currentAvailableInvoker;
    }

    protected void setMigrationRuleListener(MigrationRuleListener migrationRuleListener) {
        this.migrationRuleListener = migrationRuleListener;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public URL getConsumerUrl() {
        return consumerUrl;
    }

    @Override
    public String toString() {
        return "MigrationInvoker{" + "serviceKey="
                + consumerUrl.getServiceKey() + ", invoker="
                + decideInvoker() + ", step="
                + step + '}';
    }
}
