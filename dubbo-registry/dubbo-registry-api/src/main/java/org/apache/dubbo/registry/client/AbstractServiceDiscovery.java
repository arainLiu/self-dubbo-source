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
package org.apache.dubbo.registry.client;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.metadata.event.MetadataEvent;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.store.MetaCacheManager;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_INFO_CACHE_EXPIRE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_INFO_CACHE_SIZE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_INFO_CACHE_EXPIRE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_INFO_CACHE_SIZE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_LOAD_METADATA;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_KEY;
import static org.apache.dubbo.metadata.RevisionResolver.EMPTY_REVISION;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.EXPORTED_SERVICES_REVISION_PROPERTY_NAME;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.isValidInstance;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.setMetadataStorageType;

/**
 * Each service discovery is bond to one application.
 */
public abstract class AbstractServiceDiscovery implements ServiceDiscovery {
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(AbstractServiceDiscovery.class);
    private volatile boolean isDestroy;

    protected final String serviceName;
    protected volatile ServiceInstance serviceInstance;
    protected volatile MetadataInfo metadataInfo;
    protected final ConcurrentHashMap<String, MetadataInfoStat> metadataInfos = new ConcurrentHashMap<>();
    protected volatile ScheduledFuture<?> refreshCacheFuture;
    protected MetadataReport metadataReport;
    protected String metadataType;
    protected final MetaCacheManager metaCacheManager;
    protected URL registryURL;

    protected Set<ServiceInstancesChangedListener> instanceListeners = new ConcurrentHashSet<>();

    protected ApplicationModel applicationModel;

    public AbstractServiceDiscovery(ApplicationModel applicationModel, URL registryURL) {
        this(applicationModel, applicationModel.getApplicationName(), registryURL);
        MetadataReportInstance metadataReportInstance =
                applicationModel.getBeanFactory().getBean(MetadataReportInstance.class);
        this.metadataType = metadataReportInstance.getMetadataType();
        this.metadataReport = metadataReportInstance.getMetadataReport(registryURL.getParameter(REGISTRY_CLUSTER_KEY));
    }

    public AbstractServiceDiscovery(String serviceName, URL registryURL) {
        this(ApplicationModel.defaultModel(), serviceName, registryURL);
    }

    private AbstractServiceDiscovery(ApplicationModel applicationModel, String serviceName, URL registryURL) {
        this.applicationModel = applicationModel;
        this.serviceName = serviceName;
        this.registryURL = registryURL;
        this.metadataInfo = new MetadataInfo(serviceName);
        boolean localCacheEnabled = registryURL.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        this.metaCacheManager = new MetaCacheManager(
                localCacheEnabled,
                getCacheNameSuffix(),
                applicationModel
                        .getFrameworkModel()
                        .getBeanFactory()
                        .getBean(FrameworkExecutorRepository.class)
                        .getCacheRefreshingScheduledExecutor());
        int metadataInfoCacheExpireTime =
                registryURL.getParameter(METADATA_INFO_CACHE_EXPIRE_KEY, DEFAULT_METADATA_INFO_CACHE_EXPIRE);
        int metadataInfoCacheSize =
                registryURL.getParameter(METADATA_INFO_CACHE_SIZE_KEY, DEFAULT_METADATA_INFO_CACHE_SIZE);
        startRefreshCache(metadataInfoCacheExpireTime / 2, metadataInfoCacheSize, metadataInfoCacheExpireTime);
    }

    private void removeExpiredMetadataInfo(int metadataInfoCacheSize, int metadataInfoCacheExpireTime) {
        Long nextTime = null;
        // Cache cleanup is only required when the cache size exceeds the cache limit.
        if (metadataInfos.size() > metadataInfoCacheSize) {
            List<MetadataInfoStat> values = new ArrayList<>(metadataInfos.values());
            // Place the earliest data at the front
            values.sort(Comparator.comparingLong(MetadataInfoStat::getUpdateTime));
            for (MetadataInfoStat v : values) {
                long time = System.currentTimeMillis() - v.getUpdateTime();
                if (time > metadataInfoCacheExpireTime) {
                    metadataInfos.remove(v.metadataInfo.getRevision(), v);
                } else {
                    // Calculate how long it will take for the next task to start
                    nextTime = metadataInfoCacheExpireTime - time;
                    break;
                }
            }
        }
        // If there is no metadata to clean up this time, the next task will start within half of the cache expiration
        // time.
        startRefreshCache(
                nextTime == null ? metadataInfoCacheExpireTime / 2 : nextTime,
                metadataInfoCacheSize,
                metadataInfoCacheExpireTime);
    }

    private void startRefreshCache(long nextTime, int metadataInfoCacheSize, int metadataInfoCacheExpireTime) {
        this.refreshCacheFuture = applicationModel
                .getFrameworkModel()
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getSharedScheduledExecutor()
                .schedule(
                        () -> removeExpiredMetadataInfo(metadataInfoCacheSize, metadataInfoCacheExpireTime),
                        nextTime,
                        TimeUnit.MILLISECONDS);
    }

    /**
     * 注册当前应用实例到服务发现中心，完成元数据上报和实例注册。
     * <p>
     * 该方法是Dubbo3应用级服务发现的核心实现，负责将当前应用实例信息注册到注册中心（如Nacos、Zookeeper），
     * 使得消费者可以通过应用名查找到该实例。支持基于Revision的增量更新和幂等性控制。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>状态校验</b>：检查服务发现是否已销毁，已销毁则直接返回避免无效操作</li>
     *   <li><b>实例创建</b>：如果serviceInstance为空，调用createServiceInstance创建包含元数据的实例对象</li>
     *   <li><b>有效性验证</b>：通过isValidInstance检查实例的必要字段（主机、端口、应用名等）是否完整</li>
     *   <li><b>Revision计算</b>：调用calOrUpdateInstanceRevision计算元数据的Revision，判断是否有变更</li>
     *   <li><b>元数据上报</b>：将MetadataInfo上报到元数据中心，存储服务接口的详细配置信息</li>
     *   <li><b>实例注册</b>：调用doRegister将实例信息写入注册中心（由子类实现具体逻辑）</li>
     *   <li><b>异常回滚</b>：如果注册失败，清空serviceInstance以便下次重试时重新创建</li>
     * </ol>
     * </p>
     *
     * @throws RuntimeException 当注册中心连接失败、元数据上报异常或实例信息不合法时抛出运行时异常
     */
    @Override
    public synchronized void register() throws RuntimeException {
        if (isDestroy) {
            return;
        }
        /*
         * 延迟创建服务实例：
         * 首次注册时创建ServiceInstance，包含应用名、主机地址、端口、元数据信息等
         */
        if (this.serviceInstance == null) {
            ServiceInstance serviceInstance = createServiceInstance(this.metadataInfo);
            if (!isValidInstance(serviceInstance)) {
                return;
            }
            this.serviceInstance = serviceInstance;
        }
        /*
         * Revision版本校验：
         * 计算当前元数据的Revision并与上次比较，只有发生变更时才执行注册
         * 避免重复注册导致的性能开销和网络流量
         */
        boolean revisionUpdated = calOrUpdateInstanceRevision(this.serviceInstance);
        if (revisionUpdated) {
            try {
                /*
                 * 上报元数据到元数据中心：
                 * 将MetadataInfo（包含所有服务接口信息）存储到远程元数据中心，供消费者查询
                 */
                reportMetadata(this.metadataInfo);
                /*
                 * 执行实例注册：
                 * 调用子类实现（如NacosServiceDiscovery.doRegister）将实例写入注册中心
                 */
                doRegister(this.serviceInstance);
            } catch (Exception e) {
                /*
                 * 注册失败回滚：
                 * 清空serviceInstance，确保下次重试时能够重新创建干净的实例对象
                 */
                this.serviceInstance = null;
                throw e;
            }
        }
    }

    /**
     * Update assumes that DefaultServiceInstance and its attributes will never get updated once created.
     * Checking hasExportedServices() before registration guarantees that at least one service is ready for creating the
     * instance.
     */
    @Override
    public synchronized void update() throws RuntimeException {
        if (isDestroy) {
            return;
        }

        if (this.serviceInstance == null) {
            register();
        }

        if (!isValidInstance(this.serviceInstance)) {
            return;
        }
        ServiceInstance oldServiceInstance = this.serviceInstance;
        DefaultServiceInstance newServiceInstance =
                new DefaultServiceInstance((DefaultServiceInstance) oldServiceInstance);
        boolean revisionUpdated = calOrUpdateInstanceRevision(newServiceInstance);
        if (revisionUpdated) {
            logger.info(String.format(
                    "Metadata of instance changed, updating instance with revision %s.",
                    newServiceInstance.getServiceMetadata().getRevision()));
            doUpdate(oldServiceInstance, newServiceInstance);
            this.serviceInstance = newServiceInstance;
        }
    }

    @Override
    public synchronized void unregister() throws RuntimeException {
        if (isDestroy) {
            return;
        }
        // fixme, this metadata info might still being shared by other instances
        //        unReportMetadata(this.metadataInfo);
        if (!isValidInstance(this.serviceInstance)) {
            return;
        }
        doUnregister(this.serviceInstance);
    }

    @Override
    public final ServiceInstance getLocalInstance() {
        return this.serviceInstance;
    }

    @Override
    public MetadataInfo getLocalMetadata() {
        return this.metadataInfo;
    }

    @Override
    public MetadataInfo getLocalMetadata(String revision) {
        MetadataInfoStat metadataInfoStat = metadataInfos.get(revision);
        if (metadataInfoStat != null) {
            return metadataInfoStat.getMetadataInfo();
        } else {
            return null;
        }
    }

    /**
     * 获取指定Revision的应用级元数据信息，支持本地缓存和远程拉取，具备重试机制。
     * <p>
     * 该方法是Dubbo3应用级服务发现中元数据管理的核心接口，负责从本地缓存或远程存储中获取MetadataInfo对象。
     * 采用"缓存优先+三次重试+指标上报"的策略，保证元数据获取的高可用性和性能。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>查询本地缓存</b>：调用metaCacheManager.get(revision)查找之前已加载的元数据，如果命中且非EMPTY则直接返回，避免重复网络请求</li>
     *   <li><b>初始化元数据</b>：调用metadata.init()解析服务接口列表和方法定义，构建内部索引结构便于后续RPC调用时快速查找</li>
     *   <li><b>加锁同步执行</b>：通过synchronized(metaCacheManager)防止多个线程同时拉取同一revision的元数据，避免并发浪费</li>
     *   <li><b>最多重试3次</b>：
     *     <ul>
     *       <li>调用MetadataUtils.getRemoteMetadata从远程获取元数据（可能是元数据中心或提供者实例）</li>
     *       <li>通过MetricsEventBus.post上报订阅事件，记录成功/失败状态用于监控统计</li>
     *       <li>如果返回EMPTY表示失败，等待1秒后重试，最多尝试3次</li>
     *       <li>每次重试前记录DEBUG日志，便于问题排查</li>
     *     </ul>
     *   </li>
     *   <li><b>结果处理</b>：
     *     <ul>
     *       <li>3次重试后仍为EMPTY：记录ERROR日志，返回EMPTY对象，调用方需处理元数据缺失的场景</li>
     *       <li>成功获取元数据：调用metaCacheManager.put写入本地缓存，供后续查询复用</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param revision  元数据的版本号，用于标识特定时刻的应用配置快照，格式通常为MD5哈希值
     * @param instances 服务实例列表，包含多个相同应用的实例地址，用于从中选择一个进行通信获取元数据
     * @return 应用级元数据信息对象，包含所有服务接口的定义、方法列表、配置参数等，如果3次重试后仍失败则返回EMPTY常量
     */
    @Override
    public MetadataInfo getRemoteMetadata(String revision, List<ServiceInstance> instances) {
        MetadataInfo metadata = metaCacheManager.get(revision);

        if (metadata != null && metadata != MetadataInfo.EMPTY) {
            metadata.init();
            // metadata loaded from cache
            if (logger.isDebugEnabled()) {
                logger.debug("MetadataInfo for revision=" + revision + ", " + metadata);
            }
            return metadata;
        }

        synchronized (metaCacheManager) {
            /*
             * 从远程加载元数据：
             * 最多重试3次，每次间隔1秒，提高在网络抖动或元数据中心短暂不可用时的成功率
             */
            int triedTimes = 0;
            while (triedTimes < 3) {

                metadata = MetricsEventBus.post(
                        MetadataEvent.toSubscribeEvent(applicationModel),
                        () -> MetadataUtils.getRemoteMetadata(revision, instances, metadataReport),
                        result -> result != MetadataInfo.EMPTY);

                if (metadata != MetadataInfo.EMPTY) { // succeeded
                    metadata.init();
                    break;
                } else { // failed
                    if (triedTimes > 0) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Retry the " + triedTimes + " times to get metadata for revision=" + revision);
                        }
                    }
                    triedTimes++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (metadata == MetadataInfo.EMPTY) {
                /*
                 * 3次重试后仍失败：
                 * 记录错误日志，返回EMPTY对象，调用方需处理元数据缺失的情况
                 */
                logger.error(
                        REGISTRY_FAILED_LOAD_METADATA,
                        "",
                        "",
                        "Failed to get metadata for revision after 3 retries, revision=" + revision);
            } else {
                /*
                 * 成功获取元数据：
                 * 写入本地缓存，避免后续重复请求远程服务
                 */
                metaCacheManager.put(revision, metadata);
            }
        }
        return metadata;
    }

    // ... existing code ...


    @Override
    public MetadataInfo getRemoteMetadata(String revision) {
        return metaCacheManager.get(revision);
    }

    @Override
    public final void destroy() throws Exception {
        isDestroy = true;
        metaCacheManager.destroy();
        refreshCacheFuture.cancel(true);
        doDestroy();
    }

    @Override
    public final boolean isDestroy() {
        return isDestroy;
    }

    @Override
    public void register(URL url) {
        metadataInfo.addService(url);
    }

    @Override
    public void unregister(URL url) {
        metadataInfo.removeService(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        metadataInfo.addSubscribedURL(url);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        metadataInfo.removeSubscribedURL(url);
    }

    @Override
    public List<URL> lookup(URL url) {
        throw new UnsupportedOperationException(
                "Service discovery implementation does not support lookup of url list.");
    }

    /**
     * Update Service Instance. Unregister and then register by default.
     * Can be override if registry support update instance directly.
     * <br/>
     * NOTICE: Remind to update {@link AbstractServiceDiscovery#serviceInstance}'s reference if updated
     * and report metadata by {@link AbstractServiceDiscovery#reportMetadata(MetadataInfo)}
     *
     * @param oldServiceInstance origin service instance
     * @param newServiceInstance new service instance
     */
    protected void doUpdate(ServiceInstance oldServiceInstance, ServiceInstance newServiceInstance) {
        this.doUnregister(oldServiceInstance);

        this.serviceInstance = newServiceInstance;

        if (!EMPTY_REVISION.equals(getExportedServicesRevision(newServiceInstance))) {
            reportMetadata(newServiceInstance.getServiceMetadata());
            this.doRegister(newServiceInstance);
        }
    }

    @Override
    public URL getUrl() {
        return registryURL;
    }

    protected abstract void doRegister(ServiceInstance serviceInstance) throws RuntimeException;

    protected abstract void doUnregister(ServiceInstance serviceInstance);

    protected abstract void doDestroy() throws Exception;

    protected ServiceInstance createServiceInstance(MetadataInfo metadataInfo) {
        DefaultServiceInstance instance = new DefaultServiceInstance(serviceName, applicationModel);
        instance.setServiceMetadata(metadataInfo);
        setMetadataStorageType(instance, metadataType);
        ServiceInstanceMetadataUtils.customizeInstance(instance, applicationModel);
        return instance;
    }

    protected boolean calOrUpdateInstanceRevision(ServiceInstance instance) {
        String existingInstanceRevision = getExportedServicesRevision(instance);
        MetadataInfo metadataInfo = instance.getServiceMetadata();
        String newRevision = metadataInfo.calAndGetRevision();
        if (!newRevision.equals(existingInstanceRevision)) {
            instance.getMetadata().put(EXPORTED_SERVICES_REVISION_PROPERTY_NAME, metadataInfo.getRevision());
            return true;
        }
        return false;
    }

    /**
     * 上报应用元数据到元数据中心，并在本地缓存中保存副本。
     * <p>
     * 该方法负责将应用的MetadataInfo（包含所有服务接口的详细配置）推送到远程元数据中心，
     * 同时在本地维护一个带版本控制的元数据缓存，用于历史版本查询和对比。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>空值校验</b>：如果metadataInfo为空则直接返回，避免空指针异常</li>
     *   <li><b>远程存储判断</b>：根据metadataType和shouldReportMetadata判断是否需要上报到远程元数据中心</li>
     *   <li><b>构建标识符</b>：创建SubscriberMetadataIdentifier，使用应用名+Revision作为唯一键</li>
     *   <li><b>指标上报</b>：通过MetricsEventBus发布元数据推送事件，用于监控统计</li>
     *   <li><b>执行发布</b>：调用metadataReport.publishAppMetadata将元数据写入远程存储（如Zookeeper、Nacos）</li>
     *   <li><b>本地缓存</b>：克隆MetadataInfo并存入metadataInfos缓存，key为Revision，便于后续版本对比</li>
     * </ol>
     * </p>
     *
     * @param metadataInfo 应用级别的元数据信息，包含所有已导出服务的接口定义、方法列表、配置参数等
     */
    protected void reportMetadata(MetadataInfo metadataInfo) {
        if (metadataInfo == null) {
            return;
        }
        if (metadataReport != null) {
            /*
             * 构建元数据标识符：
             * 使用应用名和Revision组合作为唯一标识，格式：{appName}:{revision}
             */
            SubscriberMetadataIdentifier identifier =
                    new SubscriberMetadataIdentifier(serviceName, metadataInfo.getRevision());
            /*
             * 判断是否需要上报到远程元数据中心：
             * 条件1：默认存储类型且配置了shouldReportMetadata=true
             * 条件2：显式配置为远程存储类型（remote）
             */
            if ((DEFAULT_METADATA_STORAGE_TYPE.equals(metadataType) && metadataReport.shouldReportMetadata())
                    || REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                /*
                 * 发布元数据并上报指标：
                 * 通过MetricsEventBus记录元数据推送的耗时和成功/失败状态
                 */
                MetricsEventBus.post(MetadataEvent.toPushEvent(applicationModel), () -> {
                    metadataReport.publishAppMetadata(identifier, metadataInfo);
                    return null;
                });
            }
        }
        /*
         * 本地缓存元数据：
         * 克隆MetadataInfo对象避免外部修改影响缓存一致性，按Revision分组存储
         */
        MetadataInfo clonedMetadataInfo = metadataInfo.clone();
        metadataInfos.put(metadataInfo.getRevision(), new MetadataInfoStat(clonedMetadataInfo));
    }

    protected void unReportMetadata(MetadataInfo metadataInfo) {
        if (metadataReport != null) {
            SubscriberMetadataIdentifier identifier =
                    new SubscriberMetadataIdentifier(serviceName, metadataInfo.getRevision());
            if ((DEFAULT_METADATA_STORAGE_TYPE.equals(metadataType) && metadataReport.shouldReportMetadata())
                    || REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                metadataReport.unPublishAppMetadata(identifier, metadataInfo);
            }
        }
    }

    private String getCacheNameSuffix() {
        String name = this.getClass().getSimpleName();
        int i = name.indexOf(ServiceDiscovery.class.getSimpleName());
        if (i != -1) {
            name = name.substring(0, i);
        }
        StringBuilder stringBuilder = new StringBuilder(128);
        Optional<ApplicationConfig> application =
                applicationModel.getApplicationConfigManager().getApplication();
        if (application.isPresent()) {
            stringBuilder.append(application.get().getName());
            stringBuilder.append(".");
        }
        stringBuilder.append(name.toLowerCase());
        URL url = this.getUrl();
        if (url != null) {
            stringBuilder.append(".");
            stringBuilder.append(url.getBackupAddress());
        }
        return stringBuilder.toString();
    }

    private static class MetadataInfoStat {
        private final MetadataInfo metadataInfo;
        private final long updateTime = System.currentTimeMillis();

        public MetadataInfoStat(MetadataInfo metadataInfo) {
            this.metadataInfo = metadataInfo;
        }

        public MetadataInfo getMetadataInfo() {
            return metadataInfo;
        }

        public long getUpdateTime() {
            return updateTime;
        }
    }
}
