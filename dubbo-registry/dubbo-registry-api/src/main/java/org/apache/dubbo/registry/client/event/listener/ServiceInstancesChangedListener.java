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
package org.apache.dubbo.registry.client.event.listener;

import org.apache.dubbo.common.ProtocolServiceKey;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.MetadataInfo.ServiceInfo;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.registry.event.RegistryEvent;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.RetryServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceNotificationCustomizer;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_REFRESH_ADDRESS;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_ENABLE_EMPTY_PROTECTION;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.ENABLE_EMPTY_PROTECTION_KEY;
import static org.apache.dubbo.metadata.RevisionResolver.EMPTY_REVISION;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;

/**
 * TODO, refactor to move revision-metadata mapping to ServiceDiscovery. Instances should have already been mapped with metadata when reached here.
 * <p>
 * The operations of ServiceInstancesChangedListener should be synchronized.
 */
public class ServiceInstancesChangedListener {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(ServiceInstancesChangedListener.class);

    protected final Set<String> serviceNames;
    protected final ServiceDiscovery serviceDiscovery;
    protected ConcurrentHashMap<String, Set<NotifyListenerWithKey>> listeners;

    protected AtomicBoolean destroyed = new AtomicBoolean(false);

    protected Map<String, List<ServiceInstance>> allInstances;
    protected Map<String, List<ProtocolServiceKeyWithUrls>> serviceUrls;

    private volatile long lastRefreshTime;
    private final Semaphore retryPermission;
    private volatile ScheduledFuture<?> retryFuture;
    private final ScheduledExecutorService scheduler;
    private volatile boolean hasEmptyMetadata;
    private final Set<ServiceInstanceNotificationCustomizer> serviceInstanceNotificationCustomizers;
    private final ApplicationModel applicationModel;

    public ServiceInstancesChangedListener(Set<String> serviceNames, ServiceDiscovery serviceDiscovery) {
        this.serviceNames = serviceNames;
        this.serviceDiscovery = serviceDiscovery;
        this.listeners = new ConcurrentHashMap<>();
        this.allInstances = new HashMap<>();
        this.serviceUrls = new HashMap<>();
        retryPermission = new Semaphore(1);
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(
                serviceDiscovery == null || serviceDiscovery.getUrl() == null
                        ? null
                        : serviceDiscovery.getUrl().getScopeModel());
        this.scheduler = applicationModel
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getMetadataRetryExecutor();
        this.serviceInstanceNotificationCustomizers = applicationModel
                .getExtensionLoader(ServiceInstanceNotificationCustomizer.class)
                .getSupportedExtensionInstances();
        this.applicationModel = applicationModel;
    }

    /**
     * On {@link ServiceInstancesChangedEvent the service instances change event}
     *
     * @param event {@link ServiceInstancesChangedEvent}
     */
    public void onEvent(ServiceInstancesChangedEvent event) {
        if (destroyed.get() || !accept(event) || isRetryAndExpired(event)) {
            return;
        }
        doOnEvent(event);
    }

    /**
     * 处理服务实例变更事件，刷新本地缓存并通知所有订阅者更新Invoker。
     * <p>
     * 该方法是应用级服务发现地址变更的核心处理逻辑，当Zookeeper/Nacos等注册中心推送实例变化事件时被触发。
     * 负责将原始实例列表按Revision分组、拉取元数据、构建URL缓存，最终通知所有接口的NotifyListener重建Invoker链。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>前置校验</b>：检查监听器是否已销毁、事件是否属于当前应用、是否为重试且已过期的事件，任一条件满足则直接返回</li>
     *   <li><b>刷新实例缓存</b>：调用refreshInstance更新allInstances Map，维护应用名到实例列表的映射关系</li>
     *   <li><b>按Revision分组</b>：遍历所有实例，根据exportedServicesRevision参数将同一版本的实例归类到一起，形成revisionToInstances Map</li>
     *   <li><b>获取元数据</b>：
     *     <ul>
     *       <li>优先从现有实例的serviceMetadata中提取指定revision的MetadataInfo</li>
     *       <li>如果本地没有，调用serviceDiscovery.getRemoteMetadata从远程元数据中心拉取（如Zookeeper/MetaServer）</li>
     *       <li>将元数据回填到每个实例中，确保新创建的实例也能拿到完整的接口定义信息</li>
     *     </ul>
     *   </li>
     *   <li><b>解析元数据</b>：调用parseMetadata从MetadataInfo中提取所有服务接口的ProtocolServiceKey，建立serviceInfo到revisions的映射</li>
     *   <li><b>空元数据检查</b>：统计metadata为空的revision数量，如果全部为空则提交重试任务（5秒后重试），暂时不通知地址变更</li>
     *   <li><b>构建URL缓存</b>：
     *     <ul>
     *       <li>遍历localServiceToRevisions，为每个ServiceInfo（接口+协议+端口）生成对应的URL列表</li>
     *       <li>调用getServiceUrlsCache将revision对应的实例列表转换为Dubbo URL格式（如：tri://192.168.1.100:20880?revision=xxx）</li>
     *       <li>按protocolServiceKey组织成newServiceUrls Map，便于后续快速查找</li>
     *     </ul>
     *   </li>
     *   <li><b>更新缓存</b>：将newServiceUrls赋值给this.serviceUrls，替换旧的地址缓存</li>
     *   <li><b>通知地址变更</b>：调用notifyAddressChanged遍历所有订阅的接口，触发Directory.refresh刷新Invoker链</li>
     *   <li><b>重试机制</b>：如果仍有部分元数据为空，提交RetryTask到延迟队列，等待元数据中心恢复后重新拉取</li>
     * </ol>
     * </p>
     *
     * @param event 服务实例变更事件，包含应用名、新增/删除/更新的实例列表等信息，由注册中心事件监听器触发
     */
    private synchronized void doOnEvent(ServiceInstancesChangedEvent event) {
        if (destroyed.get() || !accept(event) || isRetryAndExpired(event)) {
            return;
        }

        refreshInstance(event);

        if (logger.isDebugEnabled()) {
            logger.debug(event.getServiceInstances().toString());
        }

        Map<String, List<ServiceInstance>> revisionToInstances = new HashMap<>();
        Map<ServiceInfo, Set<String>> localServiceToRevisions = new HashMap<>();

        /*
         * 按Revision分组实例：
         * 将同一应用下的所有实例按照metadata revision归类，相同revision的实例共享同一份元数据
         */
        for (Map.Entry<String, List<ServiceInstance>> entry : allInstances.entrySet()) {
            List<ServiceInstance> instances = entry.getValue();
            for (ServiceInstance instance : instances) {
                String revision = getExportedServicesRevision(instance);
                if (revision == null || EMPTY_REVISION.equals(revision)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Find instance without valid service metadata: " + instance.getAddress());
                    }
                    continue;
                }
                List<ServiceInstance> subInstances =
                        revisionToInstances.computeIfAbsent(revision, r -> new LinkedList<>());
                subInstances.add(instance);
            }
        }

        /*
         * 获取并填充元数据：
         * 优先从本地实例缓存中获取MetadataInfo，缺失时从远程元数据中心拉取
         */
        // get MetadataInfo with revision
        for (Map.Entry<String, List<ServiceInstance>> entry : revisionToInstances.entrySet()) {
            String revision = entry.getKey();
            List<ServiceInstance> subInstances = entry.getValue();

            MetadataInfo metadata = subInstances.stream()
                    .map(ServiceInstance::getServiceMetadata)
                    .filter(Objects::nonNull)
                    .filter(m -> revision.equals(m.getRevision()))
                    .findFirst()
                    .orElseGet(() -> serviceDiscovery.getRemoteMetadata(revision, subInstances));

            parseMetadata(revision, metadata, localServiceToRevisions);
            /*
             * 更新实例元数据：
             * 确保每个实例都持有最新的MetadataInfo，新实例创建时能直接使用
             */
            for (ServiceInstance tmpInstance : subInstances) {
                MetadataInfo originMetadata = tmpInstance.getServiceMetadata();
                if (originMetadata == null || !Objects.equals(originMetadata.getRevision(), metadata.getRevision())) {
                    tmpInstance.setServiceMetadata(metadata);
                }
            }
        }

        /*
         * 空元数据检查：
         * 如果所有revision的元数据都为空，说明元数据中心不可用，提交重试任务暂不通知
         */
        int emptyNum = hasEmptyMetadata(revisionToInstances);
        if (emptyNum != 0) {
            hasEmptyMetadata = true;

            // return if all metadata is empty, this notification will not take effect.
            if (emptyNum == revisionToInstances.size()) {
                // 1-17 - Address refresh failed.
                logger.error(
                        REGISTRY_FAILED_REFRESH_ADDRESS,
                        "metadata Server failure",
                        "",
                        "Address refresh failed because of Metadata Server failure, wait for retry or new address refresh event.");

                submitRetryTask(event);
                return;
            }
        } else {
            hasEmptyMetadata = false;
        }

        /*
         * 构建URL缓存：
         * 将revision分组转换为Dubbo URL格式，按protocolServiceKey组织便于后续路由和负载均衡使用
         */
        Map<String, Map<Integer, Map<Set<String>, Object>>> protocolRevisionsToUrls = new HashMap<>();
        Map<String, List<ProtocolServiceKeyWithUrls>> newServiceUrls = new HashMap<>();
        for (Map.Entry<ServiceInfo, Set<String>> entry : localServiceToRevisions.entrySet()) {
            ServiceInfo serviceInfo = entry.getKey();
            Set<String> revisions = entry.getValue();

            Map<Integer, Map<Set<String>, Object>> portToRevisions =
                    protocolRevisionsToUrls.computeIfAbsent(serviceInfo.getProtocol(), k -> new HashMap<>());
            Map<Set<String>, Object> revisionsToUrls =
                    portToRevisions.computeIfAbsent(serviceInfo.getPort(), k -> new HashMap<>());
            Object urls = revisionsToUrls.computeIfAbsent(
                    revisions,
                    k -> getServiceUrlsCache(
                            revisionToInstances, revisions, serviceInfo.getProtocol(), serviceInfo.getPort()));

            List<ProtocolServiceKeyWithUrls> list =
                    newServiceUrls.computeIfAbsent(serviceInfo.getPath(), k -> new LinkedList<>());
            list.add(new ProtocolServiceKeyWithUrls(serviceInfo.getProtocolServiceKey(), (List<URL>) urls));
        }

        this.serviceUrls = newServiceUrls;
        /*
         * 通知所有订阅者：
         * 触发Directory.refresh重建Invoker链，完成地址变更的最终生效
         */
        this.notifyAddressChanged();

        if (hasEmptyMetadata) {
            submitRetryTask(event);
        }
    }

    private void submitRetryTask(ServiceInstancesChangedEvent event) {
        // retry every 10 seconds
        if (retryPermission.tryAcquire()) {
            if (retryFuture != null && !retryFuture.isDone()) {
                // cancel last retryFuture because only one retryFuture will be canceled at destroy().
                retryFuture.cancel(true);
            }
            try {
                retryFuture = scheduler.schedule(
                        new AddressRefreshRetryTask(retryPermission, event.getServiceName()),
                        10_000L,
                        TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.error(
                        INTERNAL_ERROR, "unknown error in registry module", "", "Error submitting async retry task.");
            }
            logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", "Address refresh try task submitted");
        }
    }

    /**
     * 添加通知监听器并立即触发一次地址聚合通知
     * 该方法用于在服务订阅阶段注册监听器，并确保消费者能立即获取到当前可用的服务地址列表
     *
     * @param url 订阅的服务URL，包含接口名、版本、分组等标识信息
     * @param listener 通知监听器，当服务地址发生变更时接收回调
     */
    public synchronized void addListenerAndNotify(URL url, NotifyListener listener) {
        // 如果监听器已销毁，则直接返回不再处理
        if (destroyed.get()) {
            return;
        }

        // 根据服务键（ServiceKey）获取或创建对应的监听器集合，确保同一服务的多个监听器能被统一管理
        Set<NotifyListenerWithKey> notifyListeners = ConcurrentHashMapUtils.computeIfAbsent(
                this.listeners, url.getServiceKey(), _k -> new ConcurrentHashSet<>());

        // 提取协议信息并构建协议服务键，用于精确匹配特定协议下的服务实例
        String protocol = listener.getConsumerUrl().getParameter(PROTOCOL_KEY, url.getProtocol());
        ProtocolServiceKey protocolServiceKey = new ProtocolServiceKey(
                url.getServiceInterface(),
                url.getVersion(),
                url.getGroup(),
                !CommonConstants.CONSUMER.equals(protocol) ? protocol : null);

        // 封装监听器与协议服务键，存入集合中
        NotifyListenerWithKey listenerWithKey = new NotifyListenerWithKey(protocolServiceKey, listener);
        notifyListeners.add(listenerWithKey);

        // Aggregate address and notify on subscription.
        // 聚合当前所有可用的服务地址，并在订阅时立即通知监听器，实现“订阅即发现”
        List<URL> urls = getAddresses(protocolServiceKey, listener.getConsumerUrl());

        if (CollectionUtils.isNotEmpty(urls)) {
            logger.info(String.format(
                    "Notify serviceKey: %s, listener: %s with %s urls on subscription",
                    protocolServiceKey, listener, urls.size()));
            listener.notify(urls);
        }
    }


    public synchronized void removeListener(String serviceKey, NotifyListener notifyListener) {
        if (destroyed.get()) {
            return;
        }

        // synchronized method, no need to use DCL
        Set<NotifyListenerWithKey> notifyListeners = this.listeners.get(serviceKey);
        if (notifyListeners != null) {
            notifyListeners.removeIf(listener -> listener.getNotifyListener().equals(notifyListener));

            // ServiceKey has no listener, remove set
            if (notifyListeners.isEmpty()) {
                this.listeners.remove(serviceKey);
            }
        }
    }

    public boolean hasListeners() {
        return CollectionUtils.isNotEmptyMap(listeners);
    }

    /**
     * Get the correlative service name
     *
     * @return the correlative service name
     */
    public final Set<String> getServiceNames() {
        return serviceNames;
    }

    public Map<String, List<ServiceInstance>> getAllInstances() {
        return allInstances;
    }

    /**
     * @param event {@link ServiceInstancesChangedEvent event}
     * @return If service name matches, return <code>true</code>, or <code>false</code>
     */
    private boolean accept(ServiceInstancesChangedEvent event) {
        return serviceNames.contains(event.getServiceName());
    }

    protected boolean isRetryAndExpired(ServiceInstancesChangedEvent event) {
        if (event instanceof RetryServiceInstancesChangedEvent) {
            RetryServiceInstancesChangedEvent retryEvent = (RetryServiceInstancesChangedEvent) event;
            logger.warn(
                    INTERNAL_ERROR,
                    "unknown error in registry module",
                    "",
                    "Received address refresh retry event, " + retryEvent.getFailureRecordTime());
            if (retryEvent.getFailureRecordTime() < lastRefreshTime && !hasEmptyMetadata) {
                logger.warn(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Ignore retry event, event time: " + retryEvent.getFailureRecordTime() + ", last refresh time: "
                                + lastRefreshTime);
                return true;
            }
            logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", "Retrying address notification...");
        }
        return false;
    }

    private void refreshInstance(ServiceInstancesChangedEvent event) {
        if (event instanceof RetryServiceInstancesChangedEvent) {
            return;
        }
        String appName = event.getServiceName();
        List<ServiceInstance> appInstances = event.getServiceInstances();
        logger.info("Received instance notification, serviceName: " + appName + ", instances: " + appInstances.size());
        for (ServiceInstanceNotificationCustomizer serviceInstanceNotificationCustomizer :
                serviceInstanceNotificationCustomizers) {
            serviceInstanceNotificationCustomizer.customize(appInstances);
        }
        allInstances.put(appName, appInstances);
        lastRefreshTime = System.currentTimeMillis();
    }

    /**
     * Calculate the number of revisions that failed to find metadata info.
     *
     * @param revisionToInstances instance list classified by revisions
     * @return the number of revisions that failed at fetching MetadataInfo
     */
    protected int hasEmptyMetadata(Map<String, List<ServiceInstance>> revisionToInstances) {
        if (revisionToInstances == null) {
            return 0;
        }

        StringBuilder builder = new StringBuilder();
        int emptyMetadataNum = 0;
        for (Map.Entry<String, List<ServiceInstance>> entry : revisionToInstances.entrySet()) {
            DefaultServiceInstance serviceInstance =
                    (DefaultServiceInstance) entry.getValue().get(0);
            if (serviceInstance == null || serviceInstance.getServiceMetadata() == MetadataInfo.EMPTY) {
                emptyMetadataNum++;
            }

            builder.append(entry.getKey());
            builder.append(' ');
        }

        if (emptyMetadataNum > 0) {
            builder.insert(
                    0,
                    emptyMetadataNum + "/" + revisionToInstances.size()
                            + " revisions failed to get metadata from remote: ");
            logger.error(INTERNAL_ERROR, "unknown error in registry module", "", builder.toString());
        } else {
            builder.insert(0, revisionToInstances.size() + " unique working revisions: ");
            logger.info(builder.toString());
        }
        return emptyMetadataNum;
    }

    /**
     * 解析元数据信息，建立服务信息与版本号（Revision）之间的映射关系
     * 将当前实例上报的所有服务接口关联到指定的Revision，用于后续的版本比对和变更检测
     *
     * @param revision 当前实例的元数据版本号
     * @param metadata 从远程获取的应用级元数据对象，包含该应用下所有服务的详细信息
     * @param localServiceToRevisions 本地维护的服务到Revision集合的映射表，该方法会向其中追加新的映射关系
     * @return 更新后的服务到Revision集合的映射表
     */
    protected Map<ServiceInfo, Set<String>> parseMetadata(
            String revision, MetadataInfo metadata, Map<ServiceInfo, Set<String>> localServiceToRevisions) {
        // 获取元数据中包含的所有服务信息
        Map<String, ServiceInfo> serviceInfos = metadata.getServices();

        // 遍历所有服务，将当前Revision添加到对应服务的版本集合中
        for (Map.Entry<String, ServiceInfo> entry : serviceInfos.entrySet()) {
            // 如果该服务尚未在映射表中，则创建一个新的TreeSet来存储其关联的Revisions
            Set<String> set = localServiceToRevisions.computeIfAbsent(entry.getValue(), _k -> new TreeSet<>());
            set.add(revision);
        }

        return localServiceToRevisions;
    }


    protected Object getServiceUrlsCache(
            Map<String, List<ServiceInstance>> revisionToInstances, Set<String> revisions, String protocol, int port) {
        List<URL> urls = new ArrayList<>();
        for (String r : revisions) {
            for (ServiceInstance i : revisionToInstances.get(r)) {
                if (port > 0) {
                    if (i.getPort() == port) {
                        urls.add(i.toURL(protocol).setScopeModel(i.getApplicationModel()));
                    } else {
                        urls.add(((DefaultServiceInstance) i)
                                .copyFrom(port)
                                .toURL(protocol)
                                .setScopeModel(i.getApplicationModel()));
                    }
                    continue;
                }
                // different protocols may have ports specified in meta
                if (ServiceInstanceMetadataUtils.hasEndpoints(i)) {
                    DefaultServiceInstance.Endpoint endpoint = ServiceInstanceMetadataUtils.getEndpoint(i, protocol);
                    if (endpoint != null && endpoint.getPort() != i.getPort()) {
                        urls.add(((DefaultServiceInstance) i).copyFrom(endpoint).toURL(endpoint.getProtocol()));
                        continue;
                    }
                }
                urls.add(i.toURL(protocol).setScopeModel(i.getApplicationModel()));
            }
        }
        return urls;
    }

    protected List<URL> getAddresses(ProtocolServiceKey protocolServiceKey, URL consumerURL) {
        List<ProtocolServiceKeyWithUrls> protocolServiceKeyWithUrlsList =
                serviceUrls.get(protocolServiceKey.getInterfaceName());
        List<URL> urls = new ArrayList<>();
        if (protocolServiceKeyWithUrlsList != null) {
            for (ProtocolServiceKeyWithUrls protocolServiceKeyWithUrls : protocolServiceKeyWithUrlsList) {
                if (ProtocolServiceKey.Matcher.isMatch(
                        protocolServiceKey, protocolServiceKeyWithUrls.getProtocolServiceKey())) {
                    urls.addAll(protocolServiceKeyWithUrls.getUrls());
                }
            }
        }
        if (serviceUrls.containsKey(CommonConstants.ANY_VALUE)) {
            for (ProtocolServiceKeyWithUrls protocolServiceKeyWithUrls : serviceUrls.get(CommonConstants.ANY_VALUE)) {
                urls.addAll(protocolServiceKeyWithUrls.getUrls());
            }
        }
        return urls;
    }

    /**
     * race condition is protected by onEvent/doOnEvent
     */
    protected void notifyAddressChanged() {

        MetricsEventBus.post(RegistryEvent.toNotifyEvent(applicationModel), () -> {
            Map<String, Integer> lastNumMap = new HashMap<>();
            // 1 different services
            listeners.forEach((serviceKey, listenerSet) -> {
                // 2 multiple subscription listener of the same service
                for (NotifyListenerWithKey listenerWithKey : listenerSet) {
                    NotifyListener notifyListener = listenerWithKey.getNotifyListener();

                    List<URL> urls = toUrlsWithEmpty(
                            getAddresses(listenerWithKey.getProtocolServiceKey(), notifyListener.getConsumerUrl()));
                    logger.info(
                            "Notify service " + listenerWithKey.getProtocolServiceKey() + " with urls " + urls.size());
                    notifyListener.notify(urls);
                    lastNumMap.put(serviceKey, urls.size());
                }
            });
            return lastNumMap;
        });
    }

    protected List<URL> toUrlsWithEmpty(List<URL> urls) {
        boolean emptyProtectionEnabled =
                serviceDiscovery.getUrl().getParameter(ENABLE_EMPTY_PROTECTION_KEY, DEFAULT_ENABLE_EMPTY_PROTECTION);
        if (!emptyProtectionEnabled && urls == null) {
            urls = new ArrayList<>();
        } else if (emptyProtectionEnabled && urls == null) {
            urls = Collections.emptyList();
        }

        if (CollectionUtils.isEmpty(urls) && !emptyProtectionEnabled) {
            // notice that the service of this.url may not be the same as notify listener.
            URL empty = URLBuilder.from(serviceDiscovery.getUrl())
                    .setProtocol(EMPTY_PROTOCOL)
                    .build();
            urls.add(empty);
        }
        return urls;
    }

    /**
     * Since this listener is shared among interfaces, destroy this listener only when all interface listener are unsubscribed
     */
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            logger.info("Destroying instance listener of  " + this.getServiceNames());
            serviceDiscovery.removeServiceInstancesChangedListener(this);
            synchronized (this) {
                allInstances.clear();
                serviceUrls.clear();
                listeners.clear();
                if (retryFuture != null && !retryFuture.isDone()) {
                    retryFuture.cancel(true);
                }
            }
        }
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServiceInstancesChangedListener)) {
            return false;
        }
        ServiceInstancesChangedListener that = (ServiceInstancesChangedListener) o;
        return Objects.equals(getServiceNames(), that.getServiceNames()) && Objects.equals(listeners, that.listeners);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), getServiceNames());
    }

    protected class AddressRefreshRetryTask implements Runnable {
        private final RetryServiceInstancesChangedEvent retryEvent;
        private final Semaphore retryPermission;

        public AddressRefreshRetryTask(Semaphore semaphore, String serviceName) {
            this.retryEvent = new RetryServiceInstancesChangedEvent(serviceName);
            this.retryPermission = semaphore;
        }

        @Override
        public void run() {
            retryPermission.release();
            ServiceInstancesChangedListener.this.onEvent(retryEvent);
        }
    }

    public static class NotifyListenerWithKey {
        private final ProtocolServiceKey protocolServiceKey;
        private final NotifyListener notifyListener;

        public NotifyListenerWithKey(ProtocolServiceKey protocolServiceKey, NotifyListener notifyListener) {
            this.protocolServiceKey = protocolServiceKey;
            this.notifyListener = notifyListener;
        }

        public ProtocolServiceKey getProtocolServiceKey() {
            return protocolServiceKey;
        }

        public NotifyListener getNotifyListener() {
            return notifyListener;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NotifyListenerWithKey that = (NotifyListenerWithKey) o;
            return Objects.equals(protocolServiceKey, that.protocolServiceKey)
                    && Objects.equals(notifyListener, that.notifyListener);
        }

        @Override
        public int hashCode() {
            return Objects.hash(protocolServiceKey, notifyListener);
        }
    }

    public static class ProtocolServiceKeyWithUrls {
        private final ProtocolServiceKey protocolServiceKey;
        private final List<URL> urls;

        public ProtocolServiceKeyWithUrls(ProtocolServiceKey protocolServiceKey, List<URL> urls) {
            this.protocolServiceKey = protocolServiceKey;
            this.urls = urls;
        }

        public ProtocolServiceKey getProtocolServiceKey() {
            return protocolServiceKey;
        }

        public List<URL> getUrls() {
            return urls;
        }
    }
}
