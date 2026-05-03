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
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.metadata.AbstractServiceNameMapping;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.registry.event.RegistryEvent;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_TYPE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_TYPE;
import static org.apache.dubbo.common.function.ThrowableAction.execute;
import static org.apache.dubbo.common.utils.CollectionUtils.toTreeSet;
import static org.apache.dubbo.metadata.ServiceNameMapping.toStringKeys;
import static org.apache.dubbo.registry.client.ServiceDiscoveryFactory.getExtension;

/**
 * TODO, this bridge implementation is not necessary now, protocol can interact with service discovery directly.
 * <p>
 * ServiceDiscoveryRegistry is a very special Registry implementation, which is used to bridge the old interface-level service discovery model
 * with the new service discovery model introduced in 3.0 in a compatible manner.
 * <p>
 * It fully complies with the extension specification of the Registry SPI, but is different from the specific implementation of zookeeper and Nacos,
 * because it does not interact with any real third-party registry, but only with the relevant components of ServiceDiscovery in the process.
 * In short, it bridges the old interface model and the new service discovery model:
 * <p>
 * - register() aggregates interface level data into MetadataInfo by mainly interacting with MetadataService.
 * - subscribe() triggers the whole subscribe process of the application level service discovery model.
 * - Maps interface to applications depending on ServiceNameMapping.
 * - Starts the new service discovery listener (InstanceListener) and makes NotifierListeners part of the InstanceListener.
 */
public class ServiceDiscoveryRegistry extends FailbackRegistry {

    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private final ServiceDiscovery serviceDiscovery;

    private final AbstractServiceNameMapping serviceNameMapping;

    /* apps - listener */
    private final Map<String, ServiceInstancesChangedListener> serviceListeners = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<MappingListener>> mappingListeners = new ConcurrentHashMap<>();
    /* This lock has the same scope and lifecycle as its corresponding instance listener.
    It's used to make sure that only one interface mapping to the same app list can do subscribe or unsubscribe at the same moment.
    And the lock should be destroyed when listener destroying its corresponding instance listener.
    * */
    private final ConcurrentMap<String, Lock> appSubscriptionLocks = new ConcurrentHashMap<>();

    public ServiceDiscoveryRegistry(URL registryURL, ApplicationModel applicationModel) {
        super(registryURL);
        this.serviceDiscovery = createServiceDiscovery(registryURL);
        this.serviceNameMapping =
                (AbstractServiceNameMapping) ServiceNameMapping.getDefaultExtension(registryURL.getScopeModel());
        super.applicationModel = applicationModel;
    }

    // Currently, for test purpose
    protected ServiceDiscoveryRegistry(
            URL registryURL, ServiceDiscovery serviceDiscovery, ServiceNameMapping serviceNameMapping) {
        super(registryURL);
        this.serviceDiscovery = serviceDiscovery;
        this.serviceNameMapping = (AbstractServiceNameMapping) serviceNameMapping;
    }

    public ServiceDiscovery getServiceDiscovery() {
        return serviceDiscovery;
    }

    /**
     * Create the {@link ServiceDiscovery} from the registry {@link URL}
     *
     * @param registryURL the {@link URL} to connect the registry
     * @return non-null
     */
    protected ServiceDiscovery createServiceDiscovery(URL registryURL) {
        return getServiceDiscovery(registryURL
                .addParameter(INTERFACE_KEY, ServiceDiscovery.class.getName())
                .removeParameter(REGISTRY_TYPE_KEY));
    }

    /**
     * Get the instance {@link ServiceDiscovery} from the registry {@link URL} using
     * {@link ServiceDiscoveryFactory} SPI
     *
     * @param registryURL the {@link URL} to connect the registry
     * @return
     */
    private ServiceDiscovery getServiceDiscovery(URL registryURL) {
        ServiceDiscoveryFactory factory = getExtension(registryURL);
        return factory.getServiceDiscovery(registryURL);
    }

    @Override
    protected boolean shouldRegister(URL providerURL) {

        String side = providerURL.getSide();

        boolean should = PROVIDER_SIDE.equals(side); // Only register the Provider.

        if (!should && logger.isDebugEnabled()) {
            logger.debug(String.format("The URL[%s] should not be registered.", providerURL));
        }

        if (!acceptable(providerURL)) {
            logger.info("URL " + providerURL + " will not be registered to Registry. Registry " + this.getUrl()
                    + " does not accept service of this protocol type.");
            return false;
        }

        return should;
    }

    protected boolean shouldSubscribe(URL subscribedURL) {
        return !shouldRegister(subscribedURL);
    }

    @Override
    public final void register(URL url) {
        if (!shouldRegister(url)) { // Should Not Register
            return;
        }
        // 进行应用级别注册，这里将服务提供者数据转换到本地内存的元数据信息中
        doRegister(url);
    }

    @Override
    public void doRegister(URL url) {
        // fixme, add registry-cluster is not necessary anymore
        url = addRegistryClusterKey(url);
        serviceDiscovery.register(url);
    }

    @Override
    public final void unregister(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
        doUnregister(url);
    }

    @Override
    public void doUnregister(URL url) {
        // fixme, add registry-cluster is not necessary anymore
        url = addRegistryClusterKey(url);
        serviceDiscovery.unregister(url);
    }

    @Override
    public final void subscribe(URL url, NotifyListener listener) {
        if (!shouldSubscribe(url)) { // Should Not Subscribe
            return;
        }
        doSubscribe(url, listener);
    }

    /**
     * 执行应用级服务发现的订阅操作，通过接口名映射查找应用实例并订阅地址变更。
     * <p>
     * 该方法是Dubbo3应用级服务发现的核心订阅逻辑，与传统的接口级订阅不同，它通过以下步骤实现：
     * 先从元数据缓存中查询接口名到应用名的映射关系，然后订阅这些应用的所有实例地址。
     * 支持动态监听映射变更，当有新应用注册同一接口时自动触发重新订阅。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>添加集群标识</b>：调用addRegistryClusterKey将注册中心集群信息附加到URL，支持多元数据中心场景</li>
     *   <li><b>基础订阅</b>：调用serviceDiscovery.subscribe注册底层的实例变更监听器，建立与应用级注册中心的连接</li>
     *   <li><b>本地缓存查询</b>：通过ServiceNameMapping.getMappingByUrl从本地缓存查找接口名对应的应用名集合，避免每次都访问远程元数据中心</li>
     *   <li><b>加锁获取映射</b>：如果本地缓存未命中，通过mappingLock保证并发安全地从元数据中心拉取映射关系</li>
     *   <li><b>注册映射监听器</b>：创建DefaultMappingListener并调用getAndListen，当元数据中心检测到新应用注册时触发回调重新订阅</li>
     *   <li><b>初始化监听器状态</b>：调用updateInitialApps更新监听器的初始应用列表，确保后续事件推送基于准确的基准值</li>
     *   <li><b>空值处理</b>：如果映射结果为空，记录INFO日志并直接返回，等待映射监听器异步回调触发订阅（不阻塞启动）</li>
     *   <li><b>订阅地址</b>：调用subscribeURLs根据映射得到的应用名集合，订阅所有相关应用的实例地址列表</li>
     * </ol>
     * </p>
     *
     * @param url      订阅URL，包含服务接口名、版本、分组等信息，用于查询接口-应用映射关系
     * @param listener 通知监听器，当应用实例地址变更或接口映射关系变化时触发回调，重新生成Invoker链
     */
    @Override
    public void doSubscribe(URL url, NotifyListener listener) {
        url = addRegistryClusterKey(url);

        serviceDiscovery.subscribe(url, listener);

        /*
         * 查询接口-应用映射关系：
         * 从本地缓存中查找该接口已经被哪些应用注册过
         */
        Set<String> mappingByUrl = ServiceNameMapping.getMappingByUrl(url);

        String key = ServiceNameMapping.buildMappingKey(url);

        if (mappingByUrl == null) {
            /*
             * 加锁获取映射：
             * 本地缓存未命中时，从元数据中心拉取并注册监听器，保证并发安全
             */
            Lock mappingLock = serviceNameMapping.getMappingLock(key);
            try {
                mappingLock.lock();
                mappingByUrl = serviceNameMapping.getMapping(url);
                try {
                    /*
                     * 注册映射变更监听器：
                     * 当元数据中心检测到新应用注册同一接口时，触发回调重新订阅
                     */
                    DefaultMappingListener mappingListener = new DefaultMappingListener(url, mappingByUrl, listener);
                    //获取接口对应的应用名？
                    mappingByUrl = serviceNameMapping.getAndListen(this.getUrl(), url, mappingListener);
                    // update the initial mapping apps we started to listen, to make sure it reflects the real value
                    // used do subscription before any event.
                    // it's protected by the mapping lock, so it won't override the event value.
                    mappingListener.updateInitialApps(mappingByUrl);
                    synchronized (mappingListeners) {
                        ConcurrentHashMapUtils.computeIfAbsent(
                                        mappingListeners, url.getProtocolServiceKey(), (k) -> new ConcurrentHashSet<>())
                                .add(mappingListener);
                    }
                } catch (Exception e) {
                    logger.warn(
                            INTERNAL_ERROR,
                            "",
                            "",
                            "Cannot find app mapping for service " + url.getServiceInterface() + ", will not migrate.",
                            e);
                }

                if (CollectionUtils.isEmpty(mappingByUrl)) {
                    /*
                     * 空映射处理：
                     * 没有应用注册该接口时，停止订阅并等待映射监听器异步回调
                     */
                    logger.info(
                            "[METADATA_REGISTER] No interface-apps mapping found in local cache, stop subscribing, will automatically wait for mapping listener callback: "
                                    + url);
                    //                if (check) {
                    //                    throw new IllegalStateException("Should has at least one way to know which
                    // services this interface belongs to, subscription url: " + url);
                    //                }
                    return;
                }
            } finally {
                mappingLock.unlock();
            }
        }
        /*
         * 订阅应用实例地址：
         * 根据映射得到的应用名集合，订阅所有相关应用的提供者地址
         */
        subscribeURLs(url, listener, mappingByUrl);
    }

    @Override
    public final void unsubscribe(URL url, NotifyListener listener) {
        if (!shouldSubscribe(url)) { // Should Not Subscribe
            return;
        }
        url = addRegistryClusterKey(url);
        doUnsubscribe(url, listener);
    }

    private URL addRegistryClusterKey(URL url) {
        String registryCluster = serviceDiscovery.getUrl().getParameter(REGISTRY_CLUSTER_KEY);
        if (registryCluster != null && url.getParameter(REGISTRY_CLUSTER_KEY) == null) {
            url = url.addParameter(REGISTRY_CLUSTER_KEY, registryCluster);
        }
        return url;
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        // TODO: remove service name mapping listener
        serviceDiscovery.unsubscribe(url, listener);
        String protocolServiceKey = url.getProtocolServiceKey();
        Set<String> serviceNames = serviceNameMapping.getMapping(url);

        synchronized (mappingListeners) {
            Set<MappingListener> keyedListeners = mappingListeners.get(protocolServiceKey);
            if (keyedListeners != null) {
                List<MappingListener> matched = keyedListeners.stream()
                        .filter(mappingListener -> mappingListener instanceof DefaultMappingListener
                                && (Objects.equals(((DefaultMappingListener) mappingListener).getListener(), listener)))
                        .collect(Collectors.toList());
                for (MappingListener mappingListener : matched) {
                    serviceNameMapping.stopListen(url, mappingListener);
                    keyedListeners.remove(mappingListener);
                }
                if (keyedListeners.isEmpty()) {
                    mappingListeners.remove(protocolServiceKey, Collections.emptySet());
                }
            }
        }
        if (CollectionUtils.isNotEmpty(serviceNames)) {
            String serviceNamesKey = toStringKeys(serviceNames);
            Lock appSubscriptionLock = getAppSubscription(serviceNamesKey);
            try {
                appSubscriptionLock.lock();
                ServiceInstancesChangedListener instancesChangedListener = serviceListeners.get(serviceNamesKey);
                if (instancesChangedListener != null) {
                    instancesChangedListener.removeListener(url.getServiceKey(), listener);
                    if (!instancesChangedListener.hasListeners()) {
                        instancesChangedListener.destroy();
                        serviceListeners.remove(serviceNamesKey);
                        removeAppSubscriptionLock(serviceNamesKey);
                    }
                }
            } finally {
                appSubscriptionLock.unlock();
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public boolean isAvailable() {
        // serviceDiscovery isAvailable has a default method, which can be used as a reference when implementing
        return serviceDiscovery.isAvailable();
    }

    @Override
    public void destroy() {
        registryManager.removeDestroyedRegistry(this);
        // stop ServiceDiscovery
        execute(serviceDiscovery::destroy);
        // destroy all event listener
        for (ServiceInstancesChangedListener listener : serviceListeners.values()) {
            listener.destroy();
        }
        appSubscriptionLocks.clear();
        serviceListeners.clear();
        mappingListeners.clear();
    }

    @Override
    public boolean isServiceDiscovery() {
        return true;
    }

    /**
     * 订阅多个应用的实例地址变更，建立ServiceInstancesChangedListener与应用集合的映射关系。
     * <p>
     * 该方法是应用级服务发现的核心订阅逻辑，负责将接口级别的订阅请求转换为应用级别的实例监听。
     * 通过共享ServiceInstancesChangedListener实现多个接口复用同一个应用实例监听器，避免重复订阅导致的资源浪费。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>参数转换</b>：将serviceNames转换为TreeSet保证顺序一致性，生成唯一的serviceNamesKey用于标识这一组应用</li>
     *   <li><b>加锁保护</b>：通过getAppSubscription获取针对该应用集合的专属锁，防止并发创建多个监听器</li>
     *   <li><b>查找或创建监听器</b>：从serviceListeners缓存中查找是否已有相同应用集合的监听器，没有则调用createListener创建新的</li>
     *   <li><b>触发历史事件</b>：遍历所有应用，如果已有缓存的实例列表则立即触发onEvent回调，确保新监听器能获取到当前状态</li>
     *   <li><b>注册监听关系</b>：调用addListenerAndNotify将当前接口的NotifyListener注册到ServiceInstancesChangedListener，并立即通知现有实例列表</li>
     *   <li><b>全局注册</b>：通过addServiceInstancesChangedListener将监听器注册到ServiceDiscovery框架，接收来自注册中心的实例变更事件</li>
     *   <li><b>指标上报</b>：通过MetricsEventBus发布订阅成功事件，记录监控数据用于运维统计</li>
     *   <li><b>异常处理</b>：如果监听器已被其他线程销毁，移除缓存并记录日志，避免使用失效的监听器</li>
     * </ol>
     * </p>
     *
     * @param url           订阅URL，包含服务接口名、版本、分组等信息，用于标识哪个接口在订阅
     * @param listener      接口级别的通知监听器，当监听到应用实例变更时会被回调触发Invoker重建
     * @param serviceNames  需要订阅的应用名集合，这些应用都注册了当前接口，需要同时监听它们的实例变化
     */
    protected void subscribeURLs(URL url, NotifyListener listener, Set<String> serviceNames) {
        serviceNames = toTreeSet(serviceNames);
        String serviceNamesKey = toStringKeys(serviceNames);
        String serviceKey = url.getServiceKey();
        logger.info(
                String.format("Trying to subscribe from apps %s for service key %s, ", serviceNamesKey, serviceKey));

        /*
         * 加锁保护应用订阅：
         * 确保同一组应用只创建一个ServiceInstancesChangedListener，避免重复订阅
         */
        Lock appSubscriptionLock = getAppSubscription(serviceNamesKey);
        try {
            appSubscriptionLock.lock();
            ServiceInstancesChangedListener serviceInstancesChangedListener = serviceListeners.get(serviceNamesKey);
            if (serviceInstancesChangedListener == null) {
                /*
                 * 创建新的应用实例监听器：
                 * 监听这组应用的所有实例变更事件
                 */
                serviceInstancesChangedListener = serviceDiscovery.createListener(serviceNames);
                for (String serviceName : serviceNames) {
                    // 根据服务名称获取当前应用实例列表，从/services/<serviceName>/节点获取
                    List<ServiceInstance> serviceInstances = serviceDiscovery.getInstances(serviceName);
                    if (CollectionUtils.isNotEmpty(serviceInstances)) {
                        /*
                         * 触发历史事件：
                         * 将当前已知的实例列表推送给新创建的监听器，确保初始化时就有完整的地址信息
                         */
                        serviceInstancesChangedListener.onEvent(
                                new ServiceInstancesChangedEvent(serviceName, serviceInstances));
                    }
                }
                serviceListeners.put(serviceNamesKey, serviceInstancesChangedListener);
            }

            if (!serviceInstancesChangedListener.isDestroyed()) {
                /*
                 * 注册接口级监听器：
                 * 将当前接口的NotifyListener关联到应用级监听器，当应用实例变更时会同时通知所有订阅该应用的接口
                 */
                listener.addServiceListener(serviceInstancesChangedListener);
                serviceInstancesChangedListener.addListenerAndNotify(url, listener);
                ServiceInstancesChangedListener finalServiceInstancesChangedListener = serviceInstancesChangedListener;

                String serviceDiscoveryName =
                        url.getParameter(RegistryConstants.REGISTRY_CLUSTER_KEY, url.getProtocol());

                /*
                 * 全局注册并上报指标：
                 * 将监听器注册到ServiceDiscovery框架，开始接收注册中心的实时事件推送
                 */
                MetricsEventBus.post(
                        RegistryEvent.toSsEvent(
                                url.getApplicationModel(), serviceKey, Collections.singletonList(serviceDiscoveryName)),
                        () -> {
                            serviceDiscovery.addServiceInstancesChangedListener(finalServiceInstancesChangedListener);
                            return null;
                        });
            } else {
                /*
                 * 监听器已销毁处理：
                 * 清理缓存避免内存泄漏，后续订阅会重新创建新的监听器
                 */
                logger.info(String.format("Listener of %s has been destroyed by another thread.", serviceNamesKey));
                serviceListeners.remove(serviceNamesKey);
            }
        } finally {
            appSubscriptionLock.unlock();
        }
    }

    /**
     * Supports or not ?
     *
     * @param registryURL the {@link URL url} of registry
     * @return if supported, return <code>true</code>, or <code>false</code>
     */
    public static boolean supports(URL registryURL) {
        return SERVICE_REGISTRY_TYPE.equalsIgnoreCase(registryURL.getParameter(REGISTRY_TYPE_KEY));
    }

    public Map<String, ServiceInstancesChangedListener> getServiceListeners() {
        return serviceListeners;
    }

    private class DefaultMappingListener implements MappingListener {
        private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(DefaultMappingListener.class);
        private final URL url;
        private final NotifyListener listener;
        private volatile Set<String> oldApps;
        private volatile boolean stopped;

        public DefaultMappingListener(URL subscribedURL, Set<String> serviceNames, NotifyListener listener) {
            this.url = subscribedURL;
            this.oldApps = serviceNames;
            this.listener = listener;
        }

        @Override
        public synchronized void onEvent(MappingChangedEvent event) {
            logger.info("Received mapping notification from meta server, " + event);

            if (stopped) {
                logger.warn(
                        INTERNAL_ERROR,
                        "",
                        "",
                        "Listener has been stopped, ignore mapping notification, check why listener is not removed.");
                return;
            }
            Set<String> newApps = event.getApps();
            Set<String> tempOldApps = oldApps;

            Lock mappingLock = serviceNameMapping.getMappingLock(event.getServiceKey());
            try {
                mappingLock.lock();
                if (CollectionUtils.isEmpty(newApps) || CollectionUtils.equals(newApps, tempOldApps)) {
                    return;
                }
                logger.info("Mapping of service " + event.getServiceKey() + "changed from " + tempOldApps + " to "
                        + newApps);

                if (CollectionUtils.isEmpty(tempOldApps) && !newApps.isEmpty()) {
                    serviceNameMapping.putCachedMapping(ServiceNameMapping.buildMappingKey(url), newApps);
                    subscribeURLs(url, listener, newApps);
                    oldApps = newApps;
                    return;
                }

                for (String newAppName : newApps) {
                    if (!tempOldApps.contains(newAppName)) {
                        serviceNameMapping.removeCachedMapping(ServiceNameMapping.buildMappingKey(url));
                        serviceNameMapping.putCachedMapping(ServiceNameMapping.buildMappingKey(url), newApps);
                        // old instance listener related to old app list that needs to be destroyed after subscribe
                        // refresh.
                        ServiceInstancesChangedListener oldListener = listener.getServiceListener();
                        if (oldListener != null) {
                            String appKey = toStringKeys(toTreeSet(tempOldApps));
                            Lock appSubscriptionLock = getAppSubscription(appKey);
                            try {
                                appSubscriptionLock.lock();
                                oldListener.removeListener(url.getServiceKey(), listener);
                                if (!oldListener.hasListeners()) {
                                    oldListener.destroy();
                                    serviceListeners.remove(appKey);
                                    removeAppSubscriptionLock(appKey);
                                }
                            } finally {
                                appSubscriptionLock.unlock();
                            }
                        }

                        subscribeURLs(url, listener, newApps);
                        oldApps = newApps;
                        return;
                    }
                }
            } finally {
                mappingLock.unlock();
            }
        }

        protected NotifyListener getListener() {
            return listener;
        }

        // writing of oldApps is protected by mapping lock to guarantee sequence consistency.
        public void updateInitialApps(Set<String> oldApps) {
            if (oldApps != null && !CollectionUtils.equals(oldApps, this.oldApps)) {
                this.oldApps = oldApps;
                logger.info("Update initial mapping apps from " + this.oldApps + " to " + oldApps);
            }
        }

        @Override
        public void stop() {
            stopped = true;
        }
    }

    public Lock getAppSubscription(String key) {
        return ConcurrentHashMapUtils.computeIfAbsent(appSubscriptionLocks, key, _k -> new ReentrantLock());
    }

    public void removeAppSubscriptionLock(String key) {
        Lock lock = appSubscriptionLocks.get(key);
        if (lock != null) {
            try {
                lock.lock();
                appSubscriptionLocks.remove(key);
            } finally {
                lock.unlock();
            }
        }
    }
}
