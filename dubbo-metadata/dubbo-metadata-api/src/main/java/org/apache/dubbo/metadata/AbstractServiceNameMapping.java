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
package org.apache.dubbo.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_FAILED_LOAD_MAPPING_CACHE;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.utils.CollectionUtils.toTreeSet;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;

public abstract class AbstractServiceNameMapping implements ServiceNameMapping {
    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());
    protected ApplicationModel applicationModel;
    private final MappingCacheManager mappingCacheManager;
    private final ConcurrentHashMap<String, Set<MappingListener>> mappingListeners = new ConcurrentHashMap<>();
    // mapping lock is shared among registries of the same application.
    private final ConcurrentMap<String, ReentrantLock> mappingLocks = new ConcurrentHashMap<>();

    public AbstractServiceNameMapping(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        boolean enableFileCache = true;
        Optional<ApplicationConfig> application =
                applicationModel.getApplicationConfigManager().getApplication();
        if (application.isPresent()) {
            enableFileCache = Boolean.TRUE.equals(application.get().getEnableFileCache()) ? true : false;
        }
        this.mappingCacheManager = new MappingCacheManager(
                enableFileCache,
                applicationModel.tryGetApplicationName(),
                applicationModel
                        .getFrameworkModel()
                        .getBeanFactory()
                        .getBean(FrameworkExecutorRepository.class)
                        .getCacheRefreshingScheduledExecutor());
    }

    // just for test
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    /**
     * Get the service names from the specified Dubbo service interface, group, version and protocol
     *
     * @return
     */
    public abstract Set<String> get(URL url);

    /**
     * Get the service names from the specified Dubbo service interface, group, version and protocol
     *
     * @return
     */
    public abstract Set<String> getAndListen(URL url, MappingListener mappingListener);

    protected abstract void removeListener(URL url, MappingListener mappingListener);

    /**
     * 获取接口名到应用名的映射关系并注册监听器，支持缓存降级和异步刷新。
     * <p>
     * 该方法是Dubbo3应用级服务发现的核心查询接口，负责从本地缓存或远程元数据中心获取接口对应的应用名集合，
     * 并注册动态监听器以便在映射关系变更时收到通知。采用"缓存优先+异步刷新"策略，保证快速响应的同时保持数据新鲜度。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>构建缓存键</b>：调用ServiceNameMapping.buildMappingKey生成唯一标识（格式：interface:version@group）</li>
     *   <li><b>查询缓存</b>：从mappingCacheManager中查找之前缓存的映射结果，避免重复访问远程元数据中心</li>
     *   <li><b>缓存未命中处理</b>：
     *     <ul>
     *       <li>创建AsyncMappingTask并同步调用call()方法，立即从元数据中心拉取映射关系</li>
     *       <li>如果拉取失败或返回空，尝试从registryURL的SUBSCRIBED_SERVICE_NAMES_KEY参数中提取应用名（由注册中心在订阅时携带）</li>
     *       <li>如果最终获取到有效映射，更新本地缓存供后续查询使用</li>
     *     </ul>
     *   </li>
     *   <li><b>缓存命中处理</b>：
     *     <ul>
     *       <li>提交AsyncMappingTask到mappingRefreshingExecutor线程池异步执行，后台检查映射关系是否有变更</li>
     *       <li>立即返回缓存的旧值，不阻塞调用方，实现"先返回后刷新"的快速响应机制</li>
     *     </ul>
     *   </li>
     *   <li><b>返回结果</b>：返回应用名集合，可能为null或空集（表示暂无应用注册该接口）</li>
     * </ol>
     * </p>
     *
     * @param registryURL 注册中心URL，包含SUBSCRIBED_SERVICE_NAMES_KEY参数作为降级数据源，当元数据中心不可用时使用
     * @param subscribedURL 订阅的服务URL，包含接口名、版本、分组等信息，用于构建查询键和注册监听器
     * @param listener      映射变更监听器，当检测到新的应用注册同一接口时触发回调，重新订阅新应用的实例地址
     * @return 已注册当前接口的应用名集合，可能来自缓存、元数据中心或注册中心参数，为空时表示暂无映射关系
     */
    @Override
    public Set<String> getAndListen(URL registryURL, URL subscribedURL, MappingListener listener) {
        String key = ServiceNameMapping.buildMappingKey(subscribedURL);
        /*
         * 查询本地缓存：
         * 优先使用之前缓存的映射结果，避免频繁访问远程元数据中心
         */
        Set<String> mappingServices = mappingCacheManager.get(key);

        /*
         * 缓存未命中处理：
         * 同步从元数据中心拉取映射关系，确保首次订阅能获取到准确的应用列表
         */
        if (CollectionUtils.isEmpty(mappingServices)) {
            try {
                logger.info("[METADATA_REGISTER] Local cache mapping is empty");
                mappingServices = (new AsyncMappingTask(listener, subscribedURL, false)).call();
            } catch (Exception e) {
                // ignore
            }
            if (CollectionUtils.isEmpty(mappingServices)) {
                /*
                 * 降级策略：
                 * 从注册中心URL的subscribed-services参数中提取应用名，作为最后的备用数据源
                 */
                String registryServices = registryURL.getParameter(SUBSCRIBED_SERVICE_NAMES_KEY);
                if (StringUtils.isNotEmpty(registryServices)) {
                    logger.info(subscribedURL.getServiceInterface() + " mapping to " + registryServices
                            + " instructed by registry subscribed-services.");
                    mappingServices = parseServices(registryServices);
                }
            }
            if (CollectionUtils.isNotEmpty(mappingServices)) {
                this.putCachedMapping(ServiceNameMapping.buildMappingKey(subscribedURL), mappingServices);
            }
        } else {
            /*
             * 缓存命中处理：
             * 异步刷新映射关系，后台检测是否有新应用注册，不阻塞当前查询
             */
            ExecutorService executorService = applicationModel
                    .getFrameworkModel()
                    .getBeanFactory()
                    .getBean(FrameworkExecutorRepository.class)
                    .getMappingRefreshingExecutor();
            executorService.submit(new AsyncMappingTask(listener, subscribedURL, true));
        }

        return mappingServices;
    }

    @Override
    public MappingListener stopListen(URL subscribeURL, MappingListener listener) {
        synchronized (mappingListeners) {
            if (listener != null) {
                String mappingKey = ServiceNameMapping.buildMappingKey(subscribeURL);
                Set<MappingListener> listeners = mappingListeners.get(mappingKey);
                // todo, remove listener from remote metadata center
                if (CollectionUtils.isNotEmpty(listeners)) {
                    listeners.remove(listener);
                    listener.stop();
                    removeListener(subscribeURL, listener);
                }
                if (CollectionUtils.isEmpty(listeners)) {
                    mappingListeners.remove(mappingKey);
                    removeCachedMapping(mappingKey);
                    removeMappingLock(mappingKey);
                }
            }
            return listener;
        }
    }

    static Set<String> parseServices(String literalServices) {
        return isBlank(literalServices)
                ? emptySet()
                : unmodifiableSet(new TreeSet<>(of(literalServices.split(","))
                        .map(String::trim)
                        .filter(StringUtils::isNotEmpty)
                        .collect(toSet())));
    }

    @Override
    public void putCachedMapping(String serviceKey, Set<String> apps) {
        mappingCacheManager.put(serviceKey, toTreeSet(apps));
    }

    protected void putCachedMappingIfAbsent(String serviceKey, Set<String> apps) {
        Lock lock = getMappingLock(serviceKey);
        try {
            lock.lock();
            if (CollectionUtils.isEmpty(mappingCacheManager.get(serviceKey))) {
                mappingCacheManager.put(serviceKey, toTreeSet(apps));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<String> getMapping(URL consumerURL) {
        Set<String> mappingByUrl = ServiceNameMapping.getMappingByUrl(consumerURL);
        if (mappingByUrl != null) {
            return mappingByUrl;
        }
        return mappingCacheManager.get(ServiceNameMapping.buildMappingKey(consumerURL));
    }

    @Override
    public Set<String> getRemoteMapping(URL consumerURL) {
        return get(consumerURL);
    }

    @Override
    public Set<String> removeCachedMapping(String serviceKey) {
        return mappingCacheManager.remove(serviceKey);
    }

    public Lock getMappingLock(String key) {
        return ConcurrentHashMapUtils.computeIfAbsent(mappingLocks, key, _k -> new ReentrantLock());
    }

    protected void removeMappingLock(String key) {
        Lock lock = mappingLocks.get(key);
        if (lock != null) {
            try {
                lock.lock();
                mappingLocks.remove(key);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void $destroy() {
        mappingCacheManager.destroy();
        mappingListeners.clear();
        mappingLocks.clear();
    }

    private class AsyncMappingTask implements Callable<Set<String>> {

        private final MappingListener listener;
        private final URL subscribedURL;
        private final boolean notifyAtFirstTime;

        public AsyncMappingTask(MappingListener listener, URL subscribedURL, boolean notifyAtFirstTime) {
            this.listener = listener;
            this.subscribedURL = subscribedURL;
            this.notifyAtFirstTime = notifyAtFirstTime;
        }

        /**
         * 执行异步映射查询任务，从元数据中心获取接口到应用的映射关系并注册监听器。
         * <p>
         * 该方法是AsyncMappingTask的核心执行逻辑，作为Callable被线程池调度执行。
         * 根据构造时传入的notifyAtFirstTime标志决定是同步通知（首次查询）还是仅后台刷新（缓存命中场景）。
         * 通过mappingListeners锁保证并发安全，防止同一接口的多个监听器重复注册。
         * </p>
         * <p>
         * 主要处理流程：
         * <ol>
         *   <li><b>加锁保护</b>：通过synchronized(mappingListeners)确保对共享资源的独占访问，避免并发修改导致的数据不一致</li>
         *   <li><b>有监听器分支</b>（listener != null）：
         *     <ul>
         *       <li>调用getAndListen从元数据中心获取映射关系并注册监听器，支持动态感知新应用注册</li>
         *       <li>将监听器添加到mappingListeners缓存中，便于后续统一管理生命周期</li>
         *       <li>如果notifyAtFirstTime=true且获取到有效映射，立即触发listener.onEvent回调，保证至少一次通知语义</li>
         *       <li>通知过程中会自动更新本地缓存，确保后续查询能拿到最新数据</li>
         *     </ul>
         *   </li>
         *   <li><b>无监听器分支</b>（listener == null）：
         *     <ul>
         *       <li>调用get仅查询映射关系不注册监听器，适用于不需要动态感知的场景</li>
         *       <li>如果获取到有效映射，手动更新本地缓存供后续查询使用</li>
         *     </ul>
         *   </li>
         *   <li><b>异常容错</b>：捕获所有Exception但不抛出，记录ERROR日志后返回空集合，避免单个接口查询失败影响其他订阅流程</li>
         *   <li><b>返回结果</b>：返回查询到的应用名集合，可能为空集表示暂无映射关系或查询失败</li>
         * </ol>
         * </p>
         *
         * @return 已注册当前接口的应用名集合，按字典序排序的TreeSet，方便后续遍历和对比
         * @throws Exception 理论上不会抛出异常（内部已捕获），符合Callable接口的签名要求
         */
        @Override
        public Set<String> call() throws Exception {
            synchronized (mappingListeners) {
                Set<String> mappedServices = emptySet();
                try {
                    String mappingKey = ServiceNameMapping.buildMappingKey(subscribedURL);
                    if (listener != null) {
                        /*
                         * 带监听器的查询,获取应用名称
                         * 从元数据中心获取映射并注册监听器，支持动态感知新应用注册
                         */
                        mappedServices = toTreeSet(getAndListen(subscribedURL, listener));
                        Set<MappingListener> listeners = ConcurrentHashMapUtils.computeIfAbsent(mappingListeners,
                                mappingKey, _k -> new HashSet<>());
                        listeners.add(listener);
                        if (CollectionUtils.isNotEmpty(mappedServices)) {
                            if (notifyAtFirstTime) {
                                /*
                                 * 首次通知保证：
                                 * 无论底层元数据中心是否支持事件推送，都确保至少触发一次回调
                                 * 这样消费者能立即知道当前有哪些应用提供了该接口
                                 */
                                // guarantee at-least-once notification no matter what kind of underlying meta server is
                                // used.
                                // listener notification will also cause updating of mapping cache.
                                listener.onEvent(new MappingChangedEvent(mappingKey, mappedServices));
                            }
                        }
                    } else {
                        /*
                         * 纯查询模式：
                         * 仅获取映射关系不注册监听器，用于不需要动态感知的场景
                         */
                        mappedServices = get(subscribedURL);
                        if (CollectionUtils.isNotEmpty(mappedServices)) {
                            AbstractServiceNameMapping.this.putCachedMapping(mappingKey, mappedServices);
                        }
                    }
                } catch (Exception e) {
                    logger.error(COMMON_FAILED_LOAD_MAPPING_CACHE, "", "",
                            "Failed getting mapping info from remote center. ", e);
                }
                return mappedServices;
            }
        }
    }
}
