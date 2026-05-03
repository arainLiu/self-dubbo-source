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
package org.apache.dubbo.config.utils;

import org.apache.dubbo.common.BaseServiceMetadata;
import org.apache.dubbo.common.config.ReferenceCache;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.rpc.service.Destroyable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_API_WRONG_USE;

/**
 * A simple util class for cache {@link ReferenceConfigBase}.
 * <p>
 * {@link ReferenceConfigBase} is a heavy Object, it's necessary to cache these object
 * for the framework which create {@link ReferenceConfigBase} frequently.
 * <p>
 * You can implement and use your own {@link ReferenceConfigBase} cache if you need use complicate strategy.
 */
public class SimpleReferenceCache implements ReferenceCache {
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(SimpleReferenceCache.class);
    public static final String DEFAULT_NAME = "_DEFAULT_";
    /**
     * Create the key with the <b>Group</b>, <b>Interface</b> and <b>version</b> attribute of {@link ReferenceConfigBase}.
     * <p>
     * key example: <code>group1/org.apache.dubbo.foo.FooService:1.0.0</code>.
     */
    public static final KeyGenerator DEFAULT_KEY_GENERATOR = referenceConfig -> {
        String iName = referenceConfig.getInterface();
        if (StringUtils.isBlank(iName)) {
            Class<?> clazz = referenceConfig.getInterfaceClass();
            iName = clazz.getName();
        }
        if (StringUtils.isBlank(iName)) {
            throw new IllegalArgumentException("No interface info in ReferenceConfig" + referenceConfig);
        }

        return BaseServiceMetadata.buildServiceKey(iName, referenceConfig.getGroup(), referenceConfig.getVersion());
    };

    private static final AtomicInteger nameIndex = new AtomicInteger();

    static final ConcurrentMap<String, SimpleReferenceCache> CACHE_HOLDER = new ConcurrentHashMap<>();
    private final String name;
    private final KeyGenerator generator;

    private final ConcurrentMap<String, List<ReferenceConfigBase<?>>> referenceKeyMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, List<ReferenceConfigBase<?>>> referenceTypeMap = new ConcurrentHashMap<>();
    private final Map<ReferenceConfigBase<?>, Object> references = new ConcurrentHashMap<>();

    protected SimpleReferenceCache(String name, KeyGenerator generator) {
        this.name = name;
        this.generator = generator;
    }

    /**
     * Get the cache use default name and {@link #DEFAULT_KEY_GENERATOR} to generate cache key.
     * Create cache if not existed yet.
     */
    public static SimpleReferenceCache getCache() {
        return getCache(DEFAULT_NAME);
    }

    public static SimpleReferenceCache newCache() {
        return getCache(DEFAULT_NAME + "#" + nameIndex.incrementAndGet());
    }

    /**
     * Get the cache use specified name and {@link KeyGenerator}.
     * Create cache if not existed yet.
     */
    public static SimpleReferenceCache getCache(String name) {
        return getCache(name, DEFAULT_KEY_GENERATOR);
    }

    /**
     * Get the cache use specified {@link KeyGenerator}.
     * Create cache if not existed yet.
     */
    public static SimpleReferenceCache getCache(String name, KeyGenerator keyGenerator) {
        return ConcurrentHashMapUtils.computeIfAbsent(
                CACHE_HOLDER, name, k -> new SimpleReferenceCache(k, keyGenerator));
    }

    /**
     * 获取或创建远程服务的本地代理对象，通过缓存机制避免重复创建ReferenceConfig。
     * <p>
     * 该方法是Dubbo服务消费者引用缓存的核心入口，负责根据ReferenceConfig生成唯一的缓存键，
     * 检查是否已存在相同服务的代理对象，如果存在则直接返回，否则创建新的Invoker代理并缓存。
     * ReferenceConfig是重量级对象（包含网络连接、线程池等资源），通过缓存可以显著降低资源消耗。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>生成缓存键</b>：通过KeyGenerator从ReferenceConfig提取接口名、分组、版本号组成唯一键（格式：group/interface:version）</li>
     *   <li><b>单例检查</b>：判断ReferenceConfig是否为单例模式（默认true），非单例模式下使用缓存可能导致内存泄漏</li>
     *   <li><b>缓存查找</b>：如果是单例模式，调用get(key, type)从referenceKeyMap中查找已存在的代理对象</li>
     *   <li><b>索引维护</b>：将ReferenceConfig同时加入referenceTypeMap（按接口类型索引）和referenceKeyMap（按服务键索引）</li>
     *   <li><b>创建代理</b>：调用rc.get(check)创建新的Invoker代理对象，建立与提供者的网络连接</li>
     *   <li><b>返回代理</b>：返回创建的代理对象，调用方可以像调用本地方法一样调用远程服务</li>
     * </ol>
     * </p>
     *
     * @param rc    引用配置对象，包含服务接口、注册地址、超时时间等所有RPC调用所需的配置信息
     * @param check 是否进行严格检查，true时在连接失败或提供者不可用时抛出异常，false时仅记录警告
     * @return 远程服务的本地代理对象，类型为服务接口
     * @throws IllegalStateException 当配置错误或注册中心连接失败时可能抛出（取决于check参数）
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(ReferenceConfigBase<T> rc, boolean check) {
        String key = generator.generateKey(rc);
        Class<?> type = rc.getInterfaceClass();

        boolean singleton = rc.getSingleton() == null || rc.getSingleton();
        T proxy = null;
        /*
         * 单例模式缓存查找：
         * 对于单例ReferenceConfig，先尝试从缓存中获取已存在的代理对象，避免重复创建
         */
        // Check existing proxy of the same 'key' and 'type' first.
        if (singleton) {
            proxy = get(key, (Class<T>) type);
        } else {
            /*
             * 非单例模式警告：
             * 非单例ReferenceConfig与缓存机制同时使用会导致内存泄漏，建议直接调用ReferenceConfig#get()
             */
            logger.warn(
                    CONFIG_API_WRONG_USE,
                    "",
                    "",
                    "Using non-singleton ReferenceConfig and ReferenceCache at the same time may cause memory leak. "
                            + "Call ReferenceConfig#get() directly for non-singleton ReferenceConfig instead of using ReferenceCache#get(ReferenceConfig)");
        }

        /*
         * 创建新代理：
         * 当缓存中不存在时，初始化ReferenceConfig并创建Invoker代理对象
         */
        if (proxy == null) {
            /*
             * 维护类型索引：
             * 将ReferenceConfig按接口类型分组存储，便于后续通过getAll(type)批量查询
             */
            List<ReferenceConfigBase<?>> referencesOfType = ConcurrentHashMapUtils.computeIfAbsent(
                    referenceTypeMap, type, _t -> Collections.synchronizedList(new ArrayList<>()));
            referencesOfType.add(rc);
            /*
             * 维护键索引：
             * 将ReferenceConfig按服务键分组存储，便于后续通过get(key)快速查找
             */
            List<ReferenceConfigBase<?>> referenceConfigList = ConcurrentHashMapUtils.computeIfAbsent(
                    referenceKeyMap, key, _k -> Collections.synchronizedList(new ArrayList<>()));
            referenceConfigList.add(rc);
            /*
             * 执行引用创建：
             * 调用ReferenceConfig.get创建Invoker代理，建立与提供者的网络连接
             */
            proxy = rc.get(check);
        }

        return proxy;
    }

    /**
     * Fetch cache with the specified key. The key is decided by KeyGenerator passed-in. If the default KeyGenerator is
     * used, then the key is in the format of <code>group/interfaceClass:version</code>
     *
     * @param key  cache key
     * @param type object class
     * @param <T>  object type
     * @return object from the cached ReferenceConfigBase
     * @see KeyGenerator#generateKey(ReferenceConfigBase)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        List<ReferenceConfigBase<?>> referenceConfigs = referenceKeyMap.get(key);
        if (CollectionUtils.isNotEmpty(referenceConfigs)) {
            return (T) referenceConfigs.get(0).get();
        }
        return null;
    }

    /**
     * Check and return existing ReferenceConfig and its corresponding proxy instance.
     *
     * @param key ServiceKey
     * @param <T> service interface type
     * @return the existing proxy instance of the same service key
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        List<ReferenceConfigBase<?>> referenceConfigBases = referenceKeyMap.get(key);
        if (CollectionUtils.isNotEmpty(referenceConfigBases)) {
            return (T) referenceConfigBases.get(0).get();
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getAll(Class<T> type) {
        List<ReferenceConfigBase<?>> referenceConfigBases = referenceTypeMap.get(type);
        if (CollectionUtils.isEmpty(referenceConfigBases)) {
            return Collections.EMPTY_LIST;
        }
        List proxiesOfType = new ArrayList(referenceConfigBases.size());
        for (ReferenceConfigBase<?> rc : referenceConfigBases) {
            proxiesOfType.add(rc.get());
        }
        return Collections.unmodifiableList(proxiesOfType);
    }

    /**
     * Check and return existing ReferenceConfig and its corresponding proxy instance.
     *
     * @param type service interface class
     * @param <T>  service interface type
     * @return the existing proxy instance of the same interface definition
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> type) {
        List<ReferenceConfigBase<?>> referenceConfigBases = referenceTypeMap.get(type);
        if (CollectionUtils.isNotEmpty(referenceConfigBases)) {
            return (T) referenceConfigBases.get(0).get();
        }
        return null;
    }

    @Override
    public void check(String key, Class<?> type, long timeout) {
        List<ReferenceConfigBase<?>> referencesOfKey = referenceKeyMap.get(key);
        if (CollectionUtils.isEmpty(referencesOfKey)) {
            return;
        }
        List<ReferenceConfigBase<?>> referencesOfType = referenceTypeMap.get(type);
        if (CollectionUtils.isEmpty(referencesOfType)) {
            return;
        }
        for (ReferenceConfigBase<?> rc : referencesOfKey) {
            rc.checkOrDestroy(timeout);
        }
    }

    @Override
    public <T> void check(ReferenceConfigBase<T> referenceConfig, long timeout) {
        String key = generator.generateKey(referenceConfig);
        Class<?> type = referenceConfig.getInterfaceClass();
        check(key, type, timeout);
    }

    @Override
    public void destroy(String key, Class<?> type) {
        List<ReferenceConfigBase<?>> referencesOfKey = referenceKeyMap.remove(key);
        if (CollectionUtils.isEmpty(referencesOfKey)) {
            return;
        }
        List<ReferenceConfigBase<?>> referencesOfType = referenceTypeMap.get(type);
        if (CollectionUtils.isEmpty(referencesOfType)) {
            return;
        }
        for (ReferenceConfigBase<?> rc : referencesOfKey) {
            referencesOfType.remove(rc);
            destroyReference(rc);
        }
    }

    @Override
    public void destroy(Class<?> type) {
        List<ReferenceConfigBase<?>> referencesOfType = referenceTypeMap.remove(type);
        for (ReferenceConfigBase<?> rc : referencesOfType) {
            String key = generator.generateKey(rc);
            referenceKeyMap.remove(key);
            destroyReference(rc);
        }
    }

    /**
     * clear and destroy one {@link ReferenceConfigBase} in the cache.
     *
     * @param referenceConfig use for create key.
     */
    @Override
    public <T> void destroy(ReferenceConfigBase<T> referenceConfig) {
        String key = generator.generateKey(referenceConfig);
        Class<?> type = referenceConfig.getInterfaceClass();
        destroy(key, type);
    }

    /**
     * clear and destroy all {@link ReferenceConfigBase} in the cache.
     */
    @Override
    public void destroyAll() {
        if (CollectionUtils.isEmptyMap(referenceKeyMap)) {
            return;
        }

        referenceKeyMap.forEach((_k, referencesOfKey) -> {
            for (ReferenceConfigBase<?> rc : referencesOfKey) {
                destroyReference(rc);
            }
        });

        referenceKeyMap.clear();
        referenceTypeMap.clear();
    }

    private void destroyReference(ReferenceConfigBase<?> rc) {
        Destroyable proxy = (Destroyable) rc.get();
        if (proxy != null) {
            proxy.$destroy();
        }
        rc.destroy();
    }

    public Map<String, List<ReferenceConfigBase<?>>> getReferenceMap() {
        return referenceKeyMap;
    }

    public Map<Class<?>, List<ReferenceConfigBase<?>>> getReferenceTypeMap() {
        return referenceTypeMap;
    }

    @Override
    public String toString() {
        return "ReferenceCache(name: " + name + ")";
    }

    public interface KeyGenerator {
        String generateKey(ReferenceConfigBase<?> referenceConfig);
    }
}
