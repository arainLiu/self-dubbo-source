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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_CREATE_INSTANCE;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * AbstractRegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.RegistryFactory
 */
public abstract class AbstractRegistryFactory implements RegistryFactory, ScopeModelAware {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(AbstractRegistryFactory.class);

    private RegistryManager registryManager;
    protected ApplicationModel applicationModel;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        this.registryManager = applicationModel.getBeanFactory().getBean(RegistryManager.class);
    }

    /**
     * 根据URL获取或创建注册中心实例，保证同一配置的注册中心全局唯一。
     * <p>
     * 该方法采用双重检查锁定（DCL）模式，确保在并发环境下只创建一个注册中心实例。
     * 主要执行以下操作：
     * <ol>
     *   <li>检查RegistryManager是否已初始化，如果未初始化则抛出异常</li>
     *   <li>检查应用是否已销毁，如果已销毁则返回一个空的NopRegistry实例，避免在关闭阶段创建新连接</li>
     *   <li>标准化URL：设置路径为RegistryService接口名，添加interface参数，移除timestamp、export、refer等运行时参数，
     *       确保相同配置的URL能生成相同的缓存键</li>
     *   <li>调用createRegistryCacheKey()生成唯一的缓存键</li>
     *   <li>获取注册锁，进入同步块进行二次检查（防止并发创建）</li>
     *   <li>从RegistryManager中查找已缓存的注册中心实例，如果存在则直接返回</li>
     *   <li>调用createRegistry()模板方法创建具体的注册中心实现（由子类实现，如ZookeeperRegistry、NacosRegistry等）</li>
     *   <li>如果启用了check检查且创建失败，则抛出异常；否则记录警告日志并返回null</li>
     *   <li>将创建的注册中心实例存入RegistryManager的缓存中</li>
     * </ol>
     * </p>
     * <p>
     * 线程安全：通过registryManager.getRegistryLock()独占锁保证同一时间只有一个线程能创建注册中心实例，
     * 避免资源浪费和状态不一致问题。
     * </p>
     *
     * @param url 注册中心的URL地址，包含协议类型、主机地址、端口号、参数等信息
     * @return 注册中心实例，如果创建失败且check=false则返回null
     * @throws IllegalStateException 当RegistryManager未初始化或check=true且创建失败时抛出
     * @throws RuntimeException 当check=true且获取或创建注册中心过程中发生异常时抛出
     */
    @Override
    public Registry getRegistry(URL url) {
        if (registryManager == null) {
            throw new IllegalStateException("Unable to fetch RegistryManager from ApplicationModel BeanFactory. "
                    + "Please check if [setApplicationModel](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/filter/GenericFilter.java#L81-L84) has been override.");
        }

        /*
         * 如果应用已销毁，返回空实现的注册中心，避免在关闭阶段创建新连接
         */
        Registry defaultNopRegistry = registryManager.getDefaultNopRegistryIfDestroyed();
        if (null != defaultNopRegistry) {
            return defaultNopRegistry;
        }

        /*
         * 标准化URL：统一路径和参数，移除运行时生成的临时参数，确保相同配置生成相同的缓存键
         */
        url = URLBuilder.from(url)
                .setPath(RegistryService.class.getName())
                .addParameter(INTERFACE_KEY, RegistryService.class.getName())
                .removeParameter(TIMESTAMP_KEY)
                .removeAttribute(EXPORT_KEY)
                .removeAttribute(REFER_KEY)
                .build();

        String key = createRegistryCacheKey(url);
        Registry registry = null;
        boolean check = UrlUtils.isCheck(url);

        // Lock the registry access process to ensure a single instance of the registry
        /*
         * 获取注册中心创建锁，保证同一配置的注册中心只被创建一次
         */
        registryManager.getRegistryLock().lock();
        try {
            // double check
            // fix https://github.com/apache/dubbo/issues/7265.
            /*
             * 二次检查：在锁内再次确认应用未被销毁
             */
            defaultNopRegistry = registryManager.getDefaultNopRegistryIfDestroyed();
            if (null != defaultNopRegistry) {
                return defaultNopRegistry;
            }

            /*
             * 从缓存中查找已存在的注册中心实例
             */
            registry = registryManager.getRegistry(key);
            if (registry != null) {
                return registry;
            }

            // create registry by spi/ioc
            /*
             * 调用子类实现的模板方法创建具体的注册中心实例
             */
            registry = createRegistry(url);
            if (check && registry == null) {
                throw new IllegalStateException("Can not create registry " + url);
            }

            if (registry != null) {
                registryManager.putRegistry(key, registry);
            }
        } catch (Exception e) {
            if (check) {
                throw new RuntimeException("Can not create registry " + url, e);
            } else {
                // 1-11 Failed to obtain or create registry (service) object.
                LOGGER.warn(REGISTRY_FAILED_CREATE_INSTANCE, "", "", "Failed to obtain or create registry ", e);
            }
        } finally {
            // Release the lock
            registryManager.getRegistryLock().unlock();
        }

        return registry;
    }

    /**
     * Create the key for the registries cache.
     * This method may be overridden by the sub-class.
     *
     * @param url the registration {@link URL url}
     * @return non-null
     */
    protected String createRegistryCacheKey(URL url) {
        return url.toServiceStringWithoutResolving();
    }

    protected abstract Registry createRegistry(URL url);
}
