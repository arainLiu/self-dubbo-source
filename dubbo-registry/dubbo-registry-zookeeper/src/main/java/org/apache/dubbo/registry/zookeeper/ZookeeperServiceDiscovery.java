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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.function.ThrowableConsumer;
import org.apache.dubbo.common.function.ThrowableFunction;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.x.discovery.ServiceCache;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_ZOOKEEPER_EXCEPTION;
import static org.apache.dubbo.common.function.ThrowableFunction.execute;
import static org.apache.dubbo.metadata.RevisionResolver.EMPTY_REVISION;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.EXPORTED_SERVICES_REVISION_PROPERTY_NAME;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.zookeeper.util.CuratorFrameworkUtils.build;
import static org.apache.dubbo.registry.zookeeper.util.CuratorFrameworkUtils.buildCuratorFramework;
import static org.apache.dubbo.registry.zookeeper.util.CuratorFrameworkUtils.buildServiceDiscovery;
import static org.apache.dubbo.registry.zookeeper.util.CuratorFrameworkUtils.getRootPath;
import static org.apache.dubbo.rpc.RpcException.REGISTRY_EXCEPTION;

/**
 * Zookeeper {@link ServiceDiscovery} implementation based on
 * <a href="https://curator.apache.org/curator-x-discovery/index.html">Apache Curator X Discovery</a>
 * <p>
 * TODO, replace curator CuratorFramework with dubbo ZookeeperClient
 */
public class ZookeeperServiceDiscovery extends AbstractServiceDiscovery {

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    public static final String DEFAULT_GROUP = "/services";

    private final CuratorFramework curatorFramework;

    private final String rootPath;

    private final org.apache.curator.x.discovery.ServiceDiscovery<ZookeeperInstance> serviceDiscovery;

    /**
     * The Key is watched Zookeeper path, the value is an instance of {@link CuratorWatcher}
     */
    private final ConcurrentHashMap<String, ZookeeperServiceDiscoveryChangeWatcher> watcherCaches =
            new ConcurrentHashMap<>();

    public ZookeeperServiceDiscovery(ApplicationModel applicationModel, URL registryURL) {
        super(applicationModel, registryURL);
        try {
            this.curatorFramework = buildCuratorFramework(registryURL, this);
            this.rootPath = getRootPath(registryURL);
            this.serviceDiscovery = buildServiceDiscovery(curatorFramework, rootPath);
            this.serviceDiscovery.start();
        } catch (Exception e) {
            throw new IllegalStateException("Create zookeeper service discovery failed.", e);
        }
    }

    @Override
    public void doDestroy() throws Exception {
        serviceDiscovery.close();
        curatorFramework.close();
        watcherCaches.clear();
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) {
        try {
            //注册应用信息
            serviceDiscovery.registerService(build(serviceInstance));
        } catch (Exception e) {
            throw new RpcException(REGISTRY_EXCEPTION, "Failed register instance " + serviceInstance.toString(), e);
        }
    }

    @Override
    public void doUnregister(ServiceInstance serviceInstance) throws RuntimeException {
        if (serviceInstance != null) {
            doInServiceRegistry(serviceDiscovery -> serviceDiscovery.unregisterService(build(serviceInstance)));
        }
    }

    @Override
    protected void doUpdate(ServiceInstance oldServiceInstance, ServiceInstance newServiceInstance)
            throws RuntimeException {
        if (EMPTY_REVISION.equals(getExportedServicesRevision(newServiceInstance))
                || EMPTY_REVISION.equals(
                        oldServiceInstance.getMetadata().get(EXPORTED_SERVICES_REVISION_PROPERTY_NAME))) {
            super.doUpdate(oldServiceInstance, newServiceInstance);
            return;
        }

        org.apache.curator.x.discovery.ServiceInstance<ZookeeperInstance> oldInstance = build(oldServiceInstance);
        org.apache.curator.x.discovery.ServiceInstance<ZookeeperInstance> newInstance = build(newServiceInstance);
        if (!Objects.equals(newInstance.getName(), oldInstance.getName())
                || !Objects.equals(newInstance.getId(), oldInstance.getId())) {
            // Ignore if id changed. Should unregister first.
            super.doUpdate(oldServiceInstance, newServiceInstance);
            return;
        }

        try {
            this.serviceInstance = newServiceInstance;
            reportMetadata(newServiceInstance.getServiceMetadata());
            serviceDiscovery.updateService(newInstance);
        } catch (Exception e) {
            throw new RpcException(REGISTRY_EXCEPTION, "Failed register instance " + newServiceInstance.toString(), e);
        }
    }

    @Override
    public Set<String> getServices() {
        return doInServiceDiscovery(s -> new LinkedHashSet<>(s.queryForNames()));
    }

    /**
     * 查询指定应用的所有服务实例列表，从Zookeeper读取注册的应用实例信息。
     * <p>
     * 该方法是Dubbo应用级服务发现的核心查询接口，负责从Zookeeper的服务发现路径中获取指定应用名下的所有实例地址。
     * 通过Curator Framework的ServiceDiscovery API封装，将Zookeeper原生数据转换为Dubbo的ServiceInstance对象。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>委托执行</b>：调用doInServiceDiscovery在ServiceDiscovery上下文中执行查询操作，自动处理异常和资源管理</li>
     *   <li><b>查询实例</b>：调用s.queryForInstances(serviceName)从Zookeeper读取指定应用名的所有实例节点，返回Curator的ServiceInstance列表</li>
     *   <li><b>数据转换</b>：调用build方法将Curator ServiceInstance<ZookeeperInstance>转换为Dubbo ServiceInstance对象，提取主机、端口、元数据等信息</li>
     *   <li><b>返回列表</b>：返回转换后的ServiceInstance列表，如果应用未注册或无实例则返回空列表（非null）</li>
     * </ol>
     * </p>
     *
     * @param serviceName 应用名称，即服务提供者的应用标识，用于在Zookeeper中定位对应的实例节点路径
     * @return 服务实例列表，包含该应用所有已注册的实例地址和元数据信息，如果应用不存在则返回空列表
     * @throws NullPointerException 当serviceName为null时抛出空指针异常
     */
    @Override
    public List<ServiceInstance> getInstances(String serviceName) throws NullPointerException {
        return doInServiceDiscovery(s -> build(registryURL, s.queryForInstances(serviceName)));
    }

    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener)
            throws NullPointerException, IllegalArgumentException {
        // check if listener has already been added through another interface/service
        if (!instanceListeners.add(listener)) {
            return;
        }
        listener.getServiceNames().forEach(serviceName -> registerServiceWatcher(serviceName, listener));
    }

    @Override
    public void removeServiceInstancesChangedListener(ServiceInstancesChangedListener listener)
            throws IllegalArgumentException {
        if (!instanceListeners.remove(listener)) {
            return;
        }
        listener.getServiceNames().forEach(serviceName -> {
            ZookeeperServiceDiscoveryChangeWatcher watcher = watcherCaches.get(serviceName);
            if (watcher != null) {
                watcher.getListeners().remove(listener);
                if (watcher.getListeners().isEmpty()) {
                    watcherCaches.remove(serviceName);
                    try {
                        watcher.getCacheInstance().close();
                    } catch (IOException e) {
                        logger.error(
                                REGISTRY_ZOOKEEPER_EXCEPTION,
                                "curator stop watch failed",
                                "",
                                "Curator Stop service discovery watch failed. Service Name: " + serviceName);
                    }
                }
            }
        });
    }

    @Override
    public boolean isAvailable() {
        // Fix the issue of timeout for all calls to the isAvailable method after the zookeeper is disconnected
        return !isDestroy() && isConnected() && CollectionUtils.isNotEmpty(getServices());
    }

    private boolean isConnected() {
        if (curatorFramework == null || curatorFramework.getZookeeperClient() == null) {
            return false;
        }
        return curatorFramework.getZookeeperClient().isConnected();
    }

    private void doInServiceRegistry(ThrowableConsumer<org.apache.curator.x.discovery.ServiceDiscovery> consumer) {
        ThrowableConsumer.execute(serviceDiscovery, s -> consumer.accept(s));
    }

    private <R> R doInServiceDiscovery(ThrowableFunction<org.apache.curator.x.discovery.ServiceDiscovery, R> function) {
        return execute(serviceDiscovery, function);
    }

    protected void registerServiceWatcher(String serviceName, ServiceInstancesChangedListener listener) {
        CountDownLatch latch = new CountDownLatch(1);

        ZookeeperServiceDiscoveryChangeWatcher watcher =
                ConcurrentHashMapUtils.computeIfAbsent(watcherCaches, serviceName, name -> {
                    ServiceCache<ZookeeperInstance> serviceCache =
                            serviceDiscovery.serviceCacheBuilder().name(name).build();
                    ZookeeperServiceDiscoveryChangeWatcher newer =
                            new ZookeeperServiceDiscoveryChangeWatcher(this, serviceCache, name, latch);
                    serviceCache.addListener(newer);

                    try {
                        serviceCache.start();
                    } catch (Exception e) {
                        throw new RpcException(REGISTRY_EXCEPTION, "Failed subscribe service: " + name, e);
                    }

                    return newer;
                });

        watcher.addListener(listener);
        listener.onEvent(new ServiceInstancesChangedEvent(serviceName, this.getInstances(serviceName)));
        latch.countDown();
    }
}
