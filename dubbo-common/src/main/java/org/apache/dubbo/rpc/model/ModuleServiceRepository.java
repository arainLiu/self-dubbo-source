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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.ServiceConfigBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Service repository for module
 */
public class ModuleServiceRepository {

    private final ModuleModel moduleModel;

    /**
     * services
     */
    private final ConcurrentMap<String, List<ServiceDescriptor>> services = new ConcurrentHashMap<>();

    /**
     * consumers ( key - group/interface:version value - consumerModel list)
     */
    private final ConcurrentMap<String, List<ConsumerModel>> consumers = new ConcurrentHashMap<>();

    /**
     * providers
     */
    private final ConcurrentMap<String, ProviderModel> providers = new ConcurrentHashMap<>();

    private final FrameworkServiceRepository frameworkServiceRepository;

    public ModuleServiceRepository(ModuleModel moduleModel) {
        this.moduleModel = moduleModel;
        frameworkServiceRepository =
                ScopeModelUtil.getFrameworkModel(moduleModel).getServiceRepository();
    }

    public ModuleModel getModuleModel() {
        return moduleModel;
    }

    /**
     * @deprecated Replaced to {@link ModuleServiceRepository#registerConsumer(ConsumerModel)}
     */
    @Deprecated
    public void registerConsumer(
            String serviceKey,
            ServiceDescriptor serviceDescriptor,
            ReferenceConfigBase<?> rc,
            Object proxy,
            ServiceMetadata serviceMetadata) {
        ClassLoader classLoader = null;
        if (rc != null) {
            classLoader = rc.getInterfaceClassLoader();
        }
        ConsumerModel consumerModel = new ConsumerModel(
                serviceMetadata.getServiceKey(), proxy, serviceDescriptor, serviceMetadata, null, classLoader);
        this.registerConsumer(consumerModel);
    }

    public void registerConsumer(ConsumerModel consumerModel) {
        ConcurrentHashMapUtils.computeIfAbsent(
                        consumers, consumerModel.getServiceKey(), (serviceKey) -> new CopyOnWriteArrayList<>())
                .add(consumerModel);
    }

    /**
     * @deprecated Replaced to {@link ModuleServiceRepository#registerProvider(ProviderModel)}
     */
    @Deprecated
    public void registerProvider(
            String serviceKey,
            Object serviceInstance,
            ServiceDescriptor serviceModel,
            ServiceConfigBase<?> serviceConfig,
            ServiceMetadata serviceMetadata) {
        ClassLoader classLoader = null;
        Class<?> cla = null;
        if (serviceConfig != null) {
            classLoader = serviceConfig.getInterfaceClassLoader();
            cla = serviceConfig.getInterfaceClass();
        }
        ProviderModel providerModel =
                new ProviderModel(serviceKey, serviceInstance, serviceModel, serviceMetadata, classLoader);
        this.registerProvider(providerModel);
    }

    public void registerProvider(ProviderModel providerModel) {
        providers.putIfAbsent(providerModel.getServiceKey(), providerModel);
        frameworkServiceRepository.registerProvider(providerModel);
    }

    public ServiceDescriptor registerService(ServiceDescriptor serviceDescriptor) {
        return registerService(serviceDescriptor.getServiceInterfaceClass(), serviceDescriptor);
    }

    public ServiceDescriptor registerService(Class<?> interfaceClazz) {
        ServiceDescriptor serviceDescriptor = new ReflectionServiceDescriptor(interfaceClazz);
        return registerService(interfaceClazz, serviceDescriptor);
    }

    /**
     * 注册服务描述符到模块服务仓库
     * <p>
     * 该方法负责将ServiceDescriptor注册到模块的服务仓库中，采用线程安全的方式管理：
     * 1. 使用接口类名作为key，在services Map中获取或创建ServiceDescriptor列表
     * 2. 使用CopyOnWriteArrayList保证并发读写的线程安全性
     * 3. 通过同步锁检查是否已存在相同接口的ServiceDescriptor
     * 4. 如果已存在则直接返回已有的描述符，避免重复注册
     * 5. 如果不存在则将新的描述符添加到列表中
     * <p>
     * 该设计确保了同一接口在模块中只会有一个ServiceDescriptor实例，
     * 支持多线程环境下的安全注册操作。
     *
     * @param interfaceClazz 服务接口的Class对象
     * @param serviceDescriptor 要注册的服务描述符对象
     * @return 已注册的ServiceDescriptor，如果已存在则返回已有的描述符
     */
    public ServiceDescriptor registerService(Class<?> interfaceClazz, ServiceDescriptor serviceDescriptor) {
        // 根据接口类名获取或创建ServiceDescriptor列表
        List<ServiceDescriptor> serviceDescriptors = ConcurrentHashMapUtils.computeIfAbsent(
                services, interfaceClazz.getName(), k -> new CopyOnWriteArrayList<>());

        // 同步检查并添加ServiceDescriptor，避免重复注册
        synchronized (serviceDescriptors) {
            Optional<ServiceDescriptor> previous = serviceDescriptors.stream()
                    .filter(s -> s.getServiceInterfaceClass().equals(interfaceClazz))
                    .findFirst();

            if (previous.isPresent()) {
                // 如果已存在相同接口的描述符，直接返回
                return previous.get();
            } else {
                // 否则添加到列表中并返回
                serviceDescriptors.add(serviceDescriptor);
                return serviceDescriptor;
            }
        }
    }

    /**
     * 注册服务并建立路径映射关系
     * <p>
     * 该方法在{@link #registerService(Class)}的基础上，额外处理了服务路径与接口名称不一致的情况。
     * <p>
     * 设计假设：
     * 1. 具有不同接口的服务不允许使用相同的路径
     * 2. 共享同一接口但具有不同group/version的服务可以共享同一路径
     * 3. 路径的默认值是接口的全限定名
     * <p>
     * 当用户自定义path与接口名称不同时，该方法会创建额外的路径映射，
     * 使得服务可以通过自定义path被访问。
     *
     * @param path 服务的路径，可以是自定义路径或接口名称
     * @param interfaceClass 服务接口的Class对象
     * @return 已注册的ServiceDescriptor对象
     */
    public ServiceDescriptor registerService(String path, Class<?> interfaceClass) {
        // 首先注册服务获取ServiceDescriptor
        ServiceDescriptor serviceDescriptor = registerService(interfaceClass);

        // if path is different with interface name, add extra path mapping
        // 如果自定义path与接口名称不同，则添加额外的路径映射
        if (!interfaceClass.getName().equals(path)) {
            List<ServiceDescriptor> serviceDescriptors =
                    ConcurrentHashMapUtils.computeIfAbsent(services, path, _k -> new CopyOnWriteArrayList<>());

            // 同步检查是否存在相同接口的ServiceDescriptor，避免重复添加
            synchronized (serviceDescriptors) {
                Optional<ServiceDescriptor> previous = serviceDescriptors.stream()
                        .filter(s -> s.getServiceInterfaceClass().equals(serviceDescriptor.getServiceInterfaceClass()))
                        .findFirst();
                if (previous.isPresent()) {
                    // 如果已存在相同接口的描述符，直接返回
                    return previous.get();
                } else {
                    // 否则添加到路径映射中
                    serviceDescriptors.add(serviceDescriptor);
                    return serviceDescriptor;
                }
            }
        }
        return serviceDescriptor;
    }

    @Deprecated
    public void reRegisterProvider(String newServiceKey, String serviceKey) {
        ProviderModel providerModel = this.providers.get(serviceKey);
        frameworkServiceRepository.unregisterProvider(providerModel);
        providerModel.setServiceKey(newServiceKey);
        this.providers.putIfAbsent(newServiceKey, providerModel);
        frameworkServiceRepository.registerProvider(providerModel);
        this.providers.remove(serviceKey);
    }

    @Deprecated
    public void reRegisterConsumer(String newServiceKey, String serviceKey) {
        List<ConsumerModel> consumerModel = this.consumers.get(serviceKey);
        consumerModel.forEach(c -> c.setServiceKey(newServiceKey));
        ConcurrentHashMapUtils.computeIfAbsent(this.consumers, newServiceKey, (k) -> new CopyOnWriteArrayList<>())
                .addAll(consumerModel);
        this.consumers.remove(serviceKey);
    }

    public void unregisterService(Class<?> interfaceClazz) {
        // TODO remove
        unregisterService(interfaceClazz.getName());
    }

    public void unregisterService(String path) {
        services.remove(path);
    }

    public void unregisterProvider(ProviderModel providerModel) {
        frameworkServiceRepository.unregisterProvider(providerModel);
        providers.remove(providerModel.getServiceKey());
    }

    public void unregisterConsumer(ConsumerModel consumerModel) {
        consumers.get(consumerModel.getServiceKey()).remove(consumerModel);
    }

    public List<ServiceDescriptor> getAllServices() {
        List<ServiceDescriptor> serviceDescriptors =
                services.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        return Collections.unmodifiableList(serviceDescriptors);
    }

    public ServiceDescriptor getService(String serviceName) {
        // TODO, may need to distinguish service by class loader.
        List<ServiceDescriptor> serviceDescriptors = services.get(serviceName);
        if (CollectionUtils.isEmpty(serviceDescriptors)) {
            return null;
        }
        return serviceDescriptors.get(0);
    }

    public ServiceDescriptor lookupService(String interfaceName) {
        if (interfaceName != null && services.containsKey(interfaceName)) {
            List<ServiceDescriptor> serviceDescriptors = services.get(interfaceName);
            return serviceDescriptors.size() > 0 ? serviceDescriptors.get(0) : null;
        } else {
            return null;
        }
    }

    public MethodDescriptor lookupMethod(String interfaceName, String methodName) {
        ServiceDescriptor serviceDescriptor = lookupService(interfaceName);
        if (serviceDescriptor == null) {
            return null;
        }

        List<MethodDescriptor> methods = serviceDescriptor.getMethods(methodName);
        if (CollectionUtils.isEmpty(methods)) {
            return null;
        }
        return methods.iterator().next();
    }

    public List<ProviderModel> getExportedServices() {
        return Collections.unmodifiableList(new ArrayList<>(providers.values()));
    }

    public ProviderModel lookupExportedService(String serviceKey) {
        return providers.get(serviceKey);
    }

    public List<ConsumerModel> getReferredServices() {
        List<ConsumerModel> consumerModels =
                consumers.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        return Collections.unmodifiableList(consumerModels);
    }

    /**
     * @deprecated Replaced to {@link ModuleServiceRepository#lookupReferredServices(String)}
     */
    @Deprecated
    public ConsumerModel lookupReferredService(String serviceKey) {
        if (consumers.containsKey(serviceKey)) {
            List<ConsumerModel> consumerModels = consumers.get(serviceKey);
            return consumerModels.size() > 0 ? consumerModels.get(0) : null;
        } else {
            return null;
        }
    }

    public List<ConsumerModel> lookupReferredServices(String serviceKey) {
        return consumers.get(serviceKey);
    }

    public void destroy() {
        for (ProviderModel providerModel : providers.values()) {
            frameworkServiceRepository.unregisterProvider(providerModel);
        }
        providers.clear();
        consumers.clear();
        services.clear();
    }
}
