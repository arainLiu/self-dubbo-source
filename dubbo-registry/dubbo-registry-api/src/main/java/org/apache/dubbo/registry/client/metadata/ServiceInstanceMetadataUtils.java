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
package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.registry.event.RegistryEvent;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.DefaultServiceInstance.Endpoint;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstanceCustomizer;
import org.apache.dubbo.registry.support.RegistryManager;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.registry.integration.InterfaceCompatibleRegistryProtocol.DEFAULT_REGISTER_PROVIDER_KEYS;
import static org.apache.dubbo.rpc.Constants.DEPRECATED_KEY;

/**
 * The Utilities class for the {@link ServiceInstance#getMetadata() metadata of the service instance}
 *
 * @see StandardMetadataServiceURLBuilder
 * @see ServiceInstance#getMetadata()
 * @see MetadataService
 * @see URL
 * @since 2.7.5
 */
public class ServiceInstanceMetadataUtils {
    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(ServiceInstanceMetadataUtils.class);

    /**
     * The prefix of {@link MetadataService} : "dubbo.metadata-service."
     */
    public static final String METADATA_SERVICE_PREFIX = "dubbo.metadata-service.";

    public static final String ENDPOINTS = "dubbo.endpoints";

    /**
     * The property name of metadata JSON of {@link MetadataService}'s {@link URL}
     */
    public static final String METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "url-params";

    /**
     * The {@link URL URLs} property name of {@link MetadataService} :
     * "dubbo.metadata-service.urls", which is used to be compatible with Dubbo Spring Cloud and
     * discovery the metadata of instance
     */
    public static final String METADATA_SERVICE_URLS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "urls";

    /**
     * The property name of The revision for all exported Dubbo services.
     */
    public static final String EXPORTED_SERVICES_REVISION_PROPERTY_NAME = "dubbo.metadata.revision";

    /**
     * The property name of metadata storage type.
     */
    public static final String METADATA_STORAGE_TYPE_PROPERTY_NAME = "dubbo.metadata.storage-type";

    public static final String METADATA_SERVICE_VERSION_NAME = "meta-v";

    public static final String METADATA_CLUSTER_PROPERTY_NAME = "dubbo.metadata.cluster";

    public static String getMetadataServiceParameter(URL url) {
        if (url == null) {
            return "";
        }
        url = url.removeParameters(APPLICATION_KEY, GROUP_KEY, DEPRECATED_KEY, TIMESTAMP_KEY);
        Map<String, String> params = getParams(url);

        if (params.isEmpty()) {
            return null;
        }

        return JsonUtils.toJson(params);
    }

    private static Map<String, String> getParams(URL providerURL) {
        Map<String, String> params = new LinkedHashMap<>();
        setDefaultParams(params, providerURL);
        params.put(PORT_KEY, String.valueOf(providerURL.getPort()));
        params.put(PROTOCOL_KEY, providerURL.getProtocol());
        return params;
    }

    /**
     * The revision for all exported Dubbo services from the specified {@link ServiceInstance}.
     *
     * @param serviceInstance the specified {@link ServiceInstance}
     * @return <code>null</code> if not exits
     */
    public static String getExportedServicesRevision(ServiceInstance serviceInstance) {
        return Optional.ofNullable(serviceInstance.getServiceMetadata())
                .map(MetadataInfo::getRevision)
                .filter(StringUtils::isNotEmpty)
                .orElse(serviceInstance.getMetadata(EXPORTED_SERVICES_REVISION_PROPERTY_NAME));
    }

    /**
     * Get metadata's storage type
     *
     * @param registryURL the {@link URL} to connect the registry
     * @return if not found in {@link URL#getParameters() parameters} of {@link URL registry URL}, return
     */
    public static String getMetadataStorageType(URL registryURL) {
        return registryURL.getParameter(METADATA_STORAGE_TYPE_PROPERTY_NAME, DEFAULT_METADATA_STORAGE_TYPE);
    }

    /**
     * Get the metadata storage type specified by the peer instance.
     *
     * @return storage type, remote or local
     */
    public static String getMetadataStorageType(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.getOrDefault(METADATA_STORAGE_TYPE_PROPERTY_NAME, DEFAULT_METADATA_STORAGE_TYPE);
    }

    /**
     * Set the metadata storage type in specified {@link ServiceInstance service instance}
     *
     * @param serviceInstance {@link ServiceInstance service instance}
     * @param metadataType    remote or local
     */
    public static void setMetadataStorageType(ServiceInstance serviceInstance, String metadataType) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        metadata.put(METADATA_STORAGE_TYPE_PROPERTY_NAME, metadataType);
    }

    public static String getRemoteCluster(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.get(METADATA_CLUSTER_PROPERTY_NAME);
    }

    public static boolean hasEndpoints(ServiceInstance serviceInstance) {
        return StringUtils.isNotEmpty(serviceInstance.getMetadata().get(ENDPOINTS));
    }

    public static void setEndpoints(ServiceInstance serviceInstance, Map<String, Integer> protocolPorts) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        List<Endpoint> endpoints = new ArrayList<>();
        protocolPorts.forEach((k, v) -> {
            Endpoint endpoint = new Endpoint(v, k);
            endpoints.add(endpoint);
        });

        metadata.put(ENDPOINTS, JsonUtils.toJson(endpoints));
    }

    /**
     * Get the property value of port by the specified {@link ServiceInstance#getMetadata() the metadata of
     * service instance} and protocol
     *
     * @param serviceInstance {@link ServiceInstance service instance}
     * @param protocol        the name of protocol, e.g, dubbo, rest, and so on
     * @return if not found, return <code>null</code>
     */
    public static Endpoint getEndpoint(ServiceInstance serviceInstance, String protocol) {
        List<Endpoint> endpoints = ((DefaultServiceInstance) serviceInstance).getEndpoints();
        if (endpoints != null) {
            for (Endpoint endpoint : endpoints) {
                if (endpoint.getProtocol().equals(protocol)) {
                    return endpoint;
                }
            }
        }
        return null;
    }

    public static void registerMetadataAndInstance(ApplicationModel applicationModel) {
        RegistryManager registryManager = applicationModel.getBeanFactory().getBean(RegistryManager.class);
        // register service instance
        if (CollectionUtils.isNotEmpty(registryManager.getServiceDiscoveries())) {
            LOGGER.info("[METADATA_REGISTER] Start registering instance address to registry.");
            List<ServiceDiscovery> serviceDiscoveries = registryManager.getServiceDiscoveries();
            for (ServiceDiscovery serviceDiscovery : serviceDiscoveries) {
                MetricsEventBus.post(
                        RegistryEvent.toRegisterEvent(
                                applicationModel, Collections.singletonList(getServiceDiscoveryName(serviceDiscovery))),
                        () -> {
                            // register service instance
                            serviceDiscovery.register();
                            return null;
                        });
            }
        }
    }

    private static String getServiceDiscoveryName(ServiceDiscovery serviceDiscovery) {
        return serviceDiscovery
                .getUrl()
                .getParameter(
                        RegistryConstants.REGISTRY_CLUSTER_KEY,
                        serviceDiscovery.getUrl().getParameter(REGISTRY_KEY));
    }

        /**
     * 刷新应用元数据和服务实例信息，确保服务发现信息的实时性和准确性。
     * <p>
     * 该方法通过获取应用模型中的注册管理器，遍历所有的服务发现组件并触发其更新操作。
     * 主要用于服务配置变更、实例状态变化等场景，确保注册中心和服务消费者能够获取到最新的
     * 服务实例元数据和版本信息（revision）。
     * </p>
     *
     * @param applicationModel 应用模型对象，用于获取注册管理器和相关组件
     */
    public static void refreshMetadataAndInstance(ApplicationModel applicationModel) {
        /*
         * 获取注册管理器，管理当前应用的所有注册和服务发现组件
         */
        RegistryManager registryManager = applicationModel.getBeanFactory().getBean(RegistryManager.class);
        // update service instance revision
        /*
         * 遍历所有服务发现组件，触发实例元数据和版本信息的更新
         */
        registryManager.getServiceDiscoveries().forEach(ServiceDiscovery::update);
    }


    public static void unregisterMetadataAndInstance(ApplicationModel applicationModel) {
        RegistryManager registryManager = applicationModel.getBeanFactory().getBean(RegistryManager.class);
        registryManager.getServiceDiscoveries().forEach(serviceDiscovery -> {
            try {
                serviceDiscovery.unregister();
            } catch (Exception ignored) {
                // ignored
            }
        });
    }

    /**
     * 应用所有服务实例定制器，对服务实例进行自定义扩展处理。
     * <p>
     * 该方法通过SPI机制加载所有已注册的{@link ServiceInstanceCustomizer}实现，并依次调用其customize()方法，
     * 允许开发者在不修改核心代码的情况下为服务实例添加额外的属性、标签或元数据信息。
     * 这种设计遵循开闭原则，支持通过插件化方式扩展服务实例的功能。
     * </p>
     * <p>
     * 典型使用场景包括：
     * <ul>
     *   <li>添加自定义的实例标签（如机房标识、环境标识等）</li>
     *   <li>设置额外的元数据键值对（如版本号、权重等）</li>
     *   <li>根据业务需求修改实例的网络地址或端口信息</li>
     * </ul>
     * </p>
     * <p>
     * 注意：当前实现中未对定制器进行排序（见FIXME注释），定制器的执行顺序取决于SPI加载的顺序，
     * 如果多个定制器之间存在依赖关系，可能会导致不确定的行为。
     * </p>
     *
     * @param instance 要定制的服务实例对象，定制器会直接修改该实例的属性
     * @param applicationModel 应用模型，提供定制器执行所需的上下文信息和配置
     */
    public static void customizeInstance(ServiceInstance instance, ApplicationModel applicationModel) {
        ExtensionLoader<ServiceInstanceCustomizer> loader =
                instance.getOrDefaultApplicationModel().getExtensionLoader(ServiceInstanceCustomizer.class);
        // FIXME, sort customizer before apply
        loader.getSupportedExtensionInstances().forEach(customizer -> {
            // customize
            customizer.customize(instance, applicationModel);
        });
    }

    public static boolean isValidInstance(ServiceInstance instance) {
        return instance != null && instance.getHost() != null && instance.getPort() != 0;
    }

    /**
     * Set the default parameters via the specified {@link URL providerURL}
     *
     * @param params      the parameters
     * @param providerURL the provider's {@link URL}
     */
    private static void setDefaultParams(Map<String, String> params, URL providerURL) {
        for (String parameterName : DEFAULT_REGISTER_PROVIDER_KEYS) {
            String parameterValue = providerURL.getParameter(parameterName);
            if (!isBlank(parameterValue)) {
                params.put(parameterName, parameterValue);
            }
        }
    }
}
