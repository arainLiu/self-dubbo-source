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
import org.apache.dubbo.common.aot.NativeDetector;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.MetadataRequest;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.MetadataServiceV2;
import org.apache.dubbo.metadata.MetadataServiceV2Detector;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.util.MetadataServiceVersionUtils;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.filter.FilterChainBuilder;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.service.Destroyable;
import org.apache.dubbo.rpc.stub.StubSuppliers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.FILTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.NATIVE_STUB;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.PROXY_CLASS_REF;
import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_FILTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_CREATE_INSTANCE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_LOAD_METADATA;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_KEY;
import static org.apache.dubbo.metadata.util.MetadataServiceVersionUtils.V2;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.METADATA_SERVICE_URLS_PROPERTY_NAME;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.METADATA_SERVICE_VERSION_NAME;
import static org.apache.dubbo.rpc.Constants.AUTH_KEY;
import static org.apache.dubbo.rpc.Constants.PROXY_KEY;

public class MetadataUtils {
    public static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(MetadataUtils.class);

        /**
     * 发布服务定义元数据到远程元数据中心，支持提供者和消费者两种模式。
     * <p>
     * 该方法用于将服务的完整定义信息（包括接口、方法、参数类型等）发布到配置的元数据报告中，
     * 使得服务治理平台可以获取详细的服务契约信息，用于服务测试、Mock、监控等场景。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>检查是否配置了元数据报告，如果未配置则记录日志并直接返回</li>
     *   <li>根据URL的side参数判断是提供者还是消费者：
     *     <ul>
     *       <li><b>提供者模式</b>：从ServiceDescriptor中获取FullServiceDefinition，设置URL参数后，
     *           遍历所有元数据报告，调用storeProviderMetadata()存储服务定义</li>
     *       <li><b>消费者模式</b>：遍历所有元数据报告，调用storeConsumerMetadata()仅存储消费者的URL参数</li>
     *     </ul>
     *   </li>
     *   <li>对于每个元数据报告，检查是否启用了服务定义上报功能（shouldReportDefinition），未启用则跳过</li>
     *   <li>构造MetadataIdentifier作为唯一标识，包含服务接口、版本、分组、端类型和应用名称</li>
     * </ol>
     * </p>
     * <p>
     * 异常处理策略：所有异常都会被捕获并记录错误日志，不会中断业务流程，
     * 因为元数据发布是辅助功能，不影响核心的服务注册发现。
     * </p>
     *
     * @param url 服务URL，包含服务接口、版本、分组、端类型等信息
     * @param serviceDescriptor 服务描述符对象，用于获取服务的完整定义信息（仅提供者模式使用）
     * @param applicationModel 应用模型，用于获取元数据报告实例和应用名称
     */
    public static void publishServiceDefinition(
            URL url, ServiceDescriptor serviceDescriptor, ApplicationModel applicationModel) {
        if (getMetadataReports(applicationModel).isEmpty()) {
            logger.info("[METADATA_REGISTER] Remote Metadata Report Server is not provided or unavailable, "
                    + "will stop registering service definition to remote center!");
            return;
        }

        try {
            String side = url.getSide();
            if (PROVIDER_SIDE.equalsIgnoreCase(side)) {
                /*
                 * 提供者模式：发布完整的服务定义信息到元数据中心
                 */
                String serviceKey = url.getServiceKey();
                FullServiceDefinition serviceDefinition = serviceDescriptor.getFullServiceDefinition(serviceKey);

                if (StringUtils.isNotEmpty(serviceKey) && serviceDefinition != null) {
                    serviceDefinition.setParameters(url.getParameters());
                    for (Map.Entry<String, MetadataReport> entry :
                            getMetadataReports(applicationModel).entrySet()) {
                        MetadataReport metadataReport = entry.getValue();
                        if (!metadataReport.shouldReportDefinition()) {
                            logger.info("Report of service definition is disabled for " + entry.getKey());
                            continue;
                        }
                        metadataReport.storeProviderMetadata(
                                new MetadataIdentifier(
                                        url.getServiceInterface(),
                                        url.getVersion() == null ? "" : url.getVersion(),
                                        url.getGroup() == null ? "" : url.getGroup(),
                                        PROVIDER_SIDE,
                                        applicationModel.getApplicationName()),
                                serviceDefinition);
                    }
                }
            } else {
                /*
                 * 消费者模式：仅发布消费者的URL参数到元数据中心
                 */
                for (Map.Entry<String, MetadataReport> entry :
                        getMetadataReports(applicationModel).entrySet()) {
                    MetadataReport metadataReport = entry.getValue();
                    if (!metadataReport.shouldReportDefinition()) {
                        logger.info("Report of service definition is disabled for " + entry.getKey());
                        continue;
                    }
                    metadataReport.storeConsumerMetadata(
                            new MetadataIdentifier(
                                    url.getServiceInterface(),
                                    url.getVersion() == null ? "" : url.getVersion(),
                                    url.getGroup() == null ? "" : url.getGroup(),
                                    CONSUMER_SIDE,
                                    applicationModel.getApplicationName()),
                            url.getParameters());
                }
            }
        } catch (Exception e) {
            // ignore error
            logger.error(REGISTRY_FAILED_CREATE_INSTANCE, "", "", "publish service definition metadata error.", e);
        }
    }


    /**
     * 引用远程元数据服务，创建用于拉取提供者MetadataInfo的RPC代理对象。
     * <p>
     * 该方法是Dubbo3应用级服务发现中元数据拉取的核心入口，负责从指定的ServiceInstance构建MetadataService的Invoker代理。
     * 支持MetadataServiceV2（Triple协议原生存根）和传统MetadataService两种版本，优先使用V2以获得更好的性能。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>构建元数据URL</b>：调用buildMetadataUrl从ServiceInstance中提取元数据服务的访问地址（通常是tri://ip:port格式）</li>
     *   <li><b>获取内部模块</b>：从applicationModel.getInternalModule()获取框架内部使用的ModuleModel，避免与业务模块混淆</li>
     *   <li><b>版本检测</b>：
     *     <ul>
     *       <li>检查instance是否支持MetadataServiceV2（通过url attribute中的METADATA_SERVICE_VERSION_NAME判断）</li>
     *       <li>检测当前环境是否支持V2（Native Image环境下强制降级到JDK动态代理+传统MetadataService）</li>
     *     </ul>
     *   </li>
     *   <li><b>注册消费者模型</b>：
     *     <ul>
     *       <li>V2模式：注册MetadataServiceV2接口，设置proxy=native-stub使用Triple协议原生存根</li>
     *       <li>传统模式：注册MetadataService接口，使用默认的Javassist代理</li>
     *     </ul>
     *   </li>
     *   <li><b>安全认证配置</b>：如果URL中设置了auth=true，添加consumersign过滤器用于请求签名验证</li>
     *   <li><b>创建Invoker</b>：调用protocol.refer建立与提供者元数据端点的连接，生成可远程调用的Invoker对象</li>
     *   <li><b>构建过滤器链</b>：如果启用认证，通过FilterChainBuilder构建包含签名验证的拦截器链</li>
     *   <li><b>生成代理</b>：调用proxyFactory.getProxy将Invoker包装为本地代理对象，实现透明的RPC调用</li>
     *   <li><b>完善ConsumerModel</b>：将代理对象设置到consumerModel的ServiceMetadata中，便于后续监控和生命周期管理</li>
     * </ol>
     * </p>
     *
     * @param instance 服务实例对象，包含应用名、主机地址、端口、元数据服务等注册时上报的完整信息
     * @return RemoteMetadataService封装对象，内部持有MetadataService或MetadataServiceV2的RPC代理，可通过getInternalProxy()获取
     */
    public static RemoteMetadataService referMetadataService(ServiceInstance instance) {
        URL url = buildMetadataUrl(instance);

        // Simply rely on the first metadata url, as stated in MetadataServiceURLBuilder.
        ApplicationModel applicationModel = instance.getApplicationModel();
        ModuleModel internalModel = applicationModel.getInternalModule();

        ConsumerModel consumerModel;

        boolean useV2 = MetadataServiceDelegationV2.VERSION.equals(url.getAttribute(METADATA_SERVICE_VERSION_NAME));
        if (!MetadataServiceV2Detector.support()) {
            useV2 = false;
        }
        boolean inNativeImage = NativeDetector.inNativeImage();

        if (useV2 && !inNativeImage) {
            /*
             * 优先使用MetadataServiceV2：
             * 基于Triple协议的原生存根，性能更好且支持流式传输
             */
            url = url.addParameter(PROXY_KEY, NATIVE_STUB);
            url = url.setPath(MetadataServiceV2.class.getName());
            url = url.addParameter(VERSION_KEY, V2);

            consumerModel = applicationModel
                    .getInternalModule()
                    .registerInternalConsumer(
                            MetadataServiceV2.class,
                            url,
                            StubSuppliers.getServiceDescriptor(MetadataServiceV2.class.getName()));
        } else {
            /*
             * 降级到传统MetadataService：
             * 使用Javassist动态代理，兼容老版本提供者
             */
            consumerModel = applicationModel.getInternalModule().registerInternalConsumer(MetadataService.class, url);
        }

        if (inNativeImage) {
            /*
             * Native Image环境特殊处理：
             * GraalVM不支持字节码生成，强制使用JDK动态代理
             */
            url = url.addParameter(PROXY_KEY, "jdk");
        }

        Protocol protocol = applicationModel.getExtensionLoader(Protocol.class).getExtension(url.getProtocol(), false);

        url = url.setServiceModel(consumerModel);
        if (url.getParameter(AUTH_KEY, false)) {
            /*
             * 启用安全认证：
             * 添加consumersign过滤器，在RPC调用时附加签名参数
             */
            url = url.addParameter(FILTER_KEY, "-default,consumersign");
        }

        RemoteMetadataService remoteMetadataService;
        ProxyFactory proxyFactory =
                applicationModel.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        if (useV2 && !inNativeImage) {
            Invoker<MetadataServiceV2> invoker = protocol.refer(MetadataServiceV2.class, url);

            if (url.getParameter(AUTH_KEY, false)) {
                FilterChainBuilder filterChainBuilder = ScopeModelUtil.getExtensionLoader(
                                FilterChainBuilder.class, url.getScopeModel())
                        .getDefaultExtension();
                invoker = filterChainBuilder.buildInvokerChain(invoker, REFERENCE_FILTER_KEY, CommonConstants.CONSUMER);
            }

            remoteMetadataService =
                    new RemoteMetadataService(consumerModel, proxyFactory.getProxy(invoker), internalModel);
        } else {
            Invoker<MetadataService> invoker = protocol.refer(MetadataService.class, url);

            if (url.getParameter(AUTH_KEY, false)) {
                FilterChainBuilder filterChainBuilder = ScopeModelUtil.getExtensionLoader(
                                FilterChainBuilder.class, url.getScopeModel())
                        .getDefaultExtension();
                invoker = filterChainBuilder.buildInvokerChain(invoker, REFERENCE_FILTER_KEY, CommonConstants.CONSUMER);
            }

            remoteMetadataService =
                    new RemoteMetadataService(consumerModel, proxyFactory.getProxy(invoker), internalModel);
        }

        Object metadataServiceProxy = remoteMetadataService.getInternalProxy();
        consumerModel.getServiceMetadata().setTarget(metadataServiceProxy);
        consumerModel.getServiceMetadata().addAttribute(PROXY_CLASS_REF, metadataServiceProxy);
        consumerModel.setProxyObject(metadataServiceProxy);
        consumerModel.initMethodModels();

        return remoteMetadataService;
    }

    private static URL buildMetadataUrl(ServiceInstance instance) {
        MetadataServiceURLBuilder builder;
        ExtensionLoader<MetadataServiceURLBuilder> loader =
                instance.getApplicationModel().getExtensionLoader(MetadataServiceURLBuilder.class);

        Map<String, String> metadata = instance.getMetadata();
        // METADATA_SERVICE_URLS_PROPERTY_NAME is a unique key exists only on instances of spring-cloud-alibaba.
        String dubboUrlsForJson = metadata.get(METADATA_SERVICE_URLS_PROPERTY_NAME);
        if (metadata.isEmpty() || StringUtils.isEmpty(dubboUrlsForJson)) {
            builder = loader.getExtension(StandardMetadataServiceURLBuilder.NAME);
        } else {
            builder = loader.getExtension(SpringCloudMetadataServiceURLBuilder.NAME);
        }

        List<URL> urls = builder.build(instance);
        if (CollectionUtils.isEmpty(urls)) {
            throw new IllegalStateException("Introspection service discovery mode is enabled " + instance
                    + ", but no metadata service can build from it.");
        }
        URL url = urls.get(0);

        String version = metadata.get(METADATA_SERVICE_VERSION_NAME);
        url = url.putAttribute(METADATA_SERVICE_VERSION_NAME, version);
        url = url.addParameter(CHECK_KEY, false);

        return url;
    }

    /**
     * 从远程获取指定Revision的应用级元数据信息，支持远程存储和实例直连两种模式。
     * <p>
     * 该方法是Dubbo3应用级服务发现中元数据拉取的核心工具方法，负责根据元数据存储类型选择合适的获取策略。
     * 当元数据存储在远程中心（如Zookeeper/Nacos）时直接查询；当元数据存储在提供者实例本地时，通过RPC调用MetadataService获取。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>选择实例</b>：调用selectInstance从instances列表中选择一个可用的ServiceInstance（通常采用负载均衡策略）</li>
     *   <li><b>获取存储类型</b>：调用getMetadataStorageType读取实例的metadata-type参数，判断是remote还是local模式</li>
     *   <li><b>分支处理</b>：
     *     <ul>
     *       <li><b>远程存储模式（REMOTE_METADATA_STORAGE_TYPE）</b>：调用getMetadata直接从metadataReport查询元数据中心，避免增加提供者负担</li>
     *       <li><b>本地存储模式（默认）</b>：调用referMetadataService创建临时的RemoteMetadataService代理，通过RPC调用提供者的MetadataService接口获取元数据，使用后立即销毁避免资源泄漏</li>
     *     </ul>
     *   </li>
     *   <li><b>异常容错</b>：捕获所有Exception，记录ERROR日志后返回null，避免因单个实例元数据获取失败影响整体订阅流程</li>
     *   <li><b>空值保护</b>：如果metadataInfo为null（网络异常、实例宕机等），返回MetadataInfo.EMPTY占位对象，避免调用方出现NullPointerException</li>
     * </ol>
     * </p>
     *
     * @param revision       元数据的版本号，用于标识特定时刻的应用配置快照，格式通常为MD5哈希值
     * @param instances      服务实例列表，包含多个相同应用的实例地址，方法会从中选择一个进行通信
     * @param metadataReport 元数据报告实例，用于远程存储模式下查询元数据中心，可能为null（纯本地存储场景）
     * @return 应用级元数据信息对象，包含所有服务接口的定义、方法列表、配置参数等，如果获取失败则返回EMPTY常量
     */
    public static MetadataInfo getRemoteMetadata(
            String revision, List<ServiceInstance> instances, MetadataReport metadataReport) {
        ServiceInstance instance = selectInstance(instances);
        String metadataType = ServiceInstanceMetadataUtils.getMetadataStorageType(instance);
        MetadataInfo metadataInfo;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Instance " + instance.getAddress() + " is using metadata type " + metadataType);
            }
            if (REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                /*
                 * 远程存储模式：
                 * 直接从元数据中心（Zookeeper/Nacos）查询，减轻提供者实例负载
                 */
                metadataInfo = MetadataUtils.getMetadata(revision, instance, metadataReport);
            } else {
                /*
                 * 本地存储模式：
                 * 通过RPC调用提供者实例的MetadataService接口获取元数据，使用临时代理后立即销毁
                 */
                // change the instance used to communicate to avoid all requests route to the same instance
                RemoteMetadataService remoteMetadataService = null;
                try {
                    remoteMetadataService = MetadataUtils.referMetadataService(instance);
                    metadataInfo = remoteMetadataService.getRemoteMetadata(
                            ServiceInstanceMetadataUtils.getExportedServicesRevision(instance));
                } finally {
                    MetadataUtils.destroyProxy(remoteMetadataService);
                }
            }
        } catch (Exception e) {
            logger.error(
                    REGISTRY_FAILED_LOAD_METADATA,
                    "",
                    "",
                    "Failed to get app metadata for revision " + revision + " for type " + metadataType
                            + " from instance " + instance.getAddress(),
                    e);
            metadataInfo = null;
        }

        if (metadataInfo == null) {
            metadataInfo = MetadataInfo.EMPTY;
        }
        return metadataInfo;
    }

    // ... existing code ...


    public static void destroyProxy(RemoteMetadataService remoteMetadataService) {
        if (remoteMetadataService != null) {
            remoteMetadataService.destroy();
        }
    }

    public static MetadataInfo getMetadata(String revision, ServiceInstance instance, MetadataReport metadataReport) {
        SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier(instance.getServiceName(), revision);

        if (metadataReport == null) {
            throw new IllegalStateException("No valid remote metadata report specified.");
        }

        String registryCluster = instance.getRegistryCluster();
        Map<String, String> params = new HashMap<>(instance.getExtendParams());
        if (registryCluster != null && !registryCluster.equalsIgnoreCase(params.get(REGISTRY_CLUSTER_KEY))) {
            params.put(REGISTRY_CLUSTER_KEY, registryCluster);
        }

        return metadataReport.getAppMetadata(identifier, params);
    }

    private static Map<String, MetadataReport> getMetadataReports(ApplicationModel applicationModel) {
        return applicationModel
                .getBeanFactory()
                .getBean(MetadataReportInstance.class)
                .getMetadataReports(false);
    }

    private static ServiceInstance selectInstance(List<ServiceInstance> instances) {
        if (instances.size() == 1) {
            return instances.get(0);
        }
        return instances.get(ThreadLocalRandom.current().nextInt(0, instances.size()));
    }

    public static class RemoteMetadataService {
        private final ConsumerModel consumerModel;

        @Deprecated
        private MetadataService proxy;

        private MetadataServiceV2 proxyV2;

        private final ModuleModel internalModel;

        public RemoteMetadataService(ConsumerModel consumerModel, MetadataService proxy, ModuleModel internalModel) {
            this.consumerModel = consumerModel;
            this.proxy = proxy;
            this.internalModel = internalModel;
        }

        public RemoteMetadataService(
                ConsumerModel consumerModel, MetadataServiceV2 proxyV2, ModuleModel internalModel) {
            this.consumerModel = consumerModel;
            this.proxyV2 = proxyV2;
            this.internalModel = internalModel;
        }

        public void destroy() {
            if (proxy instanceof Destroyable) {
                ((Destroyable) proxy).$destroy();
            }

            if (proxyV2 instanceof Destroyable) {
                ((Destroyable) proxyV2).$destroy();
            }

            internalModel.getServiceRepository().unregisterConsumer(consumerModel);
        }

        public ConsumerModel getConsumerModel() {
            return consumerModel;
        }

        public Object getInternalProxy() {
            return proxy == null ? proxyV2 : proxy;
        }

        public ModuleModel getInternalModel() {
            return internalModel;
        }

        public MetadataInfo getRemoteMetadata(String revision) {
            Object existProxy = getInternalProxy();
            if (existProxy instanceof MetadataService) {
                return ((MetadataService) existProxy).getMetadataInfo(revision);
            } else {
                return MetadataServiceVersionUtils.toV1(((MetadataServiceV2) existProxy)
                        .getMetadataInfo(MetadataRequest.newBuilder()
                                .setRevision(revision)
                                .build()));
            }
        }
    }
}
