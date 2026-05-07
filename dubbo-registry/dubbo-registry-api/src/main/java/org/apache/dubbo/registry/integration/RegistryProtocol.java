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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.registry.event.RegistryEvent;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.client.ServiceDiscoveryRegistryDirectory;
import org.apache.dubbo.registry.client.migration.MigrationClusterInvoker;
import org.apache.dubbo.registry.client.migration.ServiceDiscoveryMigrationInvoker;
import org.apache.dubbo.registry.retry.ReExportTask;
import org.apache.dubbo.registry.support.SkipFailbackWrapperException;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.EXTRA_KEYS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.IPV6_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MERGEABLE_CLUSTER_NAME;
import static org.apache.dubbo.common.constants.CommonConstants.PACKABLE_METHOD_FACTORY_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PASSWORD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_PROTOCOL_LISTENER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RELEASE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.USERNAME_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_UNSUPPORTED_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ALL_CATEGORIES;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.OVERRIDE_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.utils.StringUtils.isEmpty;
import static org.apache.dubbo.common.utils.UrlUtils.classifyUrls;
import static org.apache.dubbo.registry.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.ENABLE_26X_CONFIGURATION_LISTEN;
import static org.apache.dubbo.registry.Constants.ENABLE_CONFIGURATION_LISTEN;
import static org.apache.dubbo.registry.Constants.PROVIDER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;
import static org.apache.dubbo.remoting.Constants.CODEC_KEY;
import static org.apache.dubbo.remoting.Constants.CONNECTIONS_KEY;
import static org.apache.dubbo.remoting.Constants.EXCHANGER_KEY;
import static org.apache.dubbo.remoting.Constants.PREFER_SERIALIZATION_KEY;
import static org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY;
import static org.apache.dubbo.rpc.Constants.AUTHENTICATOR_KEY;
import static org.apache.dubbo.rpc.Constants.AUTH_KEY;
import static org.apache.dubbo.rpc.Constants.DEPRECATED_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.MOCK_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CONSUMER_URL_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;
import static org.apache.dubbo.rpc.model.ScopeModelUtil.getApplicationModel;

/**
 * TODO, replace RegistryProtocol completely in the future.
 */
public class RegistryProtocol implements Protocol, ScopeModelAware {
    public static final String[] DEFAULT_REGISTER_PROVIDER_KEYS = {
        APPLICATION_KEY,
        CODEC_KEY,
        EXCHANGER_KEY,
        SERIALIZATION_KEY,
        PREFER_SERIALIZATION_KEY,
        CLUSTER_KEY,
        CONNECTIONS_KEY,
        DEPRECATED_KEY,
        GROUP_KEY,
        LOADBALANCE_KEY,
        MOCK_KEY,
        PATH_KEY,
        TIMEOUT_KEY,
        TOKEN_KEY,
        VERSION_KEY,
        WARMUP_KEY,
        WEIGHT_KEY,
        DUBBO_VERSION_KEY,
        RELEASE_KEY,
        SIDE_KEY,
        IPV6_KEY,
        PACKABLE_METHOD_FACTORY_KEY,
        AUTH_KEY,
        AUTHENTICATOR_KEY,
        USERNAME_KEY,
        PASSWORD_KEY
    };

    public static final String[] DEFAULT_REGISTER_CONSUMER_KEYS = {
        APPLICATION_KEY, VERSION_KEY, GROUP_KEY, DUBBO_VERSION_KEY, RELEASE_KEY
    };

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(RegistryProtocol.class);

    private final Map<String, ServiceConfigurationListener> serviceConfigurationListeners = new ConcurrentHashMap<>();
    // To solve the problem of RMI repeated exposure port conflicts, the services that have been exposed are no longer
    // exposed.
    // provider url <--> registry url <--> exporter
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ExporterChangeableWrapper<?>>> bounds =
            new ConcurrentHashMap<>();
    protected Protocol protocol;
    protected ProxyFactory proxyFactory;

    private ConcurrentMap<URL, ReExportTask> reExportFailedTasks = new ConcurrentHashMap<>();
    private HashedWheelTimer retryTimer = new HashedWheelTimer(
            new NamedThreadFactory("DubboReexportTimer", true),
            DEFAULT_REGISTRY_RETRY_PERIOD,
            TimeUnit.MILLISECONDS,
            128);
    private FrameworkModel frameworkModel;
    private ExporterFactory exporterFactory;

    public RegistryProtocol() {}

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
        this.exporterFactory = frameworkModel.getBeanFactory().getBean(ExporterFactory.class);
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return 9090;
    }

    public Map<URL, Set<NotifyListener>> getOverrideListeners() {
        Map<URL, Set<NotifyListener>> map = new HashMap<>();
        List<ApplicationModel> applicationModels = frameworkModel.getApplicationModels();
        if (applicationModels.size() == 1) {
            return applicationModels
                    .get(0)
                    .getBeanFactory()
                    .getBean(ProviderConfigurationListener.class)
                    .getOverrideListeners();
        } else {
            for (ApplicationModel applicationModel : applicationModels) {
                map.putAll(applicationModel
                        .getBeanFactory()
                        .getBean(ProviderConfigurationListener.class)
                        .getOverrideListeners());
            }
        }
        return map;
    }

    /**
     * 向注册中心注册服务提供者地址，并上报注册指标事件。
     * <p>
     * 该方法封装了注册中心的核心注册逻辑，包括部署器状态管理、注册中心名称解析和监控指标上报。
     * 通过ApplicationDeployer的计数器机制，确保在并发场景下服务导出状态的准确性。
     * </p>
     * <p>
     * 注册流程：
     * <ol>
     *   <li><b>部署器计数</b>：增加服务刷新计数，用于跟踪应用启动过程中的服务导出进度</li>
     *   <li><b>注册中心名称提取</b>：从Registry URL中解析注册中心标识，优先使用REGISTRY_CLUSTER_KEY参数</li>
     *   <li><b>指标上报</b>：通过MetricsEventBus发布RegistryEvent事件，记录服务注册到指定注册中心的指标数据</li>
     *   <li><b>执行注册</b>：调用Registry.register方法将服务地址写入注册中心（如Zookeeper节点创建）</li>
     *   <li><b>计数释放</b>：在finally块中减少服务刷新计数，确保异常情况下也能正确释放资源</li>
     * </ol>
     * </p>
     *
     * @param registry              注册中心实例，可以是ZookeeperRegistry、NacosRegistry等具体实现
     * @param registeredProviderUrl 需要注册的服务提供者URL，包含服务接口、协议、地址、端口等完整信息
     */
    private static void register(Registry registry, URL registeredProviderUrl) {
        ApplicationDeployer deployer =
                registeredProviderUrl.getOrDefaultApplicationModel().getDeployer();
        try {
            /*
             * 增加服务刷新计数：
             * 用于ApplicationDeployer跟踪服务导出进度，判断应用是否完成所有服务的初始化
             */
            deployer.increaseServiceRefreshCount();
            /*
             * 解析注册中心名称：
             * 优先级顺序：REGISTRY_CLUSTER_KEY参数 > 服务发现URL的REGISTRY_KEY参数 > 协议名称 > "unknown"
             */
            String registryName = Optional.ofNullable(registry.getUrl())
                    .map(u -> u.getParameter(
                            RegistryConstants.REGISTRY_CLUSTER_KEY,
                            UrlUtils.isServiceDiscoveryURL(u) ? u.getParameter(REGISTRY_KEY) : u.getProtocol()))
                    .filter(StringUtils::isNotEmpty)
                    .orElse("unknown");
            /*
             * 发布注册指标事件：
             * 通过MetricsEventBus上报服务注册成功的事件，用于运维监控和统计分析
             */
            MetricsEventBus.post(
                    RegistryEvent.toRsEvent(
                            registeredProviderUrl.getApplicationModel(),
                            registeredProviderUrl.getServiceKey(),
                            1,
                            Collections.singletonList(registryName)),
                    () -> {
                        registry.register(registeredProviderUrl);
                        return null;
                    });
        } finally {
            /*
             * 减少服务刷新计数：
             * 无论注册成功或失败，都需要释放计数器，避免内存泄漏
             */
            deployer.decreaseServiceRefreshCount();
        }
    }

    private void registerStatedUrl(URL registryUrl, URL registeredProviderUrl, boolean registered) {
        ProviderModel model = (ProviderModel) registeredProviderUrl.getServiceModel();
        model.addStatedUrl(new ProviderModel.RegisterStatedURL(registeredProviderUrl, registryUrl, registered));
    }

    /**
     * 导出服务到注册中心，完成服务注册、本地暴露和配置监听的完整流程。
     * <p>
     * 该方法是Dubbo服务导出的核心入口（RegistryProtocol层面），负责将已经在本地的协议层Exporter进一步注册到注册中心，
     * 并建立配置监听机制以支持动态配置覆盖和服务治理规则。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>配置订阅</b>：创建OverrideListener监听器，订阅服务级别的配置覆盖规则（如权重、超时等动态调整）</li>
     *   <li><b>本地导出</b>：调用doLocalExport将Invoker导出为本地Exporter，绑定到具体协议端口</li>
     *   <li><b>注册中心交互</b>：获取Registry实例，根据register标志决定是否向注册中心注册地址</li>
     *   <li><b>状态管理</b>：在ProviderModel中注册StatedUrl，跟踪服务的注册状态</li>
     *   <li><b>兼容处理</b>：对于2.6.x版本的配置监听进行兼容性处理（已废弃）</li>
     *   <li><b>事件通知</b>：触发RegistryProtocolListener监听器，通知服务导出完成</li>
     * </ol>
     * </p>
     *
     * @param originInvoker 原始调用器，包含服务引用的所有元数据信息（地址、参数、服务模型等）
     * @return 可销毁的Exporter包装对象，每次调用都会返回新的实例以避免重复导出
     * @throws RpcException RPC异常，当注册中心连接失败、服务导出异常时抛出
     */
    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        URL registryUrl = getRegistryUrl(originInvoker);
        // url to export locally
        URL providerUrl = getProviderUrl(originInvoker);

        /*
         * 配置覆盖监听机制：
         * 订阅服务级别的动态配置规则（如通过Admin控制台下发的权重调整、超时设置等）
         * FIXME: 存在缓存key冲突问题，同一JVM内暴露和调用相同服务时会相互覆盖
         */
        // Subscribe the override data
        // FIXME When the provider subscribes, it will affect the scene : a certain JVM exposes the service and call
        //  the same service. Because the subscribed is cached key with the name of the service, it causes the
        //  subscription information to cover.
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(providerUrl);
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        ConcurrentHashMap<URL, Set<NotifyListener>> overrideListeners =
                getProviderConfigurationListener(overrideSubscribeUrl).getOverrideListeners();
        ConcurrentHashMapUtils.computeIfAbsent(overrideListeners, overrideSubscribeUrl, k -> new ConcurrentHashSet<>())
                .add(overrideSubscribeListener);

        /*
         * 应用配置覆盖规则：
         * 依次应用全局配置和服务级别配置，可能修改providerUrl中的参数值
         */
        providerUrl = overrideUrlWithConfig(providerUrl, overrideSubscribeListener);
        /*
         * 1. 暴漏本地业务服务：
         * 将Invoker包装为Exporter，绑定到具体协议端口（如dubbo://192.168.1.100:20880）
         * 使用缓存机制避免同一服务的重复导出
         */
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker, providerUrl);

        /*
         * 2. 获取注册register：
         *   - 1. 获取Registry实例（可能是ZookeeperRegistry、NacosRegistry等）
         *   - 2. 定制注册地址，合并注册中心的特定参数
         *   - 3. 根据双重REGISTER标志决定是否立即注册（providerUrl和registryUrl都需为true）
         */
        final Registry registry = getRegistry(registryUrl);
        final URL registeredProviderUrl = customizeURL(providerUrl, registryUrl);

        // decide if we need to delay publish (provider itself and registry should both need to register)
        boolean register = providerUrl.getParameter(REGISTER_KEY, true) && registryUrl.getParameter(REGISTER_KEY, true);
        if (register) {
            //key3:这里有两种情况 接口级注册会将接口级服务提供者数据直接注册到zookeeper上面，
            //服务发现(应用级注册)这里仅仅会将注册数据转换为服务元数据等后面来发布元数据
            register(registry, registeredProviderUrl);
        }

        //注册状态管理：在ProviderModel中记录服务的注册状态，用于后续的服务治理和监控
        registerStatedUrl(registryUrl, registeredProviderUrl, register);

        exporter.setRegisterUrl(registeredProviderUrl);
        exporter.setSubscribeUrl(overrideSubscribeUrl);
        exporter.setNotifyListener(overrideSubscribeListener);
        exporter.setRegistered(register);

        ApplicationModel applicationModel = getApplicationModel(providerUrl.getScopeModel());
        if (applicationModel
                .modelEnvironment()
                .getConfiguration()
                .convert(Boolean.class, ENABLE_26X_CONFIGURATION_LISTEN, true)) {
            if (!registry.isServiceDiscovery()) {
                /*
                 * Dubbo 2.6.x兼容逻辑（已废弃）：
                 * 对于非应用级服务发现的注册中心，订阅旧版本的override规则
                 */
                registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
            }
        }

        //触发导出事件通知：调用所有注册的RegistryProtocolListener监听器，用于扩展点逻辑
        notifyExport(exporter);
        // Ensure that a new exporter instance is returned every time export
        return new DestroyableExporter<>(exporter);
    }

    private <T> void notifyExport(ExporterChangeableWrapper<T> exporter) {
        ScopeModel scopeModel = exporter.getRegisterUrl().getScopeModel();
        List<RegistryProtocolListener> listeners = ScopeModelUtil.getExtensionLoader(
                        RegistryProtocolListener.class, scopeModel)
                .getActivateExtension(exporter.getOriginInvoker().getUrl(), REGISTRY_PROTOCOL_LISTENER_KEY);
        if (CollectionUtils.isNotEmpty(listeners)) {
            for (RegistryProtocolListener listener : listeners) {
                listener.onExport(this, exporter);
            }
        }
    }

    private URL overrideUrlWithConfig(URL providerUrl, OverrideListener listener) {
        ProviderConfigurationListener providerConfigurationListener = getProviderConfigurationListener(providerUrl);
        providerUrl = providerConfigurationListener.overrideUrl(providerUrl);

        ServiceConfigurationListener serviceConfigurationListener =
                new ServiceConfigurationListener(providerUrl.getOrDefaultModuleModel(), providerUrl, listener);
        serviceConfigurationListeners.put(providerUrl.getServiceKey(), serviceConfigurationListener);
        return serviceConfigurationListener.overrideUrl(providerUrl);
    }

    /**
     * 执行本地服务导出
     * <p>
     * 该方法负责将Dubbo服务导出到本地Exporter管理器中，主要执行以下操作：
     * 1. 从originInvoker中提取providerUrl和registryUrl的键值
     * 2. 创建InvokerDelegate代理对象，包装原始Invoker和providerUrl
     * 3. 通过exporterFactory创建ReferenceCountExporter，实现引用计数管理的Exporter
     * 4. 使用双层Map结构缓存ExporterChangeableWrapper：
     *    - 第一层key：providerUrlKey（服务提供者URL的标识）
     *    - 第二层key：registryUrlKey（注册中心URL的标识）
     * 5. 如果缓存中不存在，则创建新的ExporterChangeableWrapper并缓存
     * <p>
     * 该方法支持同一服务向多个注册中心注册的场景，
     * 通过双层缓存结构确保服务的正确管理和复用。
     *
     * @param originInvoker 原始的Invoker对象，包含服务引用的所有信息
     * @param providerUrl 服务提供者的URL地址
     * @param <T> 服务接口的泛型类型
     * @return 封装后的ExporterChangeableWrapper对象
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker, URL providerUrl) {
        // 获取providerUrl和registryUrl的唯一标识key
        String providerUrlKey = getProviderUrlKey(originInvoker);
        String registryUrlKey = getRegistryUrlKey(originInvoker);

        // 创建InvokerDelegate，包装原始Invoker和providerUrl
        Invoker<?> invokerDelegate = new InvokerDelegate<>(originInvoker, providerUrl);

        // 创建支持引用计数的Exporter，延迟执行protocol.export()
        ReferenceCountExporter<?> exporter =
                exporterFactory.createExporter(providerUrlKey, () -> protocol.export(invokerDelegate));

        // 使用双层Map结构缓存ExporterChangeableWrapper，支持同一服务向多个注册中心注册
        return (ExporterChangeableWrapper<T>) ConcurrentHashMapUtils.computeIfAbsent(
                ConcurrentHashMapUtils.computeIfAbsent(bounds, providerUrlKey, k -> new ConcurrentHashMap<>()),
                registryUrlKey,
                s -> new ExporterChangeableWrapper<>((ReferenceCountExporter<T>) exporter, originInvoker));
    }


    public <T> void reExport(Exporter<T> exporter, URL newInvokerUrl) {
        if (exporter instanceof ExporterChangeableWrapper) {
            ExporterChangeableWrapper<T> exporterWrapper = (ExporterChangeableWrapper<T>) exporter;
            Invoker<T> originInvoker = exporterWrapper.getOriginInvoker();
            reExport(originInvoker, newInvokerUrl);
        }
    }

    /**
     * Reexport the invoker of the modified url
     *
     * @param originInvoker
     * @param newInvokerUrl
     * @param <T>
     */
    @SuppressWarnings("unchecked")
    public <T> void reExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String providerUrlKey = getProviderUrlKey(originInvoker);
        String registryUrlKey = getRegistryUrlKey(originInvoker);
        Map<String, ExporterChangeableWrapper<?>> registryMap = bounds.get(providerUrlKey);
        if (registryMap == null) {
            logger.warn(
                    INTERNAL_ERROR,
                    "error state, exporterMap can not be null",
                    "",
                    "error state, exporterMap can not be null",
                    new IllegalStateException("error state, exporterMap can not be null"));
            return;
        }
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) registryMap.get(registryUrlKey);
        if (exporter == null) {
            logger.warn(
                    INTERNAL_ERROR,
                    "error state, exporterMap can not be null",
                    "",
                    "error state, exporterMap can not be null",
                    new IllegalStateException("error state, exporterMap can not be null"));
            return;
        }
        URL registeredUrl = exporter.getRegisterUrl();

        URL registryUrl = getRegistryUrl(originInvoker);
        URL newProviderUrl = customizeURL(newInvokerUrl, registryUrl);

        // update local exporter
        Invoker<T> invokerDelegate = new InvokerDelegate<>(originInvoker, newInvokerUrl);
        exporter.setExporter(protocol.export(invokerDelegate));

        // update registry
        if (!newProviderUrl.equals(registeredUrl)) {
            try {
                doReExport(originInvoker, exporter, registryUrl, registeredUrl, newProviderUrl);
            } catch (Exception e) {
                ReExportTask oldTask = reExportFailedTasks.get(registeredUrl);
                if (oldTask != null) {
                    return;
                }
                ReExportTask task = new ReExportTask(
                        () -> doReExport(originInvoker, exporter, registryUrl, registeredUrl, newProviderUrl),
                        registeredUrl,
                        null);
                oldTask = reExportFailedTasks.putIfAbsent(registeredUrl, task);
                if (oldTask == null) {
                    // never has a retry task. then start a new task for retry.
                    retryTimer.newTimeout(
                            task,
                            registryUrl.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD),
                            TimeUnit.MILLISECONDS);
                }
            }
        }
    }

        /**
     * 执行服务的重新导出操作，注销旧的服务URL并注册新的服务URL。
     * <p>
     * 该方法用于处理服务配置变更时的动态更新场景，例如服务地址变化、参数调整等。
     * 主要执行以下操作：
     * <ol>
     *   <li>检查导出器是否已注册，如果未注册则跳过注册中心的注销和注册操作</li>
     *   <li>获取Registry对象，调用reExportUnregister()注销旧的服务URL</li>
     *   <li>调用reExportRegister()注册新的服务URL，实现服务信息的更新</li>
     *   <li>更新ProviderModel中的StatedURL，将提供者URL替换为新的URL</li>
     *   <li>更新导出器中保存的注册URL，保持状态一致性</li>
     * </ol>
     * </p>
     * <p>
     * 异常处理策略：所有异常都会被包装为SkipFailbackWrapperException抛出，
     * 由上层的失败重试机制进行处理，确保重新导出操作的可靠性。
     * </p>
     *
     * @param originInvoker 原始的Invoker对象，用于获取注册中心信息
     * @param exporter 可变更的导出器包装器，用于管理和更新导出状态
     * @param registryUrl 注册中心的URL地址
     * @param oldProviderUrl 需要被注销的旧服务URL
     * @param newProviderUrl 需要被注册的新服务URL
     * @throws SkipFailbackWrapperException 当获取注册中心或更新状态失败时抛出
     */
    private <T> void doReExport(
            final Invoker<T> originInvoker,
            ExporterChangeableWrapper<T> exporter,
            URL registryUrl,
            URL oldProviderUrl,
            URL newProviderUrl) {
        if (exporter.isRegistered()) {
            Registry registry;
            try {
                registry = getRegistry(getRegistryUrl(originInvoker));
            } catch (Exception e) {
                throw new SkipFailbackWrapperException(e);
            }

            logger.info("Try to unregister old url: " + oldProviderUrl);
            /*
             * 注销旧的服务URL，从注册中心移除过期的服务信息
             */
            registry.reExportUnregister(oldProviderUrl);

            logger.info("Try to register new url: " + newProviderUrl);
            /*
             * 注册新的服务URL，向注册中心发布最新的服务信息
             */
            registry.reExportRegister(newProviderUrl);
        }
        try {
            /*
             * 更新内存中的服务状态，确保ProviderModel和导出器持有最新的URL配置
             */
            ProviderModel.RegisterStatedURL statedUrl = getStatedUrl(registryUrl, newProviderUrl);
            statedUrl.setProviderUrl(newProviderUrl);
            exporter.setRegisterUrl(newProviderUrl);
        } catch (Exception e) {
            throw new SkipFailbackWrapperException(e);
        }
    }


    private ProviderModel.RegisterStatedURL getStatedUrl(URL registryUrl, URL providerUrl) {
        ProviderModel providerModel =
                frameworkModel.getServiceRepository().lookupExportedService(providerUrl.getServiceKey());

        List<ProviderModel.RegisterStatedURL> statedUrls = providerModel.getStatedUrl();
        return statedUrls.stream()
                .filter(u -> u.getRegistryUrl().equals(registryUrl)
                        && u.getProviderUrl().getProtocol().equals(providerUrl.getProtocol()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("There should have at least one registered url."));
    }

    /**
     * 根据注册中心地址获取对应的注册中心实例，支持通过SPI机制动态选择注册中心实现。
     * <p>
     * 该方法通过ScopeModelUtil获取RegistryFactory的自适应扩展（Adaptive Extension），
     * 根据URL中的协议类型（如zookeeper、nacos等）自动选择对应的注册中心工厂类，
     * 并创建或返回已缓存的注册中心实例。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>从registryUrl中提取ScopeModel，获取RegistryFactory的扩展加载器</li>
     *   <li>调用getAdaptiveExtension()获取自适应扩展实例，该实例会根据URL协议动态路由到具体的工厂实现</li>
     *   <li>调用factory.getRegistry(registryUrl)创建或返回已缓存的注册中心实例</li>
     * </ol>
     * </p>
     *
     * @param registryUrl 注册中心的URL地址，包含协议类型、主机地址、端口号等信息
     * @return 注册中心实例，用于执行服务的注册、订阅等操作
     */
    protected Registry getRegistry(final URL registryUrl) {
        /*
         * 获取RegistryFactory的自适应扩展，根据URL协议动态选择注册中心工厂实现
         */
        RegistryFactory registryFactory = ScopeModelUtil.getExtensionLoader(
                        RegistryFactory.class, registryUrl.getScopeModel())
                .getAdaptiveExtension();
        return registryFactory.getRegistry(registryUrl);
    }

    protected URL getRegistryUrl(Invoker<?> originInvoker) {
        return originInvoker.getUrl();
    }

    protected URL getRegistryUrl(URL url) {
        if (SERVICE_REGISTRY_PROTOCOL.equals(url.getProtocol())) {
            return url;
        }
        return url.addParameter(REGISTRY_KEY, url.getProtocol()).setProtocol(SERVICE_REGISTRY_PROTOCOL);
    }

    /**
     * Return the url that is registered to the registry and filter the url parameter once
     *
     * @param providerUrl provider service url
     * @param registryUrl registry center url
     * @return url to registry.
     */
    private URL customizeURL(final URL providerUrl, final URL registryUrl) {
        URL newProviderURL = providerUrl.putAttribute(SIMPLIFIED_KEY, registryUrl.getParameter(SIMPLIFIED_KEY, false));
        newProviderURL = newProviderURL.putAttribute(EXTRA_KEYS_KEY, registryUrl.getParameter(EXTRA_KEYS_KEY, ""));
        ApplicationModel applicationModel = providerUrl.getOrDefaultApplicationModel();
        ExtensionLoader<ServiceURLCustomizer> loader = applicationModel.getExtensionLoader(ServiceURLCustomizer.class);
        for (ServiceURLCustomizer customizer : loader.getSupportedExtensionInstances()) {
            newProviderURL = customizer.customize(newProviderURL, applicationModel);
        }
        return newProviderURL;
    }

    private URL getSubscribedOverrideUrl(URL registeredProviderUrl) {
        return registeredProviderUrl
                .setProtocol(PROVIDER_PROTOCOL)
                .addParameters(CATEGORY_KEY, CONFIGURATORS_CATEGORY, CHECK_KEY, String.valueOf(false));
    }

    /**
     * Get the address of the providerUrl through the url of the invoker
     *
     * @param originInvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> originInvoker) {
        Object providerURL = originInvoker.getUrl().getAttribute(EXPORT_KEY);
        if (!(providerURL instanceof URL)) {
            throw new IllegalArgumentException("The registry export url is null! registry: "
                    + originInvoker.getUrl().getAddress());
        }
        return (URL) providerURL;
    }

    /**
     * Get the key cached in bounds by invoker
     *
     * @param originInvoker
     * @return
     */
    private String getProviderUrlKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        return providerUrl.removeParameters(DYNAMIC_KEY, ENABLED_KEY).toFullString();
    }

    private String getRegistryUrlKey(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        return registryUrl.removeParameters(DYNAMIC_KEY, ENABLED_KEY).toFullString();
    }

    /**
     * 从注册中心引用远程服务，创建具备集群容错能力的Invoker对象。
     * <p>
     * 该方法是Dubbo服务消费者通过注册中心获取提供者列表的核心入口，负责根据配置参数选择合适的Cluster策略，
     * 并创建对应的MigrationInvoker支持应用级和接口级服务发现的平滑迁移。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>URL转换</b>：调用getRegistryUrl将原始URL转换为registry://协议的注册中心地址</li>
     *   <li><b>特殊类型处理</b>：如果引用的是RegistryService接口本身，直接返回Registry的代理对象，无需集群逻辑</li>
     *   <li><b>分组配置解析</b>：从REFER_KEY属性中提取group参数，判断是否为多分组或通配符模式</li>
     *   <li><b>多分组处理</b>：当group包含多个值（逗号分隔）或使用通配符"*"时，使用MergeableCluster合并多个分组的结果</li>
     *   <li><b>普通场景</b>：对于单分组场景，使用配置的cluster策略（默认Failover）创建Invoker</li>
     *   <li><b>委托执行</b>：调用doRefer完成ConsumerURL构建、MigrationInvoker创建和监听器拦截</li>
     * </ol>
     * </p>
     *
     * @param type 服务接口类型，即消费者引用的服务接口Class对象
     * @param url  注册中心URL，包含注册地址、协议类型、分组配置、集群策略等所有引用所需的参数
     * @return 具备集群容错能力的Invoker对象，可能是ServiceDiscoveryMigrationInvoker或普通的ClusterInvoker
     * @throws RpcException RPC异常，当注册中心连接失败、服务不可用或配置错误时抛出
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        url = getRegistryUrl(url);
        Registry registry = getRegistry(url);
        /*
         * RegistryService特殊处理：
         * 当直接引用RegistryService时，无需集群和路由逻辑，直接返回Registry实例的代理
         */
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        /*
         * 提取引用参数：
         * REFER_KEY属性中存储了ReferenceConfig传递的所有消费端配置参数
         */
        // group="a,b" or group="*"
        Map<String, String> qs = (Map<String, String>) url.getAttribute(REFER_KEY);
        String group = qs.get(GROUP_KEY);
        /*
         * 多分组场景判断：
         * 当配置了group="a,b"（多个分组）或group="*"（所有分组）时，使用MergeableCluster合并结果
         */
        if (StringUtils.isNotEmpty(group)) {
            if ((COMMA_SPLIT_PATTERN.split(group)).length > 1 || "*".equals(group)) {
                return doRefer(
                        Cluster.getCluster(url.getScopeModel(), MERGEABLE_CLUSTER_NAME), registry, type, url, qs);
            }
        }

        /*
         * 单分组场景：
         * 使用配置的cluster策略（如failover、failsafe等），未配置时使用默认的FailoverCluster
         */
        Cluster cluster = Cluster.getCluster(url.getScopeModel(), qs.get(CLUSTER_KEY));
        return doRefer(cluster, registry, type, url, qs);
    }

    /**
     * 执行服务引用的核心逻辑，构建消费者URL并创建支持迁移的ClusterInvoker。
     * <p>
     * 该方法是RegistryProtocol.refer的实际执行者，负责将注册中心URL和引用参数转换为具备服务发现迁移能力的Invoker对象。
     * 通过ServiceDiscoveryMigrationInvoker实现应用级服务发现和接口级服务发现的无缝切换，支持Dubbo3的平滑升级。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>属性复制</b>：从注册中心URL复制所有属性到consumerAttribute，移除REFER_KEY避免循环引用</li>
     *   <li><b>协议确定</b>：优先使用parameters中的PROTOCOL_KEY，未配置时使用CONSUMER作为默认协议</li>
     *   <li><b>消费者URL构建</b>：创建consumer://协议的URL，携带消费端IP、接口名、配置参数等元数据信息</li>
     *   <li><b>属性关联</b>：将consumerUrl存储到url的CONSUMER_URL_KEY属性中，便于后续路由和负载均衡使用</li>
     *   <li><b>MigrationInvoker创建</b>：调用getMigrationInvoker生成ServiceDiscoveryMigrationInvoker，支持三种集群模式切换</li>
     *   <li><b>监听器拦截</b>：调用interceptInvoker触发RegistryProtocolListener，允许扩展点修改Invoker行为（如动态规则监听）</li>
     * </ol>
     * </p>
     *
     * @param cluster   集群策略实现，如FailoverCluster、ZoneAwareCluster、MergeableCluster等，决定多个提供者的调用方式
     * @param registry  注册中心实例，用于查询提供者列表和订阅配置变更
     * @param type      服务接口类型，即消费者引用的服务接口Class对象
     * @param url       注册中心URL，格式为registry://registry-address，包含注册中心连接信息和订阅参数
     * @param parameters 引用配置参数Map，包含group、version、cluster、timeout等所有消费端配置
     * @return 经过拦截器处理的ClusterInvoker对象，具备服务发现迁移和动态规则控制能力
     */
    protected <T> Invoker<T> doRefer(
            Cluster cluster, Registry registry, Class<T> type, URL url, Map<String, String> parameters) {
        Map<String, Object> consumerAttribute = new HashMap<>(url.getAttributes());
        /*
         * 移除REFER_KEY避免循环引用：
         * consumerUrl中不需要存储refer参数，因为已经展开为独立的URL参数
         */
        consumerAttribute.remove(REFER_KEY);
        String p = isEmpty(parameters.get(PROTOCOL_KEY)) ? CONSUMER : parameters.get(PROTOCOL_KEY);
        /*
         * 构建消费者URL：
         * consumer://协议用于标识消费端身份，在元数据中心和服务治理中使用
         * 格式：consumer://consumer-ip/interface?parameters
         */
        URL consumerUrl = new ServiceConfigURL(
                p,
                null,
                null,
                parameters.get(REGISTER_IP_KEY),
                0,
                getPath(parameters, type),
                parameters,
                consumerAttribute);
        /*
         * 关联消费者URL到注册中心URL：
         * 后续Directory在订阅和路由时会用到consumerUrl中的配置
         */
        url = url.putAttribute(CONSUMER_URL_KEY, consumerUrl);
        /*
         * 创建支持迁移的Invoker：
         * ServiceDiscoveryMigrationInvoker封装了应用级和接口级两种服务发现模式，可根据配置动态切换
         */
        ClusterInvoker<T> migrationInvoker = getMigrationInvoker(this, cluster, registry, type, url, consumerUrl);
        /*
         * 触发监听器拦截：
         * 允许RegistryProtocolListener（如MigrationRuleListener）修改Invoker的行为和状态
         */
        return interceptInvoker(migrationInvoker, url, consumerUrl);
    }

    private String getPath(Map<String, String> parameters, Class<?> type) {
        return !ProtocolUtils.isGeneric(parameters.get(GENERIC_KEY)) ? type.getName() : parameters.get(INTERFACE_KEY);
    }

    protected <T> ClusterInvoker<T> getMigrationInvoker(
            RegistryProtocol registryProtocol,
            Cluster cluster,
            Registry registry,
            Class<T> type,
            URL url,
            URL consumerUrl) {
        return new ServiceDiscoveryMigrationInvoker<>(registryProtocol, cluster, registry, type, url, consumerUrl);
    }

    /**
     * 通过RegistryProtocolListener监听器拦截Invoker，实现服务引用行为的动态控制。
     * <p>
     * 该方法是Dubbo服务治理扩展机制的核心入口，负责加载并触发所有注册的RegistryProtocolListener，
     * 允许在服務引用阶段动态修改MigrationInvoker的行为和状态。典型应用场景包括服务迁移规则控制、
     * 动态配置监听、流量路由策略调整等。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>查找监听器</b>：通过SPI机制查找所有激活的RegistryProtocolListener实现类</li>
     *   <li><b>空值优化</b>：如果没有找到任何监听器，直接返回原始Invoker，避免不必要的性能开销</li>
     *   <li><b>触发回调</b>：遍历所有监听器，依次调用onRefer方法，传入invoker、consumerUrl和registryUrl供监听器使用</li>
     *   <li><b>行为修改</b>：监听器可以在onRefer中修改invoker的状态（如设置强制应用级模式）、注册配置监听器等</li>
     *   <li><b>返回Invoker</b>：返回经过所有监听器处理后的最终Invoker对象</li>
     * </ol>
     * </p>
     * <p>
     * 典型监听器示例：
     * <ul>
     *   <li><b>MigrationRuleListener</b>：监听动态下发的服务迁移规则，根据规则切换应用级/接口级服务发现模式</li>
     *   <li><b>自定义监听器</b>：用户可通过SPI扩展实现自定义的服务引用拦截逻辑</li>
     * </ul>
     * </p>
     *
     * @param invoker     集群Invoker对象，通常是ServiceDiscoveryMigrationInvoker，支持多种服务发现模式切换
     * @param url         注册中心URL，格式为registry://registry-address，包含订阅参数和消费者配置
     * @param consumerUrl 消费者URL，格式为consumer://consumer-ip/interface，代表当前接口的消费端身份和配置
     * @return 经过监听器处理后的Invoker对象，可能被修改了行为状态或注册了额外的监听器
     */
    protected <T> Invoker<T> interceptInvoker(ClusterInvoker<T> invoker, URL url, URL consumerUrl) {
        List<RegistryProtocolListener> listeners = findRegistryProtocolListeners(url);
        /*
         * 快速返回优化：
         * 没有监听器时直接返回，避免空循环
         */
        if (CollectionUtils.isEmpty(listeners)) {
            return invoker;
        }

        /*
         * 触发监听器回调：
         * 每个监听器可以访问并修改invoker的状态，实现动态服务治理逻辑
         */
        for (RegistryProtocolListener listener : listeners) {
            listener.onRefer(this, invoker, consumerUrl, url);
        }
        return invoker;
    }

    public <T> ClusterInvoker<T> getServiceDiscoveryInvoker(
            Cluster cluster, Registry registry, Class<T> type, URL url) {
        DynamicDirectory<T> directory = new ServiceDiscoveryRegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    public <T> ClusterInvoker<T> getInvoker(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // FIXME, this method is currently not used, create the right registry before enable.
        DynamicDirectory<T> directory = new RegistryDirectory<>(type, url);
        return doCreateInvoker(directory, cluster, registry, type);
    }

    /**
     * 创建集群Invoker，完成Directory初始化、消费者注册和订阅的完整流程。
     * <p>
     * 该方法是Dubbo服务消费者通过注册中心引用的核心执行单元，负责将DynamicDirectory（动态目录）与Cluster（集群策略）结合，
     * 生成具备路由、负载均衡和故障转移能力的ClusterInvoker。支持接口级和服务发现两种Directory类型。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>设置依赖</b>：为Directory注入Registry实例和Protocol协议，用于后续的地址订阅和Invoker创建</li>
     *   <li><b>构建注册URL</b>：从ConsumerUrl提取参数，构造consumer://协议的urlToRegistry，携带消费端身份标识（IP、接口名、配置参数）</li>
     *   <li><b>消费者注册</b>：如果directory配置了shouldRegister=true，将消费者URL写入注册中心，使得提供者能够感知到谁在调用自己（用于服务治理和监控）</li>
     *   <li><b>构建路由链</b>：调用buildRouterChain加载所有激活的路由规则（如条件路由、标签路由、脚本路由等），用于后续请求过滤</li>
     *   <li><b>订阅提供者列表</b>：调用subscribe订阅providers/configurators/routers等节点，监听提供者地址变更和动态配置推送</li>
     *   <li><b>集群包装</b>：调用cluster.join将Directory包装为ClusterInvoker，实现多提供者的统一调用入口</li>
     * </ol>
     * </p>
     *
     * @param directory 动态目录对象，可以是RegistryDirectory（接口级）或ServiceDiscoveryRegistryDirectory（应用级），负责管理提供者地址列表
     * @param cluster   集群策略实现，如FailoverCluster、FailsafeCluster等，决定多个提供者的调用方式和容错逻辑
     * @param registry  注册中心实例，用于消费者注册和提供者地址订阅
     * @param type      服务接口类型，即消费者引用的服务接口Class对象
     * @return 经过集群包装的ClusterInvoker对象，内部持有Directory和Cluster引用，调用时会自动进行地址选择和负载均衡
     */
    protected <T> ClusterInvoker<T> doCreateInvoker(
            DynamicDirectory<T> directory, Cluster cluster, Registry registry, Class<T> type) {
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        /*
         * 提取消费者参数：
         * 从ConsumerUrl中复制所有参数，用于构建注册到注册中心的URL
         */
        // all attributes of REFER_KEY
        Map<String, String> parameters =
                new HashMap<>(directory.getConsumerUrl().getParameters());
        URL urlToRegistry = new ServiceConfigURL(
                parameters.get(PROTOCOL_KEY) == null ? CONSUMER : parameters.get(PROTOCOL_KEY),
                parameters.remove(REGISTER_IP_KEY),
                0,
                getPath(parameters, type),
                parameters);
        urlToRegistry = urlToRegistry.setScopeModel(directory.getConsumerUrl().getScopeModel());
        urlToRegistry = urlToRegistry.setServiceModel(directory.getConsumerUrl().getServiceModel());
        /*
         * 消费者注册：
         * 将consumer://URL写入注册中心，用于服务治理、监控统计和提供者感知
         */
        if (directory.isShouldRegister()) {
            directory.setRegisteredConsumerUrl(urlToRegistry);
            registry.register(directory.getRegisteredConsumerUrl());
        }
        /*
         * 构建路由链：
         * 加载所有激活的路由规则，用于后续请求的过滤和路由决策
         */
        directory.buildRouterChain(urlToRegistry);
        /*
         * 订阅提供者地址：
         * 监听注册中心的providers节点变化，动态更新Directory中的Invoker列表
         */
        directory.subscribe(toSubscribeUrl(urlToRegistry));

        return (ClusterInvoker<T>) cluster.join(directory, true);
    }

    public <T> void reRefer(ClusterInvoker<?> invoker, URL newSubscribeUrl) {
        if (!(invoker instanceof MigrationClusterInvoker)) {
            logger.error(
                    REGISTRY_UNSUPPORTED_CATEGORY,
                    "",
                    "",
                    "Only invoker type of MigrationClusterInvoker supports reRefer, current invoker is "
                            + invoker.getClass());
            return;
        }

        MigrationClusterInvoker<?> migrationClusterInvoker = (MigrationClusterInvoker<?>) invoker;
        migrationClusterInvoker.reRefer(newSubscribeUrl);
    }

    public static URL toSubscribeUrl(URL url) {
        return url.addParameter(CATEGORY_KEY, ALL_CATEGORIES);
    }

    protected List<RegistryProtocolListener> findRegistryProtocolListeners(URL url) {
        return ScopeModelUtil.getExtensionLoader(RegistryProtocolListener.class, url.getScopeModel())
                .getActivateExtension(url, REGISTRY_PROTOCOL_LISTENER_KEY);
    }

    @Override
    public void destroy() {
        // FIXME all application models in framework are removed at this moment
        for (ApplicationModel applicationModel : frameworkModel.getApplicationModels()) {
            for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
                List<RegistryProtocolListener> listeners = moduleModel
                        .getExtensionLoader(RegistryProtocolListener.class)
                        .getLoadedExtensionInstances();
                if (CollectionUtils.isNotEmpty(listeners)) {
                    for (RegistryProtocolListener listener : listeners) {
                        listener.onDestroy();
                    }
                }
            }
        }

        for (ApplicationModel applicationModel : frameworkModel.getApplicationModels()) {
            if (applicationModel
                    .modelEnvironment()
                    .getConfiguration()
                    .convert(Boolean.class, org.apache.dubbo.registry.Constants.ENABLE_CONFIGURATION_LISTEN, true)) {
                for (ModuleModel moduleModel : applicationModel.getPubModuleModels()) {
                    String applicationName = applicationModel.tryGetApplicationName();
                    if (applicationName == null) {
                        // already removed
                        continue;
                    }
                    if (!moduleModel
                            .getServiceRepository()
                            .getExportedServices()
                            .isEmpty()) {
                        moduleModel
                                .getExtensionLoader(GovernanceRuleRepository.class)
                                .getDefaultExtension()
                                .removeListener(
                                        applicationName + CONFIGURATORS_SUFFIX,
                                        getProviderConfigurationListener(moduleModel));
                    }
                }
            }
        }

        List<Exporter<?>> exporters =
                bounds.values().stream().flatMap(e -> e.values().stream()).collect(Collectors.toList());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    @Override
    public List<ProtocolServer> getServers() {
        return protocol.getServers();
    }

    // Merge the urls of configurators
    private static URL getConfiguredInvokerUrl(List<Configurator> configurators, URL url) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

    public static class InvokerDelegate<T> extends InvokerWrapper<T> {

        /**
         * @param invoker
         * @param url     invoker.getUrl return this value
         */
        public InvokerDelegate(Invoker<T> invoker, URL url) {
            super(invoker, url);
        }

        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegate) {
                return ((InvokerDelegate<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    private static class DestroyableExporter<T> implements Exporter<T> {

        private Exporter<T> exporter;

        public DestroyableExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            exporter.unexport();
        }

        @Override
        public void register() {
            exporter.register();
        }

        @Override
        public void unregister() {
            exporter.unregister();
        }
    }

    /**
     * Reexport: the exporter destroy problem in protocol
     * 1.Ensure that the exporter returned by registry protocol can be normal destroyed
     * 2.No need to re-register to the registry after notify
     * 3.The invoker passed by the export method , would better to be the invoker of exporter
     */
    private class OverrideListener implements NotifyListener {
        private final URL subscribeUrl;
        private final Invoker originInvoker;

        private List<Configurator> configurators;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * @param urls The list of registered information, is always not empty, The meaning is the same as the
         *             return value of {@link org.apache.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            if (logger.isDebugEnabled()) {
                logger.debug("original override urls: " + urls);
            }

            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            if (logger.isDebugEnabled()) {
                logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            }

            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            this.configurators = Configurator.toConfigurators(classifyUrls(matchedUrls, UrlUtils::isConfigurator))
                    .orElse(configurators);

            ApplicationDeployer deployer =
                    subscribeUrl.getOrDefaultApplicationModel().getDeployer();

            try {
                deployer.increaseServiceRefreshCount();
                doOverrideIfNecessary();
            } finally {
                deployer.decreaseServiceRefreshCount();
            }
        }

        public synchronized void doOverrideIfNecessary() {
            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegate) {
                invoker = ((InvokerDelegate<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            // The origin invoker
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String providerUrlKey = getProviderUrlKey(originInvoker);
            String registryUrlKey = getRegistryUrlKey(originInvoker);
            Map<String, ExporterChangeableWrapper<?>> exporterMap = bounds.get(providerUrlKey);
            if (exporterMap == null) {
                logger.warn(
                        INTERNAL_ERROR,
                        "error state, exporterMap can not be null",
                        "",
                        "error state, exporterMap can not be null",
                        new IllegalStateException("error state, exporterMap can not be null"));
                return;
            }
            ExporterChangeableWrapper<?> exporter = exporterMap.get(registryUrlKey);
            if (exporter == null) {
                logger.warn(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "error state, exporter should not be null",
                        new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            // The current, may have been merged many times
            Invoker<?> exporterInvoker = exporter.getInvoker();
            URL currentUrl = exporterInvoker == null ? null : exporterInvoker.getUrl();
            // Merged with this configuration
            URL newUrl = getConfiguredInvokerUrl(configurators, originUrl);
            newUrl = getConfiguredInvokerUrl(
                    getProviderConfigurationListener(originUrl).getConfigurators(), newUrl);
            newUrl = getConfiguredInvokerUrl(
                    serviceConfigurationListeners.get(originUrl.getServiceKey()).getConfigurators(), newUrl);
            if (!newUrl.equals(currentUrl)) {
                if (newUrl.getParameter(Constants.NEED_REEXPORT, true)) {
                    RegistryProtocol.this.reExport(originInvoker, newUrl);
                }
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: "
                        + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getCategory() == null && OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(CATEGORY_KEY, CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }
    }

    private ProviderConfigurationListener getProviderConfigurationListener(URL url) {
        return getProviderConfigurationListener(url.getOrDefaultModuleModel());
    }

    private ProviderConfigurationListener getProviderConfigurationListener(ModuleModel moduleModel) {
        return moduleModel
                .getBeanFactory()
                .getOrRegisterBean(
                        ProviderConfigurationListener.class, type -> new ProviderConfigurationListener(moduleModel));
    }

    private class ServiceConfigurationListener extends AbstractConfiguratorListener {
        private URL providerUrl;
        private OverrideListener notifyListener;

        private final ModuleModel moduleModel;

        public ServiceConfigurationListener(ModuleModel moduleModel, URL providerUrl, OverrideListener notifyListener) {
            super(moduleModel);
            this.providerUrl = providerUrl;
            this.notifyListener = notifyListener;
            this.moduleModel = moduleModel;
            if (moduleModel
                    .modelEnvironment()
                    .getConfiguration()
                    .convert(Boolean.class, ENABLE_CONFIGURATION_LISTEN, true)) {
                this.initWith(DynamicConfiguration.getRuleKey(providerUrl) + CONFIGURATORS_SUFFIX);
            }
        }

        private <T> URL overrideUrl(URL providerUrl) {
            return RegistryProtocol.getConfiguredInvokerUrl(configurators, providerUrl);
        }

        @Override
        protected void notifyOverrides() {
            ApplicationDeployer deployer =
                    this.moduleModel.getApplicationModel().getDeployer();
            try {
                deployer.increaseServiceRefreshCount();
                notifyListener.doOverrideIfNecessary();
            } finally {
                deployer.decreaseServiceRefreshCount();
            }
        }
    }

    private class ProviderConfigurationListener extends AbstractConfiguratorListener {

        private final ConcurrentHashMap<URL, Set<NotifyListener>> overrideListeners = new ConcurrentHashMap<>();

        private final ModuleModel moduleModel;

        public ProviderConfigurationListener(ModuleModel moduleModel) {
            super(moduleModel);
            this.moduleModel = moduleModel;
            if (moduleModel.modelEnvironment().getConfiguration().getBoolean(ENABLE_CONFIGURATION_LISTEN, true)) {
                this.initWith(moduleModel.getApplicationModel().getApplicationName() + CONFIGURATORS_SUFFIX);
            }
        }

        /**
         * Get existing configuration rule and override provider url before exporting.
         *
         * @param providerUrl
         * @param <T>
         * @return
         */
        private <T> URL overrideUrl(URL providerUrl) {
            return RegistryProtocol.getConfiguredInvokerUrl(configurators, providerUrl);
        }

        @Override
        protected void notifyOverrides() {
            ApplicationDeployer deployer =
                    this.moduleModel.getApplicationModel().getDeployer();
            try {
                deployer.increaseServiceRefreshCount();
                overrideListeners.values().forEach(listeners -> {
                    for (NotifyListener listener : listeners) {
                        ((OverrideListener) listener).doOverrideIfNecessary();
                    }
                });
            } finally {
                deployer.decreaseServiceRefreshCount();
            }
        }

        public ConcurrentHashMap<URL, Set<NotifyListener>> getOverrideListeners() {
            return overrideListeners;
        }
    }

    /**
     * exporter proxy, establish the corresponding relationship between the returned exporter and the exporter
     * exported by the protocol, and can modify the relationship at the time of override.
     *
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private final ScheduledExecutorService executor;

        private final Invoker<T> originInvoker;
        private Exporter<T> exporter;
        private URL subscribeUrl;
        private URL registerUrl;

        private NotifyListener notifyListener;
        private final AtomicBoolean registered = new AtomicBoolean(false);

        /**
         * 构造可变更的导出器包装器，初始化导出器、原始调用器和共享线程池。
         * <p>
         * 该构造方法在创建包装器时会执行以下操作：
         * <ol>
         *   <li>保存传入的ReferenceCountExporter和原始Invoker引用</li>
         *   <li>增加导出器的引用计数，确保资源不会被提前释放</li>
         *   <li>从FrameworkModel中获取FrameworkExecutorRepository，并从中获取共享的定时任务线程池</li>
         * </ol>
         * </p>
         * <p>
         * 该包装器主要用于支持服务导出的动态变更场景（如重新导出、取消导出等），
         * 通过引用计数管理导出器的生命周期，通过共享线程池执行异步任务。
         * </p>
         *
         * @param exporter 带引用计数的导出器对象，用于管理服务导出状态
         * @param originInvoker 原始的Invoker对象，包含服务接口、实现类引用、URL配置等信息
         */
        public ExporterChangeableWrapper(ReferenceCountExporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            /*
             * 增加导出器的引用计数，防止在其他地方关闭时影响当前包装器的使用
             */
            exporter.increaseCount();
            this.originInvoker = originInvoker;
            /*
             * 从框架模型中获取共享的定时任务线程池，用于执行异步导出任务
             */
            FrameworkExecutorRepository frameworkExecutorRepository = originInvoker
                    .getUrl()
                    .getOrDefaultFrameworkModel()
                    .getBeanFactory()
                    .getBean(FrameworkExecutorRepository.class);
            this.executor = frameworkExecutorRepository.getSharedScheduledExecutor();
        }


        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

                /**
         * 将服务注册到注册中心，并更新提供者模型中的注册状态。
         * <p>
         * 该方法通过CAS操作保证注册逻辑只执行一次，避免重复注册。主要执行以下操作：
         * <ol>
         *   <li>从原始Invoker中获取注册中心URL和Registry对象</li>
         *   <li>调用RegistryProtocol.register()方法将服务URL注册到注册中心</li>
         *   <li>从服务仓库中查找对应的ProviderModel，获取其关联的所有StatedURL</li>
         *   <li>过滤出匹配当前注册中心和协议的StatedURL，将其注册状态标记为true</li>
         *   <li>记录注册成功的日志，包含服务键、服务URL和注册中心地址</li>
         * </ol>
         * </p>
         * <p>
         * 该方法通常在服务导出成功后调用，确保服务信息被正确发布到注册中心，
         * 使得消费者可以发现并订阅该服务。
         * </p>
         */
        @Override
        public void register() {
            /*
             * 使用CAS操作保证注册逻辑只执行一次，避免重复注册
             */
            if (registered.compareAndSet(false, true)) {
                URL registryUrl = getRegistryUrl(originInvoker);
                Registry registry = getRegistry(registryUrl);
                RegistryProtocol.register(registry, getRegisterUrl());

                /*
                 * 更新提供者模型中的注册状态，标记对应的StatedURL为已注册
                 */
                ProviderModel providerModel = frameworkModel
                        .getServiceRepository()
                        .lookupExportedService(getRegisterUrl().getServiceKey());

                List<ProviderModel.RegisterStatedURL> statedUrls = providerModel.getStatedUrl();
                statedUrls.stream()
                        .filter(u -> u.getRegistryUrl().equals(registryUrl)
                                && u.getProviderUrl()
                                        .getProtocol()
                                        .equals(getRegisterUrl().getProtocol()))
                        .forEach(u -> u.setRegistered(true));
                logger.info("[INSTANCE_REGISTER] Registered dubbo service "
                        + getRegisterUrl().getServiceKey() + " url " + getRegisterUrl() + " to registry "
                        + registryUrl);
            }
        }


        @Override
        public synchronized void unregister() {
            if (registered.compareAndSet(true, false)) {
                URL registryUrl = getRegistryUrl(originInvoker);
                Registry registry = RegistryProtocol.this.getRegistry(registryUrl);

                ProviderModel providerModel = frameworkModel
                        .getServiceRepository()
                        .lookupExportedService(getRegisterUrl().getServiceKey());

                List<ProviderModel.RegisterStatedURL> statedURLs = providerModel.getStatedUrl().stream()
                        .filter(u -> u.getRegistryUrl().equals(registryUrl)
                                && u.getProviderUrl()
                                        .getProtocol()
                                        .equals(getRegisterUrl().getProtocol()))
                        .collect(Collectors.toList());
                if (statedURLs.isEmpty()
                        || statedURLs.stream().anyMatch(ProviderModel.RegisterStatedURL::isRegistered)) {
                    try {
                        registry.unregister(registerUrl);
                    } catch (Throwable t) {
                        logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", t.getMessage(), t);
                    }
                }

                try {
                    if (subscribeUrl != null) {
                        Map<URL, Set<NotifyListener>> overrideListeners =
                                getProviderConfigurationListener(subscribeUrl).getOverrideListeners();
                        Set<NotifyListener> listeners = overrideListeners.get(subscribeUrl);
                        if (listeners != null) {
                            if (listeners.remove(notifyListener)) {
                                ApplicationModel applicationModel = getApplicationModel(registerUrl.getScopeModel());
                                if (applicationModel
                                        .modelEnvironment()
                                        .getConfiguration()
                                        .convert(Boolean.class, ENABLE_26X_CONFIGURATION_LISTEN, true)) {
                                    if (!registry.isServiceDiscovery()) {
                                        registry.unsubscribe(subscribeUrl, notifyListener);
                                    }
                                }
                                if (applicationModel
                                        .modelEnvironment()
                                        .getConfiguration()
                                        .convert(Boolean.class, ENABLE_CONFIGURATION_LISTEN, true)) {
                                    for (ModuleModel moduleModel : applicationModel.getPubModuleModels()) {
                                        if (null != moduleModel.getServiceRepository()
                                                && !moduleModel
                                                        .getServiceRepository()
                                                        .getExportedServices()
                                                        .isEmpty()) {
                                            moduleModel
                                                    .getExtensionLoader(GovernanceRuleRepository.class)
                                                    .getDefaultExtension()
                                                    .removeListener(
                                                            subscribeUrl.getServiceKey() + CONFIGURATORS_SUFFIX,
                                                            serviceConfigurationListeners.remove(
                                                                    subscribeUrl.getServiceKey()));
                                        }
                                    }
                                }
                            }
                            if (listeners.isEmpty()) {
                                overrideListeners.remove(subscribeUrl);
                            }
                        }
                    }
                } catch (Throwable t) {
                    logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", t.getMessage(), t);
                }
            }
        }

        @Override
        public synchronized void unexport() {
            String providerUrlKey = getProviderUrlKey(this.originInvoker);
            String registryUrlKey = getRegistryUrlKey(this.originInvoker);
            Map<String, ExporterChangeableWrapper<?>> exporterMap = bounds.remove(providerUrlKey);
            if (exporterMap != null) {
                exporterMap.remove(registryUrlKey);
            }

            unregister();
            doUnExport();
        }

        public void setRegistered(boolean registered) {
            this.registered.set(registered);
        }

        public boolean isRegistered() {
            return registered.get();
        }

        private void doUnExport() {
            try {
                exporter.unexport();
            } catch (Throwable t) {
                logger.warn(INTERNAL_ERROR, "unknown error in registry module", "", t.getMessage(), t);
            }
        }

        public void setSubscribeUrl(URL subscribeUrl) {
            this.subscribeUrl = subscribeUrl;
        }

        public void setRegisterUrl(URL registerUrl) {
            this.registerUrl = registerUrl;
        }

        public void setNotifyListener(NotifyListener notifyListener) {
            this.notifyListener = notifyListener;
        }

        public URL getRegisterUrl() {
            return registerUrl;
        }
    }
}
