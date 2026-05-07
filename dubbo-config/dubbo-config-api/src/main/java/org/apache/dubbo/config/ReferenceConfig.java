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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.cluster.support.registry.ZoneAwareCluster;
import org.apache.dubbo.rpc.model.AsyncMethodInfo;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.DubboStub;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ModuleServiceRepository;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.stub.StubSuppliers;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_DOMAIN;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR_CHAR;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_CLUSTER_DOMAIN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_MESH_PORT;
import static org.apache.dubbo.common.constants.CommonConstants.DubboProperty.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.MESH_ENABLE;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.POD_NAMESPACE;
import static org.apache.dubbo.common.constants.CommonConstants.PROXY_CLASS_REF;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SEMICOLON_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SVC;
import static org.apache.dubbo.common.constants.CommonConstants.TRIPLE;
import static org.apache.dubbo.common.constants.CommonConstants.UNLOAD_CLUSTER_RELATED;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_NO_VALID_PROVIDER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_DESTROY_INVOKER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_LOAD_ENV_VARIABLE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_NO_METHOD_FOUND;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_PROPERTY_CONFLICT;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDED_BY;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.StringUtils.splitToSet;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PEER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * Please avoid using this class for any new application,
 * use {@link ReferenceConfigBase} instead.
 */
public class ReferenceConfig<T> extends ReferenceConfigBase<T> {

    public static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ReferenceConfig.class);

    /**
     * The {@link Protocol} implementation with adaptive functionality,it will be different in different scenarios.
     * A particular {@link Protocol} implementation is determined by the protocol attribute in the {@link URL}.
     * For example:
     *
     * <li>when the url is registry://224.5.6.7:1234/org.apache.dubbo.registry.RegistryService?application=dubbo-sample,
     * then the protocol is <b>RegistryProtocol</b></li>
     *
     * <li>when the url is dubbo://224.5.6.7:1234/org.apache.dubbo.config.api.DemoService?application=dubbo-sample, then
     * the protocol is <b>DubboProtocol</b></li>
     * <p>
     * Actually，when the {@link ExtensionLoader} init the {@link Protocol} instants,it will automatically wrap three
     * layers, and eventually will get a <b>ProtocolSerializationWrapper</b> or <b>ProtocolFilterWrapper</b> or <b>ProtocolListenerWrapper</b>
     */
    private Protocol protocolSPI;

    /**
     * A {@link ProxyFactory} implementation that will generate a reference service's proxy,the JavassistProxyFactory is
     * its default implementation
     */
    private ProxyFactory proxyFactory;

    private ConsumerModel consumerModel;

    /**
     * The interface proxy reference
     */
    private transient volatile T ref;

    /**
     * The invoker of the reference service
     */
    private transient volatile Invoker<?> invoker;

    /**
     * The flag whether the ReferenceConfig has been initialized
     */
    private transient volatile boolean initialized;

    /**
     * whether this ReferenceConfig has been destroyed
     */
    private transient volatile boolean destroyed;

    /**
     * The service names that the Dubbo interface subscribed.
     *
     * @since 2.7.8
     */
    private String services;

    protected final transient ReentrantLock lock = new ReentrantLock();

    public ReferenceConfig() {
        super();
    }

    public ReferenceConfig(ModuleModel moduleModel) {
        super(moduleModel);
    }

    public ReferenceConfig(Reference reference) {
        super(reference);
    }

    public ReferenceConfig(ModuleModel moduleModel, Reference reference) {
        super(moduleModel, reference);
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);

        protocolSPI = this.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        proxyFactory = this.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    }

    /**
     * Get a string presenting the service names that the Dubbo interface subscribed.
     * If it is a multiple-values, the content will be a comma-delimited String.
     *
     * @return non-null
     * @see RegistryConstants#SUBSCRIBED_SERVICE_NAMES_KEY
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(key = SUBSCRIBED_SERVICE_NAMES_KEY)
    public String getServices() {
        return services;
    }

    /**
     * It's an alias method for {@link #getServices()}, but the more convenient.
     *
     * @return the String {@link List} presenting the Dubbo interface subscribed
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(excluded = true)
    public Set<String> getSubscribedServices() {
        return splitToSet(getServices(), COMMA_SEPARATOR_CHAR);
    }

    /**
     * Set the service names that the Dubbo interface subscribed.
     *
     * @param services If it is a multiple-values, the content will be a comma-delimited String.
     * @since 2.7.8
     */
    public void setServices(String services) {
        this.services = services;
    }

    /**
     * 获取远程服务的本地代理对象，延迟初始化Invoker并建立网络连接。
     * <p>
     * 该方法是Dubbo服务消费者的核心入口，负责在首次调用时触发ReferenceConfig的初始化流程，
     * 创建远程服务的Invoker代理对象，使得调用方可以像调用本地方法一样进行RPC调用。
     * 支持幂等性保证，多次调用返回同一个代理实例（单例模式）。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>状态校验</b>：检查ReferenceConfig是否已被销毁，已销毁则抛出异常避免使用无效对象</li>
     *   <li><b>懒加载判断</b>：如果ref已存在直接返回，实现延迟初始化和单例缓存</li>
     *   <li><b>模块启动</b>：根据生命周期管理模式决定是prepare（外部管理）还是start（内部管理）模块部署器</li>
     *   <li><b>初始化执行</b>：调用init(check)方法完成注册中心连接、提供者发现、Invoker创建和代理生成</li>
     *   <li><b>返回代理</b>：返回生成的本地代理对象ref，类型为服务接口</li>
     * </ol>
     * </p>
     *
     * @param check 是否进行严格检查，true时在连接失败或提供者不可用时抛出异常，false时仅记录警告日志
     * @return 远程服务的本地代理对象，类型为服务接口T
     * @throws IllegalStateException 当ReferenceConfig已被销毁时尝试获取代理会抛出此异常
     */
    @Override
    @Transient
    public T get(boolean check) {
        if (destroyed) {
            throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
        }

        /*
         * 延迟初始化：
         * 仅在首次调用get时触发init流程，后续调用直接返回缓存的代理对象
         */
        if (ref == null) {
            if (getScopeModel().isLifeCycleManagedExternally()) {
                /*
                 * 外部生命周期管理模式：
                 * 当模块的生命周期由外部框架（如Spring）管理时，仅执行prepare准备必要资源
                 */
                // prepare model for reference
                getScopeModel().getDeployer().prepare();
            } else {
                /*
                 * 内部生命周期管理模式：
                 * 当模块由Dubbo框架自己管理时，启动完整的模块部署流程
                 */
                // ensure start module, compatible with old api usage
                getScopeModel().getDeployer().start();
            }

            init(check);
        }

        return ref;
    }

    @Override
    public void checkOrDestroy(long timeout) {
        if (!initialized || ref == null) {
            return;
        }
        try {
            checkInvokerAvailable(timeout);
        } catch (Throwable t) {
            logAndCleanup(t);
            throw t;
        }
    }

    private void logAndCleanup(Throwable t) {
        try {
            if (invoker != null) {
                invoker.destroy();
            }
        } catch (Throwable destroy) {
            logger.warn(
                    CONFIG_FAILED_DESTROY_INVOKER,
                    "",
                    "",
                    "Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").",
                    t);
        }
        if (consumerModel != null) {
            ModuleServiceRepository repository = getScopeModel().getServiceRepository();
            repository.unregisterConsumer(consumerModel);
        }
        initialized = false;
        invoker = null;
        ref = null;
        consumerModel = null;
        serviceMetadata.setTarget(null);
        serviceMetadata.getAttributeMap().remove(PROXY_CLASS_REF);

        // Thrown by checkInvokerAvailable().
        if (t.getClass() == IllegalStateException.class
                && t.getMessage().contains("No provider available for the service")) {

            // 2-2 - No provider available.
            logger.error(CLUSTER_NO_VALID_PROVIDER, "server crashed", "", "No provider available.", t);
        }
    }

    @Override
    public void destroy() {
        lock.lock();
        try {
            super.destroy();
            if (destroyed) {
                return;
            }
            destroyed = true;
            try {
                if (invoker != null) {
                    invoker.destroy();
                }
            } catch (Throwable t) {
                logger.warn(
                        CONFIG_FAILED_DESTROY_INVOKER,
                        "",
                        "",
                        "Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").",
                        t);
            }
            invoker = null;
            ref = null;
            if (consumerModel != null) {
                ModuleServiceRepository repository = getScopeModel().getServiceRepository();
                repository.unregisterConsumer(consumerModel);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void init() {
        init(true);
    }

    /**
     * 初始化ReferenceConfig，创建远程服务的Invoker代理并建立网络连接。
     * <p>
     * 该方法是Dubbo服务消费者初始化的核心逻辑，负责完成从配置解析到代理生成的完整流程。
     * 包括配置刷新、元数据初始化、ConsumerModel创建、Invoker组装和可用性检查等步骤。
     * 通过ReentrantLock保证线程安全，支持并发调用场景下的幂等性。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>幂等性检查</b>：如果已初始化且ref不为空则直接返回，避免重复初始化</li>
     *   <li><b>配置刷新</b>：调用refresh解析注解、XML配置和系统属性，合并到当前对象</li>
     *   <li><b>代理类型检测</b>：对于DubboStub接口自动切换到NATIVE_STUB（Triple协议）模式</li>
     *   <li><b>元数据初始化</b>：设置serviceMetadata的服务类型和服务键（格式：group/interface:version）</li>
     *   <li><b>参数组装</b>：通过appendConfig收集ApplicationConfig、ConsumerConfig、ReferenceConfig等所有配置参数</li>
     *   <li><b>服务描述符注册</b>：将接口信息注册到ModuleServiceRepository，区分Native Stub和普通代理两种模式</li>
     *   <li><b>ConsumerModel创建</b>：构建消费者模型，包含服务键、代理工厂、异步方法配置、类加载器等上下文信息</li>
     *   <li><b>代理创建</b>：调用createProxy完成注册中心查询、Invoker路由链构建和本地代理生成</li>
     *   <li><b>元数据完善</b>：将生成的代理对象设置到serviceMetadata和consumerModel中</li>
     *   <li><b>可用性检查</b>：当check=true时，验证提供者是否可用，失败则抛出异常</li>
     *   <li><b>异常清理</b>：发生异常时调用logAndCleanup销毁已创建的资源，避免内存泄漏</li>
     * </ol>
     * </p>
     *
     * @param check 是否进行严格检查，true时在无可用提供者或连接失败时抛出异常，false时仅记录警告日志继续执行
     * @throws IllegalStateException 当配置错误、注册中心连接失败或无可用提供者时可能抛出
     */
    protected void init(boolean check) {
        lock.lock();
        try {
            if (initialized && ref != null) {
                return;
            }
            try {
                if (!this.isRefreshed()) {
                    this.refresh();
                }
                /*
                 * 自动检测代理类型：
                 * 对于实现DubboStub接口的服务，自动使用NATIVE_STUB（Triple协议原生存根）模式
                 */
                // auto detect proxy type
                String proxyType = getProxy();
                if (StringUtils.isBlank(proxyType) && DubboStub.class.isAssignableFrom(interfaceClass)) {
                    setProxy(CommonConstants.NATIVE_STUB);
                }

                // init serviceMetadata
                initServiceMetadata(consumer);

                serviceMetadata.setServiceType(getServiceInterfaceClass());
                // TODO, uncomment this line once service key is unified
                serviceMetadata.generateServiceKey();
                //
                Map<String, String> referenceParameters = appendConfig();

                ModuleServiceRepository repository = getScopeModel().getServiceRepository();
                ServiceDescriptor serviceDescriptor;
                if (CommonConstants.NATIVE_STUB.equals(getProxy())) {
                    /*
                     * Native Stub模式：
                     * 使用Triple协议的原生存根，需要从StubSuppliers获取服务描述符
                     */
                    serviceDescriptor = StubSuppliers.getServiceDescriptor(interfaceName);
                    repository.registerService(serviceDescriptor);
                    setInterface(serviceDescriptor.getInterfaceName());
                } else {
                    /*
                     * 普通代理模式：
                     * 使用JDK动态代理或Javassist，直接注册接口类的服务描述符
                     */
                    serviceDescriptor = repository.registerService(interfaceClass);
                }
                /*
                 * 创建消费者模型：
                 * ConsumerModel封装了消费者的所有运行时信息，包括异步方法配置、类加载器、服务元数据等
                 */
                consumerModel = new ConsumerModel(
                        serviceMetadata.getServiceKey(),
                        proxy,
                        serviceDescriptor,
                        getScopeModel(),
                        serviceMetadata,
                        createAsyncMethodInfo(),
                        interfaceClassLoader);

                // Compatible with dependencies on ServiceModel#getReferenceConfig() , and will be removed in a future
                // version.
                consumerModel.setConfig(this);

                repository.registerConsumer(consumerModel);

                serviceMetadata.getAttachments().putAll(referenceParameters);

                /*
                 * 创建代理对象：
                 * 根据referenceParameters构建Invoker链，生成最终的本地代理ref
                 */
                ref = createProxy(referenceParameters);

                serviceMetadata.setTarget(ref);
                serviceMetadata.addAttribute(PROXY_CLASS_REF, ref);

                consumerModel.setDestroyRunner(getDestroyRunner());
                consumerModel.setProxyObject(ref);
                consumerModel.initMethodModels();

                /*
                 * 可用性检查：
                 * 当check=true时，验证是否有可用的提供者，失败则抛出异常阻止启动
                 */
                if (check) {
                    checkInvokerAvailable(0);
                }
            } catch (Throwable t) {
                logAndCleanup(t);

                throw t;
            }
            initialized = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * convert and aggregate async method info
     *
     * @return Map<String, AsyncMethodInfo>
     */
    private Map<String, AsyncMethodInfo> createAsyncMethodInfo() {
        Map<String, AsyncMethodInfo> attributes = null;
        if (CollectionUtils.isNotEmpty(getMethods())) {
            attributes = new HashMap<>(16);
            for (MethodConfig methodConfig : getMethods()) {
                AsyncMethodInfo asyncMethodInfo = methodConfig.convertMethodConfig2AsyncInfo();
                if (asyncMethodInfo != null) {
                    attributes.put(methodConfig.getName(), asyncMethodInfo);
                }
            }
        }

        return attributes;
    }

    /**
     * 组装服务引用所需的所有配置参数，生成用于创建Invoker的参数映射表。
     * <p>
     * 该方法按照优先级顺序将各层级配置合并到一个Map中，包括接口信息、运行时参数、版本信息、方法列表以及
     * 来自Application、Module、Consumer和ReferenceConfig本身的配置项。同时处理特殊的系统属性（如注册IP）
     * 和方法级别的配置转换（如retry到retries的映射）。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>放入基础参数：接口名称（interface）和端类型（side=consumer）</li>
     *   <li>调用appendRuntimeParameters()添加PID、timestamp、pid等运行时参数</li>
     *   <li>如果不是泛化调用：
     *     <ul>
     *       <li>计算接口的版本号（revision）并放入map</li>
     *       <li>提取接口中的所有方法名，如果为空则使用"*"通配符，否则用逗号分隔所有方法名</li>
     *     </ul>
     *   </li>
     *   <li>依次合并Application、Module、Consumer和ReferenceConfig的配置参数到map中</li>
     *   <li>确定注册IP地址：优先使用DUBBO_IP_TO_REGISTRY系统属性，如果未设置则使用本地主机地址，
     *       如果设置了非法的本地地址则抛出异常</li>
     *   <li>遍历MethodConfig配置，将方法级参数添加到map中（前缀为方法名），并将retry=false转换为retries=0</li>
     * </ol>
     * </p>
     *
     * @return 包含所有服务引用配置参数的Map对象，key为参数名，value为参数值
     */
    private Map<String, String> appendConfig() {
        Map<String, String> map = new HashMap<>(16);

        map.put(INTERFACE_KEY, interfaceName);
        map.put(SIDE_KEY, CONSUMER_SIDE);

        ReferenceConfigBase.appendRuntimeParameters(map);

        if (!ProtocolUtils.isGeneric(generic)) {
            /*
             * 非泛化调用场景：添加版本号和方法列表信息
             */
            String revision = Version.getVersion(interfaceClass, version);
            if (StringUtils.isNotEmpty(revision)) {
                map.put(REVISION_KEY, revision);
            }

            String[] methods = methods(interfaceClass);
            if (methods.length == 0) {
                logger.warn(
                        CONFIG_NO_METHOD_FOUND,
                        "",
                        "",
                        "No method found in service interface: " + interfaceClass.getName());
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                map.put(METHODS_KEY, StringUtils.join(new TreeSet<>(Arrays.asList(methods)), COMMA_SEPARATOR));
            }
        }

        /*
         * 按层级合并配置参数：Application -> Module -> Consumer -> ReferenceConfig
         */
        AbstractConfig.appendParameters(map, getApplication());
        AbstractConfig.appendParameters(map, getModule());
        AbstractConfig.appendParameters(map, consumer);
        AbstractConfig.appendParameters(map, this);

        /*
         * 确定注册IP地址：优先使用系统属性，其次使用本地主机地址，并进行合法性校验
         */
        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY
                    + ", value:" + hostToRegistry);
        }

        map.put(REGISTER_IP_KEY, hostToRegistry);

        /*
         * 处理方法级别的配置，将MethodConfig转换为URL参数格式
         */
        if (CollectionUtils.isNotEmpty(getMethods())) {
            for (MethodConfig methodConfig : getMethods()) {
                AbstractConfig.appendParameters(map, methodConfig, methodConfig.getName());
                /*
                 * 将retry=false转换为retries=0，兼容不同的配置方式
                 */
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
            }
        }

        return map;
    }

    /**
     * 创建远程服务的本地代理对象，完成URL解析、Invoker组装和代理生成。
     * <p>
     * 该方法是Dubbo服务消费者代理创建的核心逻辑，负责将配置参数转换为可调用的Invoker链，
     * 最终通过ProxyFactory生成本地代理对象。支持直连模式、注册中心模式和Mesh模式等多种部署场景。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>URL收集</b>：清空urls列表，根据meshMode配置决定是否启用服务网格模式</li>
     *   <li><b>URL解析</b>：如果配置了url属性（直连地址或注册中心地址），调用parseUrl解析多个分号分隔的URL</li>
     *   <li><b>注册中心查询</b>：如果未配置url，从注册中心拉取提供者地址列表，可能包含多个注册中心的URL</li>
     *   <li><b>Invoker创建</b>：调用createInvoker根据urls构建Invoker链（包括Cluster、Router、Filter等组件）</li>
     *   <li><b>日志记录</b>：记录服务引用成功的信息，标注是否为GenericService泛化调用</li>
     *   <li><b>消费者URL构建</b>：创建consumer://协议的URL，携带所有消费端配置参数用于元数据上报</li>
     *   <li><b>元数据发布</b>：通过MetadataUtils将消费者定义发布到元数据中心，供服务治理使用</li>
     *   <li><b>代理生成</b>：调用proxyFactory.getProxy根据invoker生成本地代理对象（JDK动态代理或Javassist）</li>
     * </ol>
     * </p>
     *
     * @param referenceParameters 引用配置参数Map，包含接口名、版本、分组、超时时间、重试次数等所有RPC调用所需的配置
     * @return 远程服务的本地代理对象，类型为服务接口T
     */
    @SuppressWarnings({"unchecked"})
    private T createProxy(Map<String, String> referenceParameters) {
        urls.clear();

        /*
         * Mesh模式处理：
         * 如果启用了服务网格模式，根据K8S环境变量和providedBy配置生成网格地址
         */
        meshModeHandleUrl(referenceParameters);

        /*
         * URL收集策略：
         * 优先使用用户配置的直连URL，否则从注册中心查询提供者地址
         */
        if (StringUtils.isNotEmpty(url)) {
            // user specified URL, could be peer-to-peer address, or register center's address.
            parseUrl(referenceParameters);
        } else {
            // if protocols not in jvm checkRegistry
            aggregateUrlFromRegistry(referenceParameters);
        }
        /*
         * 创建Invoker链：
         * 根据urls列表构建Cluster Invoker，包含负载均衡、路由规则、容错策略等
         */
        createInvoker();

        if (logger.isInfoEnabled()) {
            logger.info("Referred dubbo service: [" + referenceParameters.get(INTERFACE_KEY) + "]."
                    + (ProtocolUtils.isGeneric(referenceParameters.get(GENERIC_KEY))
                            ? " it's GenericService reference"
                            : " it's not GenericService reference"));
        }

        /*
         * 构建消费者URL并上报元数据：
         * consumer://协议URL用于标识消费端身份，携带IP、接口名、配置参数等信息
         */
        URL consumerUrl = new ServiceConfigURL(
                CONSUMER_PROTOCOL,
                referenceParameters.get(REGISTER_IP_KEY),
                0,
                referenceParameters.get(INTERFACE_KEY),
                referenceParameters);
        consumerUrl = consumerUrl.setScopeModel(getScopeModel());
        consumerUrl = consumerUrl.setServiceModel(consumerModel);
        MetadataUtils.publishServiceDefinition(consumerUrl, consumerModel.getServiceModel(), getApplicationModel());

        /*
         * 生成最终代理对象：
         * 通过ProxyFactory将Invoker包装为服务接口的代理，支持泛化调用和普通调用两种模式
         */
        // create service proxy
        return (T) proxyFactory.getProxy(invoker, ProtocolUtils.isGeneric(generic));
    }

    /**
     * if enable mesh mode, handle url.
     *
     * @param referenceParameters referenceParameters
     */
    private void meshModeHandleUrl(Map<String, String> referenceParameters) {
        if (!checkMeshConfig(referenceParameters)) {
            return;
        }
        if (StringUtils.isNotEmpty(url)) {
            // user specified URL, could be peer-to-peer address, or register center's address.
            if (logger.isInfoEnabled()) {
                logger.info("The url already exists, mesh no longer processes url: " + url);
            }
            return;
        }

        // get provider namespace if (@DubboReference, <reference provider-namespace="xx"/>) present
        String podNamespace = referenceParameters.get(RegistryConstants.PROVIDER_NAMESPACE);

        // get pod namespace from env if annotation not present the provider namespace
        if (StringUtils.isEmpty(podNamespace)) {
            if (StringUtils.isEmpty(System.getenv(POD_NAMESPACE))) {
                if (logger.isWarnEnabled()) {
                    logger.warn(
                            CONFIG_FAILED_LOAD_ENV_VARIABLE,
                            "",
                            "",
                            "Can not get env variable: POD_NAMESPACE, it may not be running in the K8S environment , "
                                    + "finally use 'default' replace.");
                }
                podNamespace = "default";
            } else {
                podNamespace = System.getenv(POD_NAMESPACE);
            }
        }

        // In mesh mode, providedBy equals K8S Service name.
        String providedBy = referenceParameters.get(PROVIDED_BY);
        // cluster_domain default is 'cluster.local',generally unchanged.
        String clusterDomain =
                Optional.ofNullable(System.getenv(CLUSTER_DOMAIN)).orElse(DEFAULT_CLUSTER_DOMAIN);
        // By VirtualService and DestinationRule, envoy will generate a new route rule,such as
        // 'demo.default.svc.cluster.local:80',the default port is 80.
        Integer meshPort = Optional.ofNullable(getProviderPort()).orElse(DEFAULT_MESH_PORT);
        // DubboReference default is -1, process it.
        meshPort = meshPort > -1 ? meshPort : DEFAULT_MESH_PORT;
        // get mesh url.
        url = TRIPLE + "://" + providedBy + "." + podNamespace + SVC + clusterDomain + ":" + meshPort;
    }

    /**
     * check if mesh config is correct
     *
     * @param referenceParameters referenceParameters
     * @return mesh config is correct
     */
    private boolean checkMeshConfig(Map<String, String> referenceParameters) {
        if (!"true".equals(referenceParameters.getOrDefault(MESH_ENABLE, "false"))) {
            // In mesh mode, unloadClusterRelated can only be false.
            referenceParameters.put(UNLOAD_CLUSTER_RELATED, "false");
            return false;
        }

        getScopeModel()
                .getConfigManager()
                .getProtocol(TRIPLE)
                .orElseThrow(() -> new IllegalStateException("In mesh mode, a triple protocol must be specified"));

        String providedBy = referenceParameters.get(PROVIDED_BY);
        if (StringUtils.isEmpty(providedBy)) {
            throw new IllegalStateException("In mesh mode, the providedBy of ReferenceConfig is must be set");
        }

        return true;
    }

    /**
     * Parse the directly configured url.
     */
    private void parseUrl(Map<String, String> referenceParameters) {
        String[] us = SEMICOLON_SPLIT_PATTERN.split(url);
        if (ArrayUtils.isNotEmpty(us)) {
            for (String u : us) {
                URL url = URL.valueOf(u);
                if (StringUtils.isEmpty(url.getPath())) {
                    url = url.setPath(interfaceName);
                }
                url = url.setScopeModel(getScopeModel());
                url = url.setServiceModel(consumerModel);
                if (UrlUtils.isRegistry(url)) {
                    urls.add(url.putAttribute(REFER_KEY, referenceParameters));
                } else {
                    URL peerUrl = getScopeModel()
                            .getApplicationModel()
                            .getBeanFactory()
                            .getBean(ClusterUtils.class)
                            .mergeUrl(url, referenceParameters);
                    peerUrl = peerUrl.putAttribute(PEER_KEY, true);
                    urls.add(peerUrl);
                }
            }
        }
    }

    /**
     * 从注册中心加载URL列表并进行聚合处理，构建最终的服务引用地址集合。
     * <p>
     * 该方法负责将注册中心地址与服务引用参数结合，生成用于发起远程调用的完整URL列表。
     * 支持多注册中心场景，会为每个注册中心生成一个带有引用参数的URL。
     * 如果未配置注册中心或注册中心列表为空，且满足JVM内部调用条件，则使用injvm协议进行本地调用。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>检查注册中心配置的合法性</li>
     *   <li>加载所有注册的URL列表（consumer端地址）</li>
     *   <li>遍历每个注册中心URL：
     *     <ul>
     *       <li>加载监控中心配置并作为属性附加到URL上</li>
     *       <li>设置ScopeModel和ServiceModel，用于后续的模型隔离和服务治理</li>
     *       <li>如果配置了injvm优先，则添加local.protocol=true参数</li>
     *       <li>将referenceParameters作为REFER_KEY属性存储到URL中，供后续创建Invoker时使用</li>
     *     </ul>
     *   </li>
     *   <li>如果URL列表仍为空且满足JVM引用条件（如服务仅在本地发布），创建injvm协议的本地调用URL</li>
     *   <li>如果最终URL列表仍为空，抛出IllegalStateException提示用户配置注册中心</li>
     * </ol>
     * </p>
     *
     * @param referenceParameters 服务引用的配置参数映射表，包含接口名、版本、分组、超时时间等信息
     * @throws IllegalStateException 当没有任何可用的注册中心且无法使用JVM内部调用时抛出
     */
    private void aggregateUrlFromRegistry(Map<String, String> referenceParameters) {
        checkRegistry();
        List<URL> us = ConfigValidationUtils.loadRegistries(this, false);
        if (CollectionUtils.isNotEmpty(us)) {
            for (URL u : us) {
                /*
                 * 为每个注册中心URL附加监控中心、作用域模型、服务模型和引用参数等元数据
                 */
                URL monitorUrl = ConfigValidationUtils.loadMonitor(this, u);
                if (monitorUrl != null) {
                    u = u.putAttribute(MONITOR_KEY, monitorUrl);
                }
                u = u.setScopeModel(getScopeModel());
                u = u.setServiceModel(consumerModel);
                if (isInjvm() != null && isInjvm()) {
                    u = u.addParameter(LOCAL_PROTOCOL, true);
                }
                urls.add(u.putAttribute(REFER_KEY, referenceParameters));
            }
        }
        /*
         * 如果没有注册中心URL且满足JVM内部引用条件，则创建本地调用URL
         */
        if (urls.isEmpty() && shouldJvmRefer(referenceParameters)) {
            URL injvmUrl = new URL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName())
                    .addParameters(referenceParameters);
            injvmUrl = injvmUrl.setScopeModel(getScopeModel());
            injvmUrl = injvmUrl.setServiceModel(consumerModel);
            urls.add(injvmUrl.putAttribute(REFER_KEY, referenceParameters));
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("No such any registry to reference " + interfaceName + " on the consumer "
                    + NetUtils.getLocalHost() + " use dubbo version "
                    + Version.getVersion()
                    + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
        }
    }

    /**
     * 创建引用服务的Invoker对象，根据URL数量和类型选择合适的集群策略。
     * <p>
     * 该方法是Dubbo服务消费者Invoker组装的核心逻辑，负责将底层的Protocol层Invoker包装为具备集群容错能力的Cluster Invoker。
     * 支持单注册中心、多注册中心、直连提供者等多种部署场景，并根据场景自动选择对应的Cluster实现（如Failover、ZoneAware等）。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>单URL场景</b>：
     *     <ul>
     *       <li>调用protocolSPI.refer创建基础Invoker（可能是RegistryProtocol、DubboProtocol等）</li>
     *       <li>如果不是注册中心URL且未卸载集群功能，使用DEFAULT Cluster（默认为Failover）包装为StaticDirectory</li>
     *       <li>Mesh模式下可能跳过Cluster包装，直接返回单个Invoker</li>
     *     </ul>
     *   </li>
     *   <li><b>多URL场景</b>：
     *     <ul>
     *       <li>遍历所有URL，为每个URL创建对应的refer Invoker，不检查可用性（允许后期恢复）</li>
     *       <li>如果包含注册中心URL，使用最后一个registryUrl作为目录URL，采用ZoneAwareCluster支持多区域容错</li>
     *       <li>如果不包含注册中心URL（纯直连场景），使用第一个URL的cluster配置进行包装</li>
     *     </ul>
     *   </li>
     *   <li><b>Invoker包装层次</b>：
     *     <ul>
     *       <li>注册中心模式：ZoneAwareClusterInvoker(StaticDirectory) -> FailoverClusterInvoker(RegistryDirectory) -> Invoker</li>
     *       <li>直连模式：FailoverClusterInvoker(StaticDirectory) -> Invoker</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createInvoker() {
        if (urls.size() == 1) {
            URL curUrl = urls.get(0);
            /*
             * 单URL场景：
             * 直接通过Protocol SPI创建Invoker，可能是RegistryProtocol（注册中心）或DubboProtocol（直连）
             */
            invoker = protocolSPI.refer(interfaceClass, curUrl);
            /*
             * 集群包装判断：
             * 非注册中心URL且未启用集群卸载时，使用默认Cluster（Failover）包装
             * Mesh模式下可能设置unloadClusterRelated=true，跳过集群包装以提升性能
             */
            if (!UrlUtils.isRegistry(curUrl) && !curUrl.getParameter(UNLOAD_CLUSTER_RELATED, false)) {
                List<Invoker<?>> invokers = new ArrayList<>();
                invokers.add(invoker);
                invoker = Cluster.getCluster(getScopeModel(), Cluster.DEFAULT)
                        .join(new StaticDirectory(curUrl, invokers), true);
            }
        } else {
            /*
             * 多URL场景：
             * 适用于多注册中心或多直连地址的复杂部署架构
             */
            List<Invoker<?>> invokers = new ArrayList<>();
            URL registryUrl = null;
            for (URL url : urls) {
                /*
                 * 为每个URL创建Invoker：
                 * 不检查当前是否可用，因为后续可能通过重连恢复
                 */
                // For multi-registry scenarios, it is not checked whether each referInvoker is available.
                // Because this invoker may become available later.
                invokers.add(protocolSPI.refer(interfaceClass, url));

                if (UrlUtils.isRegistry(url)) {
                    // use last registry url
                    registryUrl = url;
                }
            }

            if (registryUrl != null) {
                /*
                 * 多注册中心场景：
                 * 使用ZoneAwareCluster实现跨区域的智能路由和故障转移
                 * 包装层次：ZoneAwareClusterInvoker -> StaticDirectory(多个RegistryDirectory) -> Invoker
                 */
                // registry url is available
                // for multi-subscription scenario, use 'zone-aware' policy by default
                String cluster = registryUrl.getParameter(CLUSTER_KEY, ZoneAwareCluster.NAME);
                // The invoker wrap sequence would be: ZoneAwareClusterInvoker(StaticDirectory) ->
                // FailoverClusterInvoker
                // (RegistryDirectory, routing happens here) -> Invoker
                invoker = Cluster.getCluster(registryUrl.getScopeModel(), cluster, false)
                        .join(new StaticDirectory(registryUrl, invokers), false);
            } else {
                /*
                 * 多直连地址场景：
                 * 不使用注册中心，直接连接多个提供者实例，使用配置的Cluster策略进行负载均衡
                 */
                // not a registry url, must be direct invoke.
                if (CollectionUtils.isEmpty(invokers)) {
                    throw new IllegalArgumentException("invokers == null");
                }
                URL curUrl = invokers.get(0).getUrl();
                String cluster = curUrl.getParameter(CLUSTER_KEY, Cluster.DEFAULT);
                invoker =
                        Cluster.getCluster(getScopeModel(), cluster).join(new StaticDirectory(curUrl, invokers), true);
            }
        }
    }


    private void checkInvokerAvailable(long timeout) throws IllegalStateException {
        if (!shouldCheck()) {
            return;
        }
        boolean available = invoker.isAvailable();
        if (available) {
            return;
        }

        long startTime = System.currentTimeMillis();
        long checkDeadline = startTime + timeout;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            available = invoker.isAvailable();
        } while (!available && checkDeadline > System.currentTimeMillis());
        logger.warn(
                LoggerCodeConstants.REGISTRY_EMPTY_ADDRESS,
                "",
                "",
                "Check reference of [" + getUniqueServiceName() + "] failed very beginning. " + "After "
                        + (System.currentTimeMillis() - startTime) + "ms reties, finally "
                        + (available ? "succeed" : "failed")
                        + ".");
        if (!available) {
            // 2-2 - No provider available.

            IllegalStateException illegalStateException =
                    new IllegalStateException("Failed to check the status of the service "
                            + interfaceName
                            + ". No provider available for the service "
                            + (group == null ? "" : group + "/")
                            + interfaceName + (version == null ? "" : ":" + version)
                            + " from the url "
                            + invoker.getUrl()
                            + " to the consumer "
                            + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());

            logger.error(
                    CLUSTER_NO_VALID_PROVIDER,
                    "provider not started",
                    "",
                    "No provider available.",
                    illegalStateException);

            throw illegalStateException;
        }
    }

        /**
     * 检查并更新引用配置的子配置项，确保所有相关配置模块已正确初始化并处理属性覆盖。
     * <p>
     * 该方法必须在ReferenceConfig实例创建后立即调用，在使用其他配置模块的任何属性之前完成所有配置的初始化和校验。
     * 主要执行以下操作：
     * <ol>
     *   <li>验证interfaceName不能为空，否则抛出IllegalStateException</li>
     *   <li>调用completeCompoundConfigs()补全组合配置，从全局配置中继承缺失的属性</li>
     *   <li>加载并执行ConfigInitializer扩展，对引用配置进行额外的初始化处理</li>
     *   <li>处理泛化调用配置：
     *     <ul>
     *       <li>如果generic未设置但consumer存在，则从consumer继承generic属性</li>
     *       <li>如果是泛化调用（generic为true或"nativejava"等），将interfaceClass设置为GenericService.class，
     *           并记录与原有interfaceClass冲突的警告日志</li>
     *       <li>如果不是泛化调用，则根据interfaceName加载实际的接口类，优先使用getInterfaceClassLoader()，
     *           否则使用线程上下文类加载器</li>
     *     </ul>
     *   </li>
     *   <li>检查Stub和Local配置的有效性</li>
     *   <li>如果url未配置，则调用checkRegistry()检查注册中心配置</li>
     *   <li>调用resolveFile()解析本地配置文件的直连地址</li>
     *   <li>调用ConfigValidationUtils.validateReferenceConfig()进行完整的配置校验</li>
     *   <li>调用postProcessConfig()执行配置后处理逻辑</li>
     * </ol>
     * </p>
     */
    protected void checkAndUpdateSubConfigs() {
        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }

        // get consumer's global configuration
        /*
         * 补全组合配置，从全局消费者配置中继承缺失的属性
         */
        completeCompoundConfigs();

        // init some null configuration.
        /*
         * 加载并执行ConfigInitializer扩展，对引用配置进行额外初始化
         */
        List<ConfigInitializer> configInitializers = this.getExtensionLoader(ConfigInitializer.class)
                .getActivateExtension(URL.valueOf("configInitializer://"), (String[]) null);
        configInitializers.forEach(e -> e.initReferConfig(this));

        if (getGeneric() == null && getConsumer() != null) {
            setGeneric(getConsumer().getGeneric());
        }
        if (ProtocolUtils.isGeneric(generic)) {
            /*
             * 泛化调用场景：将interfaceClass设置为GenericService，并记录类型冲突警告
             */
            if (interfaceClass != null && !interfaceClass.equals(GenericService.class)) {
                logger.warn(
                        CONFIG_PROPERTY_CONFLICT,
                        "",
                        "",
                        String.format(
                                "Found conflicting attributes for interface type: [interfaceClass=%s] and [generic=%s], "
                                        + "because the 'generic' attribute has higher priority than 'interfaceClass', so change 'interfaceClass' to '%s'. "
                                        + "Note: it will make this reference bean as a candidate bean of type '%s' instead of '%s' when resolving dependency in Spring.",
                                interfaceClass.getName(),
                                generic,
                                GenericService.class.getName(),
                                GenericService.class.getName(),
                                interfaceClass.getName()));
            }
            interfaceClass = GenericService.class;
        } else {
            /*
             * 非泛化调用场景：加载实际的接口类
             */
            try {
                if (getInterfaceClassLoader() != null
                        && (interfaceClass == null || interfaceClass.getClassLoader() != getInterfaceClassLoader())) {
                    interfaceClass = Class.forName(interfaceName, true, getInterfaceClassLoader());
                } else if (interfaceClass == null) {
                    interfaceClass = Class.forName(
                            interfaceName, true, Thread.currentThread().getContextClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        checkStubAndLocal(interfaceClass);

        if (StringUtils.isEmpty(url)) {
            checkRegistry();
        }

        resolveFile();
        ConfigValidationUtils.validateReferenceConfig(this);
        postProcessConfig();
    }

    @Override
    protected void postProcessRefresh() {
        super.postProcessRefresh();
        checkAndUpdateSubConfigs();
    }

    protected void completeCompoundConfigs() {
        super.completeCompoundConfigs(consumer);
        if (consumer != null) {
            if (StringUtils.isEmpty(registryIds)) {
                setRegistryIds(consumer.getRegistryIds());
            }
        }
    }

    /**
     * Figure out should refer the service in the same JVM from configurations. The default behavior is true
     * 1. if injvm is specified, then use it
     * 2. then if a url is specified, then assume it's a remote call
     * 3. otherwise, check scope parameter
     * 4. if scope is not specified but the target service is provided in the same JVM, then prefer to make the local
     * call, which is the default behavior
     */
    protected boolean shouldJvmRefer(Map<String, String> map) {
        boolean isJvmRefer;
        if (isInjvm() == null) {
            // if an url is specified, don't do local reference
            if (StringUtils.isNotEmpty(url)) {
                isJvmRefer = false;
            } else {
                // by default, reference local service if there is
                URL tmpUrl = new ServiceConfigURL("temp", "localhost", 0, map);
                isJvmRefer = InjvmProtocol.getInjvmProtocol(getScopeModel()).isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = isInjvm();
        }
        return isJvmRefer;
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = this.getExtensionLoader(ConfigPostProcessor.class)
                .getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);

        HashSet<ConfigPostProcessor> allConfigPostProcessor = new HashSet<>();

        // merge common and old config
        allConfigPostProcessor.addAll(configPostProcessors);
        allConfigPostProcessor.addAll(configPostProcessors);

        allConfigPostProcessor.forEach(component -> component.postProcessReferConfig(this));
    }

    /**
     * Return if ReferenceConfig has been initialized
     * Note: Cannot use `isInitialized` as it may be treated as a Java Bean property
     *
     * @return initialized
     */
    @Transient
    public boolean configInitialized() {
        return initialized;
    }

    /**
     * just for test
     *
     * @return
     */
    @Deprecated
    @Transient
    public Invoker<?> getInvoker() {
        return invoker;
    }

    @Transient
    public Runnable getDestroyRunner() {
        return this::destroy;
    }
}
