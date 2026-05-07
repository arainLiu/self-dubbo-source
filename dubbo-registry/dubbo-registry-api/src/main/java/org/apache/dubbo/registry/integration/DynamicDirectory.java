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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.AddressListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.migration.InvokersChangedListener;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.SingleRouterChain;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_SITE_SELECTION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_DESTROY_SERVICE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_DESTROY_UNREGISTER_URL;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.registry.integration.InterfaceCompatibleRegistryProtocol.DEFAULT_REGISTER_CONSUMER_KEYS;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;

/**
 * DynamicDirectory
 */
public abstract class DynamicDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(DynamicDirectory.class);

    protected final Cluster cluster;

    protected final RouterFactory routerFactory;

    /**
     * Initialization at construction time, assertion not null
     */
    protected final String serviceKey;

    /**
     * Initialization at construction time, assertion not null
     */
    protected final Class<T> serviceType;

    /**
     * Initialization at construction time, assertion not null, and always assign non-null value
     */
    protected volatile URL directoryUrl;

    protected final boolean multiGroup;

    /**
     * Initialization at the time of injection, the assertion is not null
     */
    protected Protocol protocol;

    /**
     * Initialization at the time of injection, the assertion is not null
     */
    protected Registry registry;

    protected volatile boolean forbidden = false;
    protected boolean shouldRegister;
    protected boolean shouldSimplified;

    /**
     * Initialization at construction time, assertion not null, and always assign not null value
     */
    protected volatile URL subscribeUrl;

    protected volatile URL registeredConsumerUrl;

    /**
     * The initial value is null and the midway may be assigned to null, please use the local variable reference
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    protected volatile List<Configurator> configurators;

    protected ServiceInstancesChangedListener serviceListener;

    /**
     * Should continue route if directory is empty
     */
    private final boolean shouldFailFast;

    private volatile InvokersChangedListener invokersChangedListener;
    private volatile boolean invokersChanged;

    public DynamicDirectory(Class<T> serviceType, URL url) {
        super(url, true);

        ModuleModel moduleModel = url.getOrDefaultModuleModel();

        this.cluster = moduleModel.getExtensionLoader(Cluster.class).getAdaptiveExtension();
        this.routerFactory = moduleModel.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }

        if (StringUtils.isEmpty(url.getServiceKey())) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }

        this.shouldRegister = !ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(REGISTER_KEY, true);
        this.shouldSimplified = url.getParameter(SIMPLIFIED_KEY, false);

        this.serviceType = serviceType;
        this.serviceKey = super.getConsumerUrl().getServiceKey();

        this.directoryUrl = consumerUrl;
        String group = directoryUrl.getGroup("");
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));

        this.shouldFailFast = Boolean.parseBoolean(
                ConfigurationUtils.getProperty(moduleModel, Constants.SHOULD_FAIL_FAST_KEY, "true"));
    }

    @Override
    public void addServiceListener(ServiceInstancesChangedListener instanceListener) {
        this.serviceListener = instanceListener;
    }

    @Override
    public ServiceInstancesChangedListener getServiceListener() {
        return this.serviceListener;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public Registry getRegistry() {
        return registry;
    }

    public boolean isShouldRegister() {
        return shouldRegister;
    }

        /**
     * 向注册中心订阅服务目录，注册当前目录对象作为通知监听者。
     * <p>
     * 该方法建立与注册中心的订阅关系，使得当服务提供者列表发生变化时（如新增、下线、配置变更等），
     * 注册中心能够主动通知当前目录对象，触发服务列表的刷新和路由链的重建。
     * </p>
     *
     * @param url 订阅URL，包含服务接口、版本、分组、协议等消费者配置信息
     */
    public void subscribe(URL url) {
        setSubscribeUrl(url);
        registry.subscribe(url, this);
    }

    public void unSubscribe(URL url) {
        setSubscribeUrl(null);
        registry.unsubscribe(url, this);
    }

    /**
     * 执行服务提供者列表的路由选择，根据路由规则过滤出可用的Invoker集合。
     * <p>
     * 该方法是集群容错的核心入口点，负责在每次RPC调用时动态筛选合适的服务提供者。
     * 主要执行以下操作：
     * <ol>
     *   <li>检查服务是否被禁用（forbidden=true）且配置了快速失败（shouldFailFast=true），如果是则抛出RpcException，
     *       提示用户检查服务提供者的状态（未注册、被禁用或在黑名单中）</li>
     *   <li>如果是多分组场景（multiGroup=true），直接返回所有Invoker，跳过路由逻辑，支持跨分组调用</li>
     *   <li>调用singleRouterChain.route()执行运行时路由规则，基于消费者URL、全量Invoker列表和调用参数进行过滤</li>
     *   <li>如果路由结果为null，返回空列表；否则返回路由后的Invoker列表</li>
     *   <li>如果路由过程发生异常，记录错误日志并返回空列表，避免单个路由规则失败影响整体调用</li>
     * </ol>
     * </p>
     * <p>
     * 路由链机制：singleRouterChain包含多个运行时路由器（如条件路由、标签路由、权重路由等），
     * 这些路由器会按顺序执行，逐步缩小候选Invoker的范围，最终返回符合条件的服务提供者列表。
     * </p>
     *
     * @param singleRouterChain 单次路由链对象，包含所有需要执行的路由规则
     * @param invokers 完整的Invoker列表（BitList结构），作为路由的输入数据
     * @param invocation RPC调用上下文，包含方法名、参数、附件等信息，供路由规则使用
     * @return 经过路由过滤后的Invoker列表，如果没有可用的Invoker则返回空列表
     * @throws RpcException 当服务被禁用且配置了快速失败时抛出FORBIDDEN_EXCEPTION
     */
    @Override
    public List<Invoker<T>> doList(
            SingleRouterChain<T> singleRouterChain, BitList<Invoker<T>> invokers, Invocation invocation) {
        if (forbidden && shouldFailFast) {
            // 1. No service provider 2. Service providers are disabled
            /*
             * 服务被禁用场景：抛出快速失败异常，提示用户检查服务状态
             */
            throw new RpcException(
                    RpcException.FORBIDDEN_EXCEPTION,
                    "No provider available from registry " + this
                            + " for service " + getConsumerUrl().getServiceKey() + " on consumer "
                            + NetUtils.getLocalHost()
                            + " use dubbo version " + Version.getVersion()
                            + ", please check status of providers(disabled, not registered or in blocklist).");
        }

        /*
         * 多分组场景：跳过路由逻辑，直接返回所有Invoker
         */
        if (multiGroup) {
            return this.getInvokers();
        }

        try {
            // Get invokers from cache, only runtime routers will be executed.
            /*
             * 执行路由链，基于运行时规则过滤Invoker列表
             */
            List<Invoker<T>> result = singleRouterChain.route(getConsumerUrl(), invokers, invocation);
            return result == null ? BitList.emptyList() : result;
        } catch (Throwable t) {
            // 2-1 - Failed to execute routing.
            logger.error(
                    CLUSTER_FAILED_SITE_SELECTION,
                    "",
                    "",
                    "Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(),
                    t);

            return BitList.emptyList();
        }
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public List<Invoker<T>> getAllInvokers() {
        return this.getInvokers();
    }

    /**
     * The currently effective consumer url
     *
     * @return URL
     */
    @Override
    public URL getConsumerUrl() {
        return this.directoryUrl;
    }

    /**
     * The original consumer url
     *
     * @return URL
     */
    public URL getOriginalConsumerUrl() {
        return this.consumerUrl;
    }

    /**
     * The url registered to registry or metadata center
     *
     * @return URL
     */
    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    /**
     * The url used to subscribe from registry
     *
     * @return URL
     */
    public URL getSubscribeUrl() {
        return subscribeUrl;
    }

    public void setSubscribeUrl(URL subscribeUrl) {
        this.subscribeUrl = subscribeUrl;
    }

    public void setRegisteredConsumerUrl(URL url) {
        if (!shouldSimplified) {
            this.registeredConsumerUrl =
                    url.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        } else {
            this.registeredConsumerUrl = URL.valueOf(url, DEFAULT_REGISTER_CONSUMER_KEYS, null)
                    .addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        }
    }

        /**
     * 基于指定的URL构建路由链，初始化所有激活的路由规则。
     * <p>
     * 该方法调用RouterChain.buildChain()静态方法，根据服务接口类型和URL中的配置参数（如条件路由、标签路由等），
     * 加载并组装所有激活的路由器实例，形成完整的路由链。路由链会在每次RPC调用时执行，用于动态筛选合适的服务提供者。
     * </p>
     * <p>
     * 典型使用场景：
     * <ul>
     *   <li>服务引用初始化时，根据消费者URL构建初始路由链</li>
     *   <li>配置变更时（如新增路由规则），重新构建路由链以应用最新的路由策略</li>
     * </ul>
     * </p>
     *
     * @param url 包含路由配置的URL对象，通常为消费者URL或订阅URL
     */
    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(getInterface(), url));
    }


    @Override
    public boolean isAvailable() {
        if (isDestroyed() || this.forbidden) {
            return false;
        }
        for (Invoker<T> validInvoker : getValidInvokers()) {
            if (validInvoker.isAvailable()) {
                return true;
            } else {
                addInvalidateInvoker(validInvoker);
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            // 1-8: Failed to unregister / unsubscribe url on destroy.
            logger.warn(
                    REGISTRY_FAILED_DESTROY_UNREGISTER_URL,
                    "",
                    "",
                    "unexpected error when unregister service " + serviceKey + " from registry: " + registry.getUrl(),
                    t);
        }

        // unsubscribe.
        try {
            if (getSubscribeUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getSubscribeUrl(), this);
            }
        } catch (Throwable t) {
            // 1-8: Failed to unregister / unsubscribe url on destroy.
            logger.warn(
                    REGISTRY_FAILED_DESTROY_UNREGISTER_URL,
                    "",
                    "",
                    "unexpected error when unsubscribe service " + serviceKey + " from registry: " + registry.getUrl(),
                    t);
        }

        ExtensionLoader<AddressListener> addressListenerExtensionLoader =
                getUrl().getOrDefaultModuleModel().getExtensionLoader(AddressListener.class);
        List<AddressListener> supportedListeners =
                addressListenerExtensionLoader.getActivateExtension(getUrl(), (String[]) null);
        if (CollectionUtils.isNotEmpty(supportedListeners)) {
            for (AddressListener addressListener : supportedListeners) {
                addressListener.destroy(getConsumerUrl(), this);
            }
        }

        synchronized (this) {
            try {
                destroyAllInvokers();
            } catch (Throwable t) {
                // 1-15 - Failed to destroy service.
                logger.warn(REGISTRY_FAILED_DESTROY_SERVICE, "", "", "Failed to destroy service " + serviceKey, t);
            }
            routerChain.destroy();
            invokersChangedListener = null;
            serviceListener = null;

            super.destroy(); // must be executed after unsubscribing
        }
    }

    @Override
    public void discordAddresses() {
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            // 1-15 - Failed to destroy service.
            logger.warn(REGISTRY_FAILED_DESTROY_SERVICE, "", "", "Failed to destroy service " + serviceKey, t);
        }
    }

    public synchronized void setInvokersChangedListener(InvokersChangedListener listener) {
        this.invokersChangedListener = listener;
        if (invokersChangedListener != null && invokersChanged) {
            invokersChangedListener.onChange();
        }
    }

    protected synchronized void invokersChanged() {
        refreshInvoker();
        invokersChanged = true;
        if (invokersChangedListener != null) {
            invokersChangedListener.onChange();
            invokersChanged = false;
        }
    }

    @Override
    public boolean isNotificationReceived() {
        return invokersChanged;
    }

    protected abstract void destroyAllInvokers();

    protected abstract void refreshOverrideAndInvoker(List<URL> urls);
}
