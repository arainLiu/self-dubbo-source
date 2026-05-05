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
package org.apache.dubbo.config.deploy;

import org.apache.dubbo.common.config.ReferenceCache;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.constants.RegisterTypeEnum;
import org.apache.dubbo.common.deploy.AbstractDeployer;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.deploy.DeployListener;
import org.apache.dubbo.common.deploy.DeployState;
import org.apache.dubbo.common.deploy.ModuleDeployListener;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.context.ModuleConfigManager;
import org.apache.dubbo.config.utils.SimpleReferenceCache;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ModuleServiceRepository;
import org.apache.dubbo.rpc.model.ProviderModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_EXPORT_SERVICE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_REFERENCE_MODEL;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_REFER_SERVICE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_START_MODEL;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_WAIT_EXPORT_REFER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_UNABLE_DESTROY_MODEL;

/**
 * Export/refer services of module
 */
public class DefaultModuleDeployer extends AbstractDeployer<ModuleModel> implements ModuleDeployer {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(DefaultModuleDeployer.class);

    private final List<CompletableFuture<?>> asyncExportingFutures = new ArrayList<>();

    private final List<CompletableFuture<?>> asyncReferringFutures = new ArrayList<>();

    private final List<ServiceConfigBase<?>> exportedServices = new ArrayList<>();

    private final ModuleModel moduleModel;

    private final FrameworkExecutorRepository frameworkExecutorRepository;
    private final ExecutorRepository executorRepository;

    private final ModuleConfigManager configManager;

    private final SimpleReferenceCache referenceCache;

    private final ApplicationDeployer applicationDeployer;
    private CompletableFuture startFuture;
    private Boolean background;
    private Boolean exportAsync;
    private Boolean referAsync;

    private boolean registryInteracted;

    private CompletableFuture<?> exportFuture;
    private CompletableFuture<?> referFuture;

    public DefaultModuleDeployer(ModuleModel moduleModel) {
        super(moduleModel);
        this.moduleModel = moduleModel;
        configManager = moduleModel.getConfigManager();
        frameworkExecutorRepository = moduleModel
                .getApplicationModel()
                .getFrameworkModel()
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class);
        executorRepository = ExecutorRepository.getInstance(moduleModel.getApplicationModel());
        referenceCache = SimpleReferenceCache.newCache();
        applicationDeployer = DefaultApplicationDeployer.get(moduleModel);

        // load spi listener
        Set<ModuleDeployListener> listeners =
                moduleModel.getExtensionLoader(ModuleDeployListener.class).getSupportedExtensionInstances();
        for (ModuleDeployListener listener : listeners) {
            this.addDeployListener(listener);
        }
    }

    /**
     * 初始化ModuleDeployer
     * <p>
     * 该方法负责完成Dubbo模块部署器的完整初始化流程，采用双重检查锁定机制确保线程安全。
     * 主要执行以下初始化步骤：
     * 1. 检查初始化状态，避免重复初始化
     * 2. 执行初始化回调（onInitialize）
     * 3. 加载模块级别的配置信息
     * 4. 读取ModuleConfig配置，确定异步导出/引用、后台运行等行为
     * 5. 兼容旧版本的背景运行配置逻辑
     * <p>
     * 该方法使用同步锁保证并发场景下的初始化安全性，
     * 确保只执行一次完整的初始化流程。
     *
     * @throws IllegalStateException 当默认模块配置未正确初始化时抛出异常
     */
    @Override
    public void initialize() throws IllegalStateException {
        if (initialized) {
            return;
        }

        // 使用同步锁确保并发调用时初始化只执行一次
        synchronized (this) {
            if (initialized) {
                return;
            }

            // 执行初始化回调
            onInitialize();

            // 加载模块配置信息
            loadConfigs();

            // read ModuleConfig，读取模块配置并设置行为参数
            ModuleConfig moduleConfig = moduleModel
                    .getConfigManager()
                    .getModule()
                    .orElseThrow(() -> new IllegalStateException("Default module config is not initialized"));

            // 从ModuleConfig中读取是否异步导出服务和异步引用服务
            exportAsync = Boolean.TRUE.equals(moduleConfig.getExportAsync());
            referAsync = Boolean.TRUE.equals(moduleConfig.getReferAsync());

            // start in background，设置是否在后台运行
            background = moduleConfig.getBackground();
            if (background == null) {
                // compatible with old usages，兼容旧版本的背景运行配置
                background = isExportBackground() || isReferBackground();
            }

            initialized = true;
            if (logger.isInfoEnabled()) {
                logger.info(getIdentifier() + " has been initialized!");
            }
        }
    }

    @Override
    public Future start() throws IllegalStateException {
        // initialize，maybe deadlock applicationDeployer lock & moduleDeployer lock
        applicationDeployer.initialize();

        return startSync();
    }

    /**
     * 同步启动模块部署器，完成服务导出、引用和注册的完整流程。
     * <p>
     * 该方法是Dubbo模块启动的核心入口，负责协调模块级别的服务发布和订阅操作。
     * 支持同步和异步两种模式，通过CompletableFuture实现非阻塞等待。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>状态校验</b>：检查模块是否处于可启动状态，防止重复启动或在停止状态下启动</li>
     *   <li><b>初始化</b>：加载配置、解析参数、创建必要的资源（线程池、注册中心连接等）</li>
     *   <li><b>服务导出</b>：遍历所有ServiceConfig，将本地服务暴露为可远程调用的Exporter</li>
     *   <li><b>服务引用</b>：遍历所有ReferenceConfig，创建远程服务的本地代理Invoker</li>
     *   <li><b>内部模块准备</b>：触发应用级内部模块的初始化，确保框架组件可用</li>
     *   <li><b>完成判断</b>：根据是否有异步任务决定是同步返回还是异步等待</li>
     *   <li><b>服务注册</b>：将所有导出的服务注册地址写入注册中心（仅提供者）</li>
     *   <li><b>引用检查</b>：验证消费者引用的服务是否可用，失败时抛出异常或记录警告</li>
     *   <li><b>状态发布</b>：触发ModuleStarted/ModuleCompletion事件，通知监听器模块启动完成</li>
     * </ol>
     * </p>
     *
     * @return 模块启动的Future对象，调用方可通过它等待启动完成或获取异常信息
     * @throws IllegalStateException 当模块处于停止、 stopping或failed状态时尝试再次启动会抛出此异常
     */
    private synchronized Future startSync() throws IllegalStateException {
        if (isStopping() || isStopped() || isFailed()) {
            throw new IllegalStateException(getIdentifier() + " is stopping or stopped, can not start again");
        }

        try {
            /*
             * 幂等性保护：
             * 如果模块已经在启动中、已启动或已完成，直接返回现有的Future避免重复执行
             */
            if (isStarting() || isStarted() || isCompletion()) {
                return startFuture;
            }

            onModuleStarting();

            initialize();

            /*
             * 接口级服务注册
             * 遍历模块内所有ServiceConfig，将Java对象暴露为RPC服务（绑定端口、生成Invoker等）
             */
            // export services
            exportServices();

            /*
             * 准备应用级内部模块：
             * 排除内部模块自身，避免自等待导致的死锁问题
             */
            // prepare application instance
            // exclude internal module to avoid wait itself
            if (moduleModel != moduleModel.getApplicationModel().getInternalModule()) {
                applicationDeployer.prepareInternalModule();
            }

            /*
             * 引用远程服务：
             * 遍历模块内所有ReferenceConfig，创建远程服务的本地代理对象
             */
            // refer services
            referServices();

            /*
             * 同步执行路径：
             * 当没有异步导出/引用任务时，直接在当前线程完成后续操作
             */
            // if no async export/refer services, just set started
            if (asyncExportingFutures.isEmpty() && asyncReferringFutures.isEmpty()) {
                //应用级注册
                // publish module started event
                onModuleStarted();

                /*
                 * 注册服务到注册中心：
                 * 将所有导出的服务地址写入注册中心（Zookeeper/Nacos等），供消费者发现
                 */
                // register services to registry
                registerServices();

                /*
                 * 验证引用配置：
                 * 检查消费者引用的服务是否可达，失败时根据check参数决定是否抛出异常
                 */
                // check reference config
                checkReferences();

                // publish module completion event
                onModuleCompletion();

                // complete module start future after application state changed
                completeStartFuture(true);
            } else {
                /*
                 * 异步执行路径：
                 * 提交到共享线程池，后台等待所有导出/引用任务完成后继续执行
                 */
                frameworkExecutorRepository.getSharedExecutor().submit(() -> {
                    try {
                        /*
                         * 等待所有服务导出完成：
                         * 阻塞直到所有ServiceConfig的异步export任务结束
                         */
                        // wait for export finish
                        waitExportFinish();

                        /*
                         * 等待所有服务引用完成：
                         * 阻塞直到所有ReferenceConfig的异步refer任务结束
                         */
                        // wait for refer finish
                        waitReferFinish();

                        // publish module started event
                        onModuleStarted();

                        /*
                         * 注册服务到注册中心：
                         * 在异步任务完成后统一执行批量注册
                         */
                        // register services to registry
                        registerServices();

                        // check reference config
                        checkReferences();

                        // publish module completion event
                        onModuleCompletion();
                    } catch (Throwable e) {
                        logger.warn(
                                CONFIG_FAILED_WAIT_EXPORT_REFER,
                                "",
                                "",
                                "wait for export/refer services occurred an exception",
                                e);
                        onModuleFailed(getIdentifier() + " start failed: " + e, e);
                    } finally {
                        // complete module start future after application state changed
                        completeStartFuture(true);
                    }
                });
            }

        } catch (Throwable e) {
            onModuleFailed(getIdentifier() + " start failed: " + e, e);
            throw e;
        }

        return startFuture;
    }

    @Override
    public Future getStartFuture() {
        return startFuture;
    }

    private boolean hasExportedServices() {
        return !configManager.getServices().isEmpty();
    }

    @Override
    public void stop() throws IllegalStateException {
        moduleModel.destroy();
    }

    @Override
    public void preDestroy() throws IllegalStateException {
        if (isStopping() || isStopped()) {
            return;
        }
        onModuleStopping();

        offline();
    }

    private void offline() {
        try {
            ModuleServiceRepository serviceRepository = moduleModel.getServiceRepository();
            List<ProviderModel> exportedServices = serviceRepository.getExportedServices();
            for (ProviderModel exportedService : exportedServices) {
                List<ProviderModel.RegisterStatedURL> statedUrls = exportedService.getStatedUrl();
                for (ProviderModel.RegisterStatedURL statedURL : statedUrls) {
                    if (statedURL.isRegistered()) {
                        doOffline(statedURL);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error(
                    LoggerCodeConstants.INTERNAL_ERROR, "", "", "Exceptions occurred when unregister services.", t);
        }
    }

    private void doOffline(ProviderModel.RegisterStatedURL statedURL) {
        RegistryFactory registryFactory = statedURL
                .getRegistryUrl()
                .getOrDefaultApplicationModel()
                .getExtensionLoader(RegistryFactory.class)
                .getAdaptiveExtension();
        Registry registry = registryFactory.getRegistry(statedURL.getRegistryUrl());
        registry.unregister(statedURL.getProviderUrl());
        statedURL.setRegistered(false);
    }

    @Override
    public synchronized void postDestroy() throws IllegalStateException {
        if (isStopped()) {
            return;
        }
        unexportServices();
        unreferServices();

        ModuleServiceRepository serviceRepository = moduleModel.getServiceRepository();
        if (serviceRepository != null) {
            List<ConsumerModel> consumerModels = serviceRepository.getReferredServices();

            for (ConsumerModel consumerModel : consumerModels) {
                try {
                    if (consumerModel.getDestroyRunner() != null) {
                        consumerModel.getDestroyRunner().run();
                    }
                } catch (Throwable t) {
                    logger.error(
                            CONFIG_UNABLE_DESTROY_MODEL,
                            "there are problems with the custom implementation.",
                            "",
                            "Unable to destroy model: consumerModel.",
                            t);
                }
            }

            List<ProviderModel> exportedServices = serviceRepository.getExportedServices();
            for (ProviderModel providerModel : exportedServices) {
                try {
                    if (providerModel.getDestroyRunner() != null) {
                        providerModel.getDestroyRunner().run();
                    }
                } catch (Throwable t) {
                    logger.error(
                            CONFIG_UNABLE_DESTROY_MODEL,
                            "there are problems with the custom implementation.",
                            "",
                            "Unable to destroy model: providerModel.",
                            t);
                }
            }
            serviceRepository.destroy();
        }
        onModuleStopped();
    }

    private void onInitialize() {
        for (DeployListener<ModuleModel> listener : listeners) {
            try {
                listener.onInitialize(moduleModel);
            } catch (Throwable e) {
                logger.error(
                        CONFIG_FAILED_START_MODEL,
                        "",
                        "",
                        getIdentifier() + " an exception occurred when handle initialize event",
                        e);
            }
        }
    }

    private void onModuleStarting() {
        setStarting();
        startFuture = new CompletableFuture();
        logger.info(getIdentifier() + " is starting.");
        applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STARTING);
    }

    private void onModuleStarted() {
        if (isStarting()) {
            setStarted();
            logger.info(getIdentifier() + " has started.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STARTED);
        }
    }

    private void onModuleCompletion() {
        if (isStarted()) {
            setCompletion();
            logger.info(getIdentifier() + " has completed.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.COMPLETION);
        }
    }

    private void onModuleFailed(String msg, Throwable ex) {
        try {
            try {
                // un-export all services if start failure
                unexportServices();
            } catch (Throwable t) {
                logger.info("Failed to un-export services after module failed.", t);
            }

            setFailed(ex);
            logger.error(CONFIG_FAILED_START_MODEL, "", "", "Model start failed: " + msg, ex);
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.FAILED);
        } finally {
            completeStartFuture(false);
        }
    }

    private void completeStartFuture(boolean value) {
        if (startFuture != null && !startFuture.isDone()) {
            startFuture.complete(value);
        }
        if (exportFuture != null && !exportFuture.isDone()) {
            exportFuture.cancel(true);
        }
        if (referFuture != null && !referFuture.isDone()) {
            referFuture.cancel(true);
        }
    }

    private void onModuleStopping() {
        try {
            setStopping();
            logger.info(getIdentifier() + " is stopping.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STOPPING);
        } finally {
            completeStartFuture(false);
        }
    }

    private void onModuleStopped() {
        try {
            setStopped();
            logger.info(getIdentifier() + " has stopped.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STOPPED);
        } finally {
            completeStartFuture(false);
        }
    }

    private void loadConfigs() {
        // load module configs
        moduleModel.getConfigManager().loadConfigs();
        moduleModel.getConfigManager().refreshAll();
    }

    private void exportServices() {
        for (ServiceConfigBase sc : configManager.getServices()) {
            exportServiceInternal(sc);
        }
    }

        /**
     * 注册模块中配置的所有服务到注册中心。
     * <p>
     * 该方法遍历配置管理器中的所有服务配置（ServiceConfigBase），对于每个未显式禁用注册的服务，
     * 调用registerServiceInternal()方法执行具体的注册逻辑。注册完成后，刷新应用级别的服务实例信息，
     * 确保服务元数据和状态保持最新。
     * </p>
     * <p>
     * 注册条件：只有当服务的isRegister()属性不为false时才执行注册，
     * 允许通过配置register="false"来跳过特定服务的注册。
     * </p>
     */
    private void registerServices() {
        for (ServiceConfigBase sc : configManager.getServices()) {
            if (!Boolean.FALSE.equals(sc.isRegister())) {
                registerServiceInternal(sc);
            }
        }
        /*
         * 刷新应用级别的服务实例信息，确保服务元数据同步更新
         */
        applicationDeployer.refreshServiceInstance();
    }


    private void checkReferences() {
        Optional<ModuleConfig> module = configManager.getModule();
        long timeout = module.map(ModuleConfig::getCheckReferenceTimeout).orElse(30000L);
        for (ReferenceConfigBase<?> rc : configManager.getReferences()) {
            referenceCache.check(rc, timeout);
        }
    }

    /**
     * 导出单个服务配置，支持同步和异步两种模式。
     * <p>
     * 该方法是模块部署器中服务导出的核心执行单元，负责将ServiceConfig转换为可远程调用的服务。
     * 根据配置的异步标志决定是在当前线程立即执行，还是提交到专用线程池异步执行。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>配置刷新</b>：如果ServiceConfig未刷新，先调用refresh解析和合并配置参数</li>
     *   <li><b>幂等性检查</b>：如果服务已导出则直接返回，避免重复导出</li>
     *   <li><b>异步判断</b>：根据exportAsync全局配置或sc.shouldExportAsync()单独配置决定执行模式</li>
     *   <li><b>异步执行</b>：提交到serviceExportExecutor线程池，通过CompletableFuture跟踪任务状态</li>
     *   <li><b>同步执行</b>：直接在当前线程调用export，使用AUTO_REGISTER_BY_DEPLOYER注册类型（延迟到应用启动完成后注册）</li>
     *   <li><b>异常处理</b>：异步模式下捕获异常并记录日志，不会中断其他服务的导出流程</li>
     *   <li><b>状态标记</b>：如果服务配置了注册中心，设置registryInteracted标志用于后续判断是否需要元数据交互</li>
     * </ol>
     * </p>
     *
     * @param sc 服务配置基类对象，实际类型为ServiceConfig<?>，包含服务接口的所有导出配置
     */
    private void exportServiceInternal(ServiceConfigBase sc) {
        ServiceConfig<?> serviceConfig = (ServiceConfig<?>) sc;
        if (!serviceConfig.isRefreshed()) {
            serviceConfig.refresh();
        }
        if (sc.isExported()) {
            return;
        }
        /*
         * 异步导出路径：
         * 当配置了全局异步导出或服务级别异步导出时，提交到专用线程池执行
         */
        if (exportAsync || sc.shouldExportAsync()) {
            ExecutorService executor = executorRepository.getServiceExportExecutor();
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                    () -> {
                        try {
                            if (!sc.isExported()) {
                                sc.export();
                                exportedServices.add(sc);
                            }
                        } catch (Throwable t) {
                            logger.error(
                                    CONFIG_FAILED_EXPORT_SERVICE,
                                    "",
                                    "",
                                    "Failed to async export service config: " + getIdentifier() + " , catch error : "
                                            + t.getMessage(),
                                    t);
                        }
                    },
                    executor);

            asyncExportingFutures.add(future);
        } else {
            /*
             * 同步导出路径：
             * 在当前线程立即执行导出，使用AUTO_REGISTER_BY_DEPLOYER模式延迟注册
             * 这样可以在所有服务导出完成后再统一注册，避免部分服务提前暴露
             */
            if (!sc.isExported()) {
                sc.export(RegisterTypeEnum.AUTO_REGISTER_BY_DEPLOYER);
                exportedServices.add(sc);
            }
        }

        /*
         * 标记注册中心交互状态：
         * 如果服务配置了注册中心，后续可能需要执行元数据上报和服务注册操作
         */
        if (serviceConfig.hasRegistrySpecified()) {
            registryInteracted = true;
        }
    }

    /**
     * 执行单个服务配置的内部注册逻辑，包含状态检查和前置准备。
     * <p>
     * 该方法在调用实际注册之前会进行一系列校验，确保服务处于可注册状态：
     * <ol>
     *   <li>确保服务配置已刷新，如果未刷新则先调用refresh()方法加载最新配置</li>
     *   <li>检查服务是否已导出，未导出的服务不能直接注册，直接返回</li>
     *   <li>检查服务是否配置了延迟注册，如果是则跳过本次注册，等待延迟任务执行</li>
     *   <li>所有条件满足后，调用serviceConfig.register(true)执行实际的注册操作</li>
     * </ol>
     * </p>
     *
     * @param sc 服务配置基类对象，会被转换为ServiceConfig执行具体操作
     */
    private void registerServiceInternal(ServiceConfigBase sc) {
        ServiceConfig<?> serviceConfig = (ServiceConfig<?>) sc;
        /*
         * 确保服务配置已刷新，加载最新的配置参数
         */
        if (!serviceConfig.isRefreshed()) {
            serviceConfig.refresh();
        }
        /*
         * 未导出的服务不能注册，直接跳过
         */
        if (!sc.isExported()) {
            return;
        }
        /*
         * 延迟注册的服务由定时任务处理，此处跳过
         */
        if (sc.shouldDelay()) {
            return;
        }
        sc.register(true);
    }


    private void unexportServices() {
        exportedServices.forEach(sc -> {
            try {
                configManager.removeConfig(sc);
                sc.unexport();
            } catch (Throwable t) {
                logger.info("Failed to un-export service. Service Key: " + sc.getUniqueServiceName(), t);
            }
        });
        exportedServices.clear();

        asyncExportingFutures.forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
        asyncExportingFutures.clear();
    }

    /**
     * 引用远程服务，为所有配置的ReferenceConfig创建本地代理对象。
     * <p>
     * 该方法是Dubbo服务消费者的核心启动逻辑，负责遍历模块内所有ReferenceConfig配置，
     * 根据配置创建远程服务的本地Invoker代理，使得调用方可以像调用本地方法一样调用远程服务。
     * 支持同步和异步两种引用模式，并通过ReferenceCache实现代理对象的缓存和复用。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>配置刷新</b>：如果ReferenceConfig未刷新，先调用refresh解析和合并配置参数</li>
     *   <li><b>初始化判断</b>：通过shouldInit检查是否需要立即创建连接（lazy=true时延迟到首次调用）</li>
     *   <li><b>异步判断</b>：根据referAsync全局配置或rc.shouldReferAsync()单独配置决定执行模式</li>
     *   <li><b>异步引用</b>：提交到serviceReferExecutor线程池，通过CompletableFuture跟踪任务状态</li>
     *   <li><b>同步引用</b>：直接在当前线程调用referenceCache.get创建Invoker代理并建立网络连接</li>
     *   <li><b>异常处理</b>：捕获异常后记录日志、销毁已创建的缓存对象，然后重新抛出异常中断启动流程</li>
     * </ol>
     * </p>
     */
    private void referServices() {
        // 拿到所有@DubboReference标注的服务，然后转化为ReferenceConfig
        configManager.getReferences().forEach(rc -> {
            try {
                ReferenceConfig<?> referenceConfig = (ReferenceConfig<?>) rc;
                if (!referenceConfig.isRefreshed()) {
                    referenceConfig.refresh();
                }

                /*
                 * 懒加载判断：
                 * 只有shouldInit返回true时才立即创建引用，否则延迟到首次RPC调用时再初始化
                 */
                if (rc.shouldInit()) {
                    /*
                     * 异步引用路径：
                     * 提交到专用线程池后台执行，避免阻塞主线程导致启动缓慢
                     */
                    if (referAsync || rc.shouldReferAsync()) {
                        ExecutorService executor = executorRepository.getServiceReferExecutor();
                        CompletableFuture<Void> future = CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        /*
                                         * 从缓存获取或创建引用：
                                         * ReferenceCache会检查是否已有相同key的Invoker，有则直接返回，无则创建新的
                                         */
                                        referenceCache.get(rc, false);
                                    } catch (Throwable t) {
                                        logger.error(
                                                CONFIG_FAILED_EXPORT_SERVICE,
                                                "",
                                                "",
                                                "Failed to async export service config: " + getIdentifier()
                                                        + " , catch error : " + t.getMessage(),
                                                t);
                                    }
                                },
                                executor);

                        asyncReferringFutures.add(future);
                    } else {
                        /*
                         * 同步引用路径：
                         * 在当前线程立即创建Invoker代理，阻塞直到连接建立完成
                         */
                        referenceCache.get(rc, false);
                    }
                }
            } catch (Throwable t) {
                /*
                 * 引用失败处理：
                 * 记录错误日志、清理缓存中的失败对象，然后抛出异常中断模块启动
                 */
                logger.error(
                        CONFIG_FAILED_REFERENCE_MODEL,
                        "",
                        "",
                        "Model reference failed: " + getIdentifier() + " , catch error : " + t.getMessage(),
                        t);
                referenceCache.destroy(rc);
                throw t;
            }
        });
    }

    private void unreferServices() {
        try {
            asyncReferringFutures.forEach(future -> {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            });
            asyncReferringFutures.clear();
            referenceCache.destroyAll();
            for (ReferenceConfigBase<?> rc : configManager.getReferences()) {
                rc.destroy();
            }
        } catch (Exception ignored) {
        }
    }

    private void waitExportFinish() {
        try {
            logger.info(getIdentifier() + " waiting services exporting ...");
            exportFuture = CompletableFuture.allOf(asyncExportingFutures.toArray(new CompletableFuture[0]));
            exportFuture.get();
        } catch (Throwable e) {
            logger.warn(
                    CONFIG_FAILED_EXPORT_SERVICE,
                    "",
                    "",
                    getIdentifier() + " export services occurred an exception: " + e.toString());
        } finally {
            logger.info(getIdentifier() + " export services finished.");
            asyncExportingFutures.clear();
        }
    }

    private void waitReferFinish() {
        try {
            logger.info(getIdentifier() + " waiting services referring ...");
            referFuture = CompletableFuture.allOf(asyncReferringFutures.toArray(new CompletableFuture[0]));
            referFuture.get();
        } catch (Throwable e) {
            logger.warn(
                    CONFIG_FAILED_REFER_SERVICE,
                    "",
                    "",
                    getIdentifier() + " refer services occurred an exception: " + e.toString());
        } finally {
            logger.info(getIdentifier() + " refer services finished.");
            asyncReferringFutures.clear();
        }
    }

    @Override
    public boolean isBackground() {
        return background;
    }

    private boolean isExportBackground() {
        return moduleModel.getConfigManager().getProviders().stream()
                .map(ProviderConfig::getExportBackground)
                .anyMatch(k -> k != null && k);
    }

    private boolean isReferBackground() {
        return moduleModel.getConfigManager().getConsumers().stream()
                .map(ConsumerConfig::getReferBackground)
                .anyMatch(k -> k != null && k);
    }

    @Override
    public ReferenceCache getReferenceCache() {
        return referenceCache;
    }

    @Override
    public void registerServiceInstance() {
        applicationDeployer.registerServiceInstance();
    }

    /**
     * Prepare for export/refer service, trigger initializing application and module
     */
    @Override
    public void prepare() {
        applicationDeployer.initialize();
        this.initialize();
    }

    @Override
    public boolean hasRegistryInteraction() {
        return registryInteracted;
    }

    @Override
    public ApplicationDeployer getApplicationDeployer() {
        return applicationDeployer;
    }
}
