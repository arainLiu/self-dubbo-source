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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.profiler.ProfilerSwitch;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvocationProfilerUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcServiceContext;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RESELECT_COUNT;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_CONNECTIVITY_VALIDATION;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RESELECT_COUNT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_RESELECT_INVOKERS;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;

/**
 * AbstractClusterInvoker
 */
public abstract class AbstractClusterInvoker<T> implements ClusterInvoker<T> {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(AbstractClusterInvoker.class);

    protected Directory<T> directory;

    protected boolean availableCheck;

    private volatile int reselectCount = DEFAULT_RESELECT_COUNT;

    private volatile boolean enableConnectivityValidation = true;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    private volatile Invoker<T> stickyInvoker = null;

    public AbstractClusterInvoker() {}

    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        if (directory == null) {
            throw new IllegalArgumentException("service directory == null");
        }

        this.directory = directory;
        // sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        this.availableCheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
        Configuration configuration = ConfigurationUtils.getGlobalConfiguration(url.getOrDefaultModuleModel());
        this.reselectCount = configuration.getInt(RESELECT_COUNT, DEFAULT_RESELECT_COUNT);
        this.enableConnectivityValidation = configuration.getBoolean(ENABLE_CONNECTIVITY_VALIDATION, true);
    }

    @Override
    public Class<T> getInterface() {
        return getDirectory().getInterface();
    }

    @Override
    public URL getUrl() {
        return getDirectory().getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return getDirectory().getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return getDirectory().isAvailable();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            getDirectory().destroy();
        }
    }

    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }

    /**
     * 使用负载均衡策略选择一个Invoker进行RPC调用。
     * <p>
     * 该方法的处理逻辑：
     * 1. 检查Invoker列表是否为空，为空则直接返回null；
     * 2. 获取方法级别的sticky（粘滞）配置，判断是否启用粘滞连接；
     * 3. 如果启用了sticky且之前的stickyInvoker仍然有效（在候选列表中、未被选中过、可用），则直接返回该Invoker；
     * 4. 否则调用doSelect方法执行具体的负载均衡选择逻辑；
     * 5. 如果启用了sticky，将本次选择的Invoker保存为下次的stickyInvoker。
     * </p>
     * <p>
     * 粘滞连接特性：
     * - 优先复用上次选择的Invoker，减少连接切换开销；
     * - 当stickyInvoker不可用或在排除列表中时，会重新选择；
     * - doSelect方法负责处理重试和排除已选中的Invoker。
     * </p>
     *
     * @param loadbalance 负载均衡策略，用于从多个Invoker中选择一个（如Random、RoundRobin等）
     * @param invocation RPC调用信息，包含方法名、参数等，用于获取方法级配置
     * @param invokers 候选的Invoker列表，所有可用的服务提供者
     * @param selected 已经选择过的Invoker列表，用于排除避免重复调用同一个提供者
     * @return Invoker<T> 最终选择的Invoker，如果列表为空则返回null
     * @throws RpcException 当负载均衡选择失败或发生异常时抛出
     */
    protected Invoker<T> select(
            LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected)
            throws RpcException {

        // 如果候选Invoker列表为空，直接返回null
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 获取调用方法名，用于查询方法级别的配置参数
        String methodName = invocation == null ? StringUtils.EMPTY_STRING : RpcUtils.getMethodName(invocation);

        // 从URL中获取方法的sticky（粘滞）配置，默认不启用粘滞连接
        boolean sticky =
                invokers.get(0).getUrl().getMethodParameter(methodName, CLUSTER_STICKY_KEY, DEFAULT_CLUSTER_STICKY);

        // 如果当前的stickyInvoker不在候选列表中（可能已下线或被移除），清空引用
        // ignore overloaded method
        if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
            stickyInvoker = null;
        }

        // 如果启用了sticky且stickyInvoker有效（未在排除列表中且可用），直接返回以避免不必要的切换
        // ignore concurrency problem
        if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {
            if (availableCheck && stickyInvoker.isAvailable()) {
                return stickyInvoker;
            }
        }

        // 调用子类的具体实现，执行负载均衡选择逻辑（包含重试和排除已选中的Invoker）
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        // 如果启用了sticky，更新stickyInvoker为本次选择的结果，供下次调用优先使用
        if (sticky) {
            stickyInvoker = invoker;
        }

        return invoker;
    }

    /**
     * 执行负载均衡选择Invoker，处理已选中排除和不可用Invoker的重试逻辑。
     * <p>
     * 该方法的处理流程：
     * 1. 边界条件处理：空列表返回null，单元素列表直接返回（需验证可用性）；
     * 2. 调用LoadBalance选择一个Invoker；
     * 3. 检查选中的Invoker是否在排除列表中或不可用；
     * 4. 如果需要排除，调用reselect重新选择一个可用的Invoker；
     * 5. 如果reselect失败，使用顺序轮转策略（当前索引+1）作为兜底方案；
     * 6. 捕获异常并记录日志，提供availablecheck配置的诊断建议。
     * </p>
     *
     * @param loadbalance 负载均衡策略，用于从多个Invoker中选择一个
     * @param invocation RPC调用信息，包含方法名、参数等
     * @param invokers 候选的Invoker列表
     * @param selected 已经选择过的Invoker列表，需要排除避免重复调用
     * @return Invoker<T> 最终选择的Invoker实例
     * @throws RpcException 当负载均衡选择失败或发生异常时抛出
     */
    private Invoker<T> doSelect(
            LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected)
            throws RpcException {

        // 如果候选Invoker列表为空，直接返回null
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 如果只有一个Invoker，验证其有效性后直接返回
        if (invokers.size() == 1) {
            Invoker<T> tInvoker = invokers.get(0);
            checkShouldInvalidateInvoker(tInvoker);
            return tInvoker;
        }

        // 通过负载均衡器选择一个Invoker
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        // 判断选中的Invoker是否需要排除：已在selected列表中或不可用且启用了可用性检查
        boolean isSelected = selected != null && selected.contains(invoker);
        boolean isUnavailable = availableCheck && !invoker.isAvailable() && getUrl() != null;

        // 如果Invoker不可用，将其标记为无效并从缓存中移除
        if (isUnavailable) {
            invalidateInvoker(invoker);
        }

        // 如果Invoker已被选中过或不可用，执行重新选择逻辑
        if (isSelected || isUnavailable) {
            try {
                // 重新选择一个不在排除列表中且可用的Invoker
                Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availableCheck);
                if (rInvoker != null) {
                    invoker = rInvoker;
                } else {
                    // 如果reselect无法找到合适的Invoker，使用顺序轮转策略选择下一个Invoker作为兜底
                    int index = invokers.indexOf(invoker);
                    try {
                        // 通过取模运算实现环形轮转，避免数组越界
                        invoker = invokers.get((index + 1) % invokers.size());
                    } catch (Exception e) {
                        logger.warn(
                                CLUSTER_FAILED_RESELECT_INVOKERS,
                                "select invokers exception",
                                "",
                                e.getMessage() + " may because invokers list dynamic change, ignore.",
                                e);
                    }
                }
            } catch (Throwable t) {
                // 重新选择失败时记录错误日志，并提供配置建议（可设置availablecheck=false禁用可用性检查）
                logger.error(
                        CLUSTER_FAILED_RESELECT_INVOKERS,
                        "failed to reselect invokers",
                        "",
                        "cluster reselect fail reason is :" + t.getMessage()
                                + " if can not solve, you can set cluster.availablecheck=false in url",
                        t);
            }
        }

        return invoker;
    }


    /**
     * Reselect, use invokers not in `selected` first, if all invokers are in `selected`,
     * just pick an available one using loadbalance policy.
     *
     * @param loadbalance    load balance policy
     * @param invocation     invocation
     * @param invokers       invoker candidates
     * @param selected       exclude selected invokers or not
     * @param availableCheck check invoker available if true
     * @return the reselect result to do invoke
     * @throws RpcException exception
     */
    private Invoker<T> reselect(
            LoadBalance loadbalance,
            Invocation invocation,
            List<Invoker<T>> invokers,
            List<Invoker<T>> selected,
            boolean availableCheck)
            throws RpcException {

        // Allocating one in advance, this list is certain to be used.
        List<Invoker<T>> reselectInvokers = new ArrayList<>(Math.min(invokers.size(), reselectCount));

        // 1. Try picking some invokers not in `selected`.
        //    1.1. If all selectable invokers' size is smaller than reselectCount, just add all
        //    1.2. If all selectable invokers' size is greater than reselectCount, randomly select reselectCount.
        //            The result size of invokers might smaller than reselectCount due to disAvailable or de-duplication
        // (might be zero).
        //            This means there is probable that reselectInvokers is empty however all invoker list may contain
        // available invokers.
        //            Use reselectCount can reduce retry times if invokers' size is huge, which may lead to long time
        // hang up.
        if (reselectCount >= invokers.size()) {
            for (Invoker<T> invoker : invokers) {
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }

                if (selected == null || !selected.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        } else {
            for (int i = 0; i < reselectCount; i++) {
                // select one randomly
                Invoker<T> invoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }
                // de-duplication
                if (selected == null || !selected.contains(invoker) || !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 2. Use loadBalance to select one (all the reselectInvokers are available)
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 3. reselectInvokers is empty. Unable to find at least one available invoker.
        //    Re-check all the selected invokers. If some in the selected list are available, add to reselectInvokers.
        if (selected != null) {
            for (Invoker<T> invoker : selected) {
                if ((invoker.isAvailable()) // available first
                        && !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 4. If reselectInvokers is not empty after re-check.
        //    Pick an available invoker using loadBalance policy
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 5. No invoker match, return null.
        return null;
    }

    private void checkShouldInvalidateInvoker(Invoker<T> invoker) {
        if (availableCheck && !invoker.isAvailable()) {
            invalidateInvoker(invoker);
        }
    }

    private void invalidateInvoker(Invoker<T> invoker) {
        if (enableConnectivityValidation) {
            if (getDirectory() != null) {
                getDirectory().addInvalidateInvoker(invoker);
            }
        }
    }

    /**
     * 执行RPC调用的统一入口，协调服务发现、负载均衡和集群容错策略。
     * <p>
     * 该方法的处理流程：
     * 1. 检查集群是否已销毁；
     * 2. 通过Directory获取经过路由过滤的Invoker列表；
     * 3. 验证Invoker列表的有效性（非空且与调用匹配）；
     * 4. 初始化负载均衡策略；
     * 5. 为异步调用附加唯一标识ID；
     * 6. 调用子类的doInvoke方法执行具体的集群容错逻辑（如Failover、Failfast等）。
     * </p>
     * <p>
     * 该方法还集成了性能剖析工具（InvocationProfilerUtils），用于监控路由和集群调用两个阶段的耗时。
     * </p>
     *
     * @param invocation RPC调用信息，包含方法名、参数类型、参数值等
     * @return Result 调用结果，可能是同步或异步返回
     * @throws RpcException 当集群已销毁、无可用提供者或调用失败时抛出异常
     */
    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        // 检查集群是否已被销毁，防止在关闭状态下继续提供服务
        checkWhetherDestroyed();

        // 绑定附件到invocation中（当前注释掉的代码用于从RpcContext获取上下文附件并合并到invocation）
        //        Map<String, Object> contextAttachments = RpcContext.getClientAttachment().getObjectAttachments();
        //        if (contextAttachments != null && contextAttachments.size() != 0) {
        //            ((RpcInvocation) invocation).addObjectAttachmentsIfAbsent(contextAttachments);
        //        }

        // 启动路由阶段的性能剖析，记录路由规则匹配的耗时
        InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Router route.");
        // 通过Directory获取经过路由过滤后的Invoker列表
        List<Invoker<T>> invokers = list(invocation);
        // 释放路由阶段的性能剖析
        InvocationProfilerUtils.releaseDetailProfiler(invocation);

        // 验证Invoker列表的有效性，确保有可用的服务提供者
        checkInvokers(invokers, invocation);

        // 初始化负载均衡策略（根据URL配置选择具体的LoadBalance实现，如Random、RoundRobin等）
        LoadBalance loadbalance = initLoadBalance(invokers, invocation);
        // 如果是异步调用，为invocation附加唯一的调用ID，用于后续的结果关联
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        // 启动集群调用阶段的性能剖析，记录具体集群策略（如Failover）的执行耗时
        InvocationProfilerUtils.enterDetailProfiler(
                invocation, () -> "Cluster " + this.getClass().getName() + " invoke.");
        try {
            // 调用子类的具体实现，执行集群容错逻辑（由子类决定是故障转移、快速失败还是其他策略）
            return doInvoke(invocation, invokers, loadbalance);
        } finally {
            // 确保在finally块中释放性能剖析资源，防止内存泄漏
            InvocationProfilerUtils.releaseDetailProfiler(invocation);
        }
    }

    protected void checkWhetherDestroyed() {
        if (destroyed.get()) {
            throw new RpcException(
                    "Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                            + " use dubbo version " + Version.getVersion()
                            + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            throw new RpcException(
                    RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER,
                    "Failed to invoke the method "
                            + RpcUtils.getMethodName(invocation) + " in the service "
                            + getInterface().getName()
                            + ". No provider available for the service "
                            + getDirectory().getConsumerUrl().getServiceKey()
                            + " from registry " + getDirectory()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion()
                            + ". Please check if the providers have been started and registered.");
        }
    }

    /**
     * 执行单个Invoker的调用，管理RpcContext上下文和性能剖析。
     * <p>
     * 该方法的处理流程：
     * 1. 设置RpcContext上下文（包括服务上下文、远程地址等信息）；
     * 2. 如果启用了性能剖析，记录Invoker调用的开始时间和目标地址；
     * 3. 设置invocation中的远程地址信息；
     * 4. 执行实际的Invoker.invoke调用；
     * 5. 在finally块中清理RpcContext上下文和释放性能剖析资源。
     * </p>
     *
     * @param invoker 待执行的服务提供者Invoker实例
     * @param invocation RPC调用信息，包含方法名、参数等
     * @return Result 调用结果，由具体的Invoker实现返回
     */
    protected Result invokeWithContext(Invoker<T> invoker, Invocation invocation) {
        // 设置RpcContext上下文，保存原始的Invoker引用用于后续恢复
        Invoker<T> originInvoker = setContext(invoker);
        Result result;
        try {
            // 如果启用了简单性能剖析开关，记录Invoker调用的详细信息（包括目标提供者地址）
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                InvocationProfilerUtils.enterProfiler(
                        invocation,
                        "Invoker invoke. Target Address: " + invoker.getUrl().getAddress());
            }

            // 设置invocation中的远程地址信息，供后续过滤器和业务逻辑使用
            setRemote(invoker, invocation);
            // 执行实际的RPC调用，委托给具体的Invoker实现
            result = invoker.invoke(invocation);
        } finally {
            // 确保在finally块中清理RpcContext上下文，防止内存泄漏和线程复用导致的数据污染
            clearContext(originInvoker);
            // 释放简单性能剖析资源，停止计时并记录性能数据
            InvocationProfilerUtils.releaseSimpleProfiler(invocation);
        }
        return result;
    }

    /**
     * 设置远程服务提供者的地址和应用名称到调用上下文和 Invocation 中。
     * 这些信息会被传递给过滤器链，以便过滤器能够获取当前调用的目标提供者信息。
     *
     * 该方法在集群调用执行前被调用，确保：
     * 1. Invocation 记录了被调用的 Invoker，用于后续的路由和负载均衡决策
     * 2. RpcServiceContext 中设置了远程地址，供监控、日志等过滤器使用
     * 3. RpcServiceContext 中设置了远程应用名称，用于服务治理和追踪
     *
     * @param invoker 被选中的远程服务提供者实例
     * @param invocation 当前的 RPC 调用信息
     */
    private void setRemote(Invoker<?> invoker, Invocation invocation) {
        // 将当前 Invoker 添加到 Invocation 的已调用列表中，用于记录调用轨迹
        invocation.addInvokedInvoker(invoker);
        // 获取当前的 RPC 服务上下文
        RpcServiceContext serviceContext = RpcContext.getServiceContext();
        // 设置远程服务提供者的网络地址
        serviceContext.setRemoteAddress(invoker.getUrl().toInetSocketAddress());
        // 设置远程服务提供者的应用名称
        serviceContext.setRemoteApplicationName(invoker.getUrl().getRemoteApplication());
    }

    /**
     * 在异步或线程池分叉场景下执行远程调用。
     * 当使用线程池派生子线程时，ThreadLocal 上下文无法自动传递，
     * 因此需要通过该方法手动设置和清理 RPC 上下文。
     *
     * 该方法确保：
     * 1. 在调用前正确设置 RpcContext 上下文信息
     * 2. 无论调用成功或失败，都在 finally 块中清理上下文，防止内存泄漏和上下文污染
     *
     * @param invoker 被选中的远程服务提供者实例
     * @param invocation 当前的 RPC 调用信息
     * @param consumerUrl 消费者 URL，用于构建上下文信息
     * @return 远程调用的结果
     */
    protected Result invokeWithContextAsync(Invoker<T> invoker, Invocation invocation, URL consumerUrl) {
        // 设置 RPC 上下文并保存原始的 Invoker 以便后续恢复
        Invoker<T> originInvoker = setContext(invoker, consumerUrl);
        Result result;
        try {
            // 执行实际的远程调用
            result = invoker.invoke(invocation);
        } finally {
            // 清理 RPC 上下文，恢复到调用前的状态
            clearContext(originInvoker);
        }
        return result;
    }


    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
            throws RpcException;

    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return getDirectory().list(invocation);
    }

    /**
     * Init LoadBalance.
     * <p>
     * if invokers is not empty, init from the first invoke's url and invocation
     * if invokes is empty, init a default LoadBalance(RandomLoadBalance)
     * </p>
     * 初始化并获取负载均衡策略实例。
     * 该方法根据调用信息从服务提供者 URL 中提取针对特定方法的负载均衡配置。
     * 如果未配置或调用者列表为空，则使用默认的负载均衡策略。
     *
     * 查找优先级的逻辑：
     * 1. 优先使用第一个 Invoker 的 URL 中配置的方法级负载均衡参数
     * 2. 如果不存在方法级配置，回退到 DEFAULT_LOADBALANCE（通常为 random）
     *
     * @param invokers 可用的服务提供者列表，用于提取 URL 配置信息
     * @param invocation RPC 调用信息，用于获取方法名以匹配特定的负载均衡规则
     * @return 选定的负载均衡策略实例
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        // 获取应用模型以便加载扩展点
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel());
        // 如果调用者列表不为空，尝试从第一个 Invoker 的 URL 中提取方法级的负载均衡配置
        if (CollectionUtils.isNotEmpty(invokers)) {
            return applicationModel
                    .getExtensionLoader(LoadBalance.class)
                    .getExtension(invokers.get(0)
                            .getUrl()
                            .getMethodParameter(
                                    RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE));
        } else {
            // 如果列表为空，直接返回默认的负载均衡策略
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
        }
    }

    private Invoker<T> setContext(Invoker<T> invoker) {
        return setContext(invoker, null);
    }

    /**
     * 设置当前调用的 RPC 上下文信息。
     * 该方法在调用远程服务前执行，将选中的 Invoker 和消费者 URL 设置到 RpcServiceContext 中，
     * 以便后续的过滤器和拦截器能够获取当前的调用目标信息。
     *
     * 同时，该方法会保存并返回原始的 Invoker，用于在调用结束后恢复上下文状态，
     * 确保多线程环境下的上下文隔离和正确性。
     *
     * @param invoker 当前选中的远程服务提供者实例
     * @param consumerUrl 消费者 URL，如果为空则使用上下文中的默认值
     * @return 调用前的原始 Invoker 实例，用于后续恢复上下文
     */
    private Invoker<T> setContext(Invoker<T> invoker, URL consumerUrl) {
        // 获取当前的 RPC 服务上下文
        RpcServiceContext context = RpcContext.getServiceContext();
        // 保存原始的 Invoker，以便调用结束后恢复
        Invoker<?> originInvoker = context.getInvoker();
        // 设置当前的 Invoker 和消费者 URL 到上下文中
        context.setInvoker(invoker)
                .setConsumerUrl(
                        null != consumerUrl
                                ? consumerUrl
                                : RpcContext.getServiceContext().getConsumerUrl());
        return (Invoker<T>) originInvoker;
    }

    private void clearContext(Invoker<T> invoker) {
        // do nothing
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(invoker);
    }
}
