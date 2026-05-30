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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotSwitcher;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.StateRouter;
import org.apache.dubbo.rpc.cluster.router.state.StateRouterFactory;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;

/**
 * Router chain
 */
public class RouterChain<T> {
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(RouterChain.class);

    /**
     * 主路由链，承载当前正在生效的路由规则集合
     * 在路由规则更新时，新规则会首先构建在此链上，经过验证后切换为当前链
     */
    private volatile SingleRouterChain<T> mainChain;

    /**
     * 备用路由链，用于在异步构建新路由链时暂存旧规则或作为过渡状态
     * 确保在路由规则刷新过程中，服务调用依然可以使用稳定的路由逻辑而不被中断
     */
    private volatile SingleRouterChain<T> backupChain;

    /**
     * 当前实际执行的路由链引用
     * 该字段指向 mainChain 或 backupChain 中的一个，通过原子切换实现路由规则的无锁更新和无缝衔接
     */
    private volatile SingleRouterChain<T> currentChain;


    /**
     * 构建路由链对象，包含多个独立的路由链实例以支持并发场景下的无锁读取。
     * <p>
     * 该方法通过调用buildSingleChain()创建两个完全独立的SingleRouterChain实例，
     * 并将它们封装到RouterChain中。这种双链设计是Dubbo的优化策略：
     * 在路由链更新时，可以交替使用两条链，避免在重建路由规则时影响正在进行的RPC调用，
     * 实现读写分离和无锁化操作，提升高并发场景下的性能。
     * </p>
     *
     * @param interfaceClass 服务接口类型，用于加载与该接口关联的路由规则
     * @param url 包含路由配置的URL对象，通常为消费者URL或订阅URL
     * @return 包含两条单路由链的RouterChain对象
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> RouterChain<T> buildChain(Class<T> interfaceClass, URL url) {
        SingleRouterChain<T> chain1 = buildSingleChain(interfaceClass, url);
        SingleRouterChain<T> chain2 = buildSingleChain(interfaceClass, url);
        return new RouterChain<>(new SingleRouterChain[] {chain1, chain2});
    }


    /**
     * 构建单个路由链实例，加载并组装所有激活的路由器和状态路由器。
     * <p>
     * 该方法负责从模块模型中扩展加载器获取所有激活的路由工厂和状态路由工厂，
     * 创建具体的路由器实例并按优先级排序，最终构建成一个完整的路由链。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>从URL中提取ModuleModel，用于获取扩展加载器</li>
     *   <li>加载所有激活的RouterFactory扩展（通过ROUTER_KEY参数筛选），调用getRouter()创建路由器实例</li>
     *   <li>对普通路由器进行排序（通过Router.compareTo()），确保路由规则按正确顺序执行</li>
     *   <li>加载所有激活的StateRouterFactory扩展，创建状态路由器实例（无需排序）</li>
     *   <li>从配置中读取SHOULD_FAIL_FAST参数，决定是否在路由失败时快速抛出异常</li>
     *   <li>获取RouterSnapshotSwitcher单例Bean，用于管理路由快照的启用/禁用状态</li>
     *   <li>创建并返回SingleRouterChain实例，包含普通路由器列表、状态路由器列表、快速失败标志和快照开关</li>
     * </ol>
     * </p>
     *
     * @param interfaceClass 服务接口类型，用于状态路由器创建时绑定接口信息
     * @param url 包含路由配置的URL对象，通常为消费者URL或订阅URL
     * @return 构建完成的SingleRouterChain实例，包含所有激活的路由规则
     */
    public static <T> SingleRouterChain<T> buildSingleChain(Class<T> interfaceClass, URL url) {
        ModuleModel moduleModel = url.getOrDefaultModuleModel();

        /*
         * 加载所有激活的普通路由器工厂，创建路由器实例并按优先级排序
         */
        List<RouterFactory> extensionFactories =
                moduleModel.getExtensionLoader(RouterFactory.class).getActivateExtension(url, ROUTER_KEY);

        List<Router> routers = extensionFactories.stream()
                .map(factory -> factory.getRouter(url))
                .sorted(Router::compareTo)
                .collect(Collectors.toList());

        /*
         * 加载所有激活的状态路由器工厂，创建状态路由器实例
         */
        List<StateRouter<T>> stateRouters =
                moduleModel.getExtensionLoader(StateRouterFactory.class).getActivateExtension(url, ROUTER_KEY).stream()
                        .map(factory -> factory.getRouter(interfaceClass, url))
                        .collect(Collectors.toList());

        /*
         * 读取快速失败配置，默认值为true
         */
        boolean shouldFailFast = Boolean.parseBoolean(
                ConfigurationUtils.getProperty(moduleModel, Constants.SHOULD_FAIL_FAST_KEY, "true"));

        /*
         * 获取路由快照开关Bean，用于管理快照功能
         */
        RouterSnapshotSwitcher routerSnapshotSwitcher =
                ScopeModelUtil.getFrameworkModel(moduleModel).getBeanFactory().getBean(RouterSnapshotSwitcher.class);

        return new SingleRouterChain<>(routers, stateRouters, shouldFailFast, routerSnapshotSwitcher);
    }

    public RouterChain(SingleRouterChain<T>[] chains) {
        if (chains.length != 2) {
            throw new IllegalArgumentException("chains' size should be 2.");
        }
        this.mainChain = chains[0];
        this.backupChain = chains[1];
        this.currentChain = this.mainChain;
    }

    private final AtomicReference<BitList<Invoker<T>>> notifyingInvokers = new AtomicReference<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ReadWriteLock getLock() {
        return lock;
    }

        /**
     * 根据当前调用上下文获取应使用的单次路由链实例
     * 该方法通过对比通知中的Invoker列表与当前可用的Invoker列表，智能选择主路由链或备用路由链，
     * 确保在路由规则动态更新期间调用的连续性和数据的一致性
     *
     * @param url 消费者URL，包含路由配置信息
     * @param availableInvokers 当前可用的Invoker列表，用于判断是否处于通知刷新阶段
     * @param invocation 调用上下文，包含方法名、参数等路由决策所需信息
     * @return 选定的SingleRouterChain实例，用于执行具体的路由过滤逻辑
     */
    public SingleRouterChain<T> getSingleChain(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // If current is in:
        // 1. [setInvokers](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/RpcContext.java#L721-L723) is in progress
        // 2. Most of the invocation should use backup chain => currentChain == backupChain
        // 3. Main chain has been update success => notifyingInvokers.get() != null
        //     If `availableInvokers` is created from origin invokers => use backup chain
        //     If `availableInvokers` is created from newly invokers  => use main chain

        // 获取正在通知刷新的Invoker列表引用，用于判断当前是否处于路由链切换的中间状态
        BitList<Invoker<T>> notifying = notifyingInvokers.get();

        // 如果存在正在通知的列表，且当前执行链已切换至备用链，同时可用列表与通知列表同源，
        // 则说明本次调用基于新产生的Invoker数据，应直接使用已更新完成的主路由链
        if (notifying != null
                && currentChain == backupChain
                && availableInvokers.getOriginList() == notifying.getOriginList()) {
            return mainChain;
        }

        // 默认情况下返回当前生效的路由链（通常是备用链，直到切换完全结束）
        return currentChain;
    }


    /**
     * @deprecated use {@link RouterChain#getSingleChain(URL, BitList, Invocation)} and {@link SingleRouterChain#route(URL, BitList, Invocation)} instead
     */
    @Deprecated
    public List<Invoker<T>> route(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        return getSingleChain(url, availableInvokers, invocation).route(url, availableInvokers, invocation);
    }

    /**
     * 通知路由链接收来自注册中心的初始地址或变更后的地址列表
     * 采用双缓冲（Double Buffering）机制实现路由链的无锁更新，确保在地址刷新期间业务调用的连续性与一致性
     *
     * @param invokers 新的服务提供者Invoker列表
     * @param switchAction 用于切换Directory内部Invoker引用的回调动作，确保原子性更新
     */
    public synchronized void setInvokers(BitList<Invoker<T>> invokers, Runnable switchAction) {
        try {
            // Lock to prevent directory continue list
            // 获取全局写锁，暂停新的调用进入路由选择阶段，为切换做准备
            lock.writeLock().lock();

            // Switch to back up chain. Will update main chain first.
            // 将当前执行链指向备用链，确保后续新进来的调用暂时使用旧的路由规则，为主链更新腾出空间
            currentChain = backupChain;
        } finally {
            // Release lock to minimize the impact for each newly created invocations as much as possible.
            // Should not release lock until main chain update finished. Or this may cause long hang.
            // 快速释放全局锁，减少对并发调用的阻塞时间
            lock.writeLock().unlock();
        }

        // Refresh main chain.
        // No one can request to use main chain. [currentChain](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/RouterChain.java#L62-L62) is backup chain. [route](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-compatible/src/main/java/com/alibaba/dubbo/rpc/cluster/Router.java#L33-L37) method cannot access main
        // chain.
        try {
            // Lock main chain to wait all invocation end
            // To wait until no one is using main chain.
            // 获取主路由链的独立写锁，等待所有正在使用主链的调用执行完毕，确保数据静止
            mainChain.getLock().writeLock().lock();

            // refresh
            // 利用最新的Invoker列表重建主路由链
            mainChain.setInvokers(invokers);
        } catch (Throwable t) {
            logger.error(LoggerCodeConstants.INTERNAL_ERROR, "", "", "Error occurred when refreshing router chain.", t);
            throw t;
        } finally {
            // Unlock main chain
            mainChain.getLock().writeLock().unlock();
        }

        // Set the reference of newly invokers to temp variable.
        // Reason: The next step will switch the invokers reference in directory, so we should check the
        // `availableInvokers`
        //         argument when [route](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-compatible/src/main/java/com/alibaba/dubbo/rpc/cluster/Router.java#L33-L37). If the current invocation use newly invokers, we should use main chain to
        // route, and
        //         this can prevent use newly invokers to route backup chain, which can only route origin invokers now.
        // 将新Invoker列表存入临时变量，用于在随后的切换窗口期辅助路由决策，防止新旧数据错配
        notifyingInvokers.set(invokers);

        // Switch the invokers reference in directory.
        // Cannot switch before update main chain or after backup chain update success. Or that will cause state
        // inconsistent.
        // 执行Directory内部的引用切换，此时消费者获取到的将是最新的Invoker列表
        switchAction.run();

        try {
            // Lock to prevent directory continue list
            // The invokers reference in directory now should be the newly one and should always use the newly one once
            // lock released.
            // 再次获取全局写锁，准备将流量正式切换到已更新好的主链上
            lock.writeLock().lock();

            // Switch to main chain. Will update backup chain later.
            // 将当前执行链指向主链，完成核心的路由规则切换
            currentChain = mainChain;

            // Clean up temp variable.
            // `availableInvokers` check is useless now, because [route](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-compatible/src/main/java/com/alibaba/dubbo/rpc/cluster/Router.java#L33-L37) method will no longer receive any
            // `availableInvokers` related
            // with the origin invokers. The getter of invokers reference in directory is locked now, and will return
            // newly invokers
            // once lock released.
            // 清理临时通知变量，切换过程结束，后续调用将统一使用新链和新地址
            notifyingInvokers.set(null);
        } finally {
            // Release lock to minimize the impact for each newly created invocations as much as possible.
            // Will use newly invokers and main chain now.
            // 释放全局锁，恢复正常的并发调用
            lock.writeLock().unlock();
        }

        // Refresh main chain.
        // No one can request to use main chain. [currentChain](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/RouterChain.java#L62-L62) is main chain. [route](file:///Users/liupengyu/openCode/dubbo/self-dubbo-source/dubbo-compatible/src/main/java/com/alibaba/dubbo/rpc/cluster/Router.java#L33-L37) method cannot access backup
        // chain.
        try {
            // Lock main chain to wait all invocation end
            // 获取备用路由链的独立写锁，准备对其进行同步更新
            backupChain.getLock().writeLock().lock();

            // refresh
            // 更新备用链，使其与主链保持一致，为下一次更新周期做准备
            backupChain.setInvokers(invokers);
        } catch (Throwable t) {
            logger.error(LoggerCodeConstants.INTERNAL_ERROR, "", "", "Error occurred when refreshing router chain.", t);
            throw t;
        } finally {
            // Unlock backup chain
            backupChain.getLock().writeLock().unlock();
        }
    }


    public synchronized void destroy() {
        // 1. destroy another
        backupChain.destroy();

        // 2. switch
        lock.writeLock().lock();
        currentChain = backupChain;
        lock.writeLock().unlock();

        // 4. destroy
        mainChain.destroy();
    }

    public void addRouters(List<Router> routers) {
        mainChain.addRouters(routers);
        backupChain.addRouters(routers);
    }

    public SingleRouterChain<T> getCurrentChain() {
        return currentChain;
    }

    public List<Router> getRouters() {
        return currentChain.getRouters();
    }

    public StateRouter<T> getHeadStateRouter() {
        return currentChain.getHeadStateRouter();
    }

    @Deprecated
    public List<StateRouter<T>> getStateRouters() {
        return currentChain.getStateRouters();
    }
}
