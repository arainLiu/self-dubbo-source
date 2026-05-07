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

    private volatile SingleRouterChain<T> mainChain;
    private volatile SingleRouterChain<T> backupChain;
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

    public SingleRouterChain<T> getSingleChain(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // If current is in:
        // 1. `setInvokers` is in progress
        // 2. Most of the invocation should use backup chain => currentChain == backupChain
        // 3. Main chain has been update success => notifyingInvokers.get() != null
        //     If `availableInvokers` is created from origin invokers => use backup chain
        //     If `availableInvokers` is created from newly invokers  => use main chain
        BitList<Invoker<T>> notifying = notifyingInvokers.get();
        if (notifying != null
                && currentChain == backupChain
                && availableInvokers.getOriginList() == notifying.getOriginList()) {
            return mainChain;
        }
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
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public synchronized void setInvokers(BitList<Invoker<T>> invokers, Runnable switchAction) {
        try {
            // Lock to prevent directory continue list
            lock.writeLock().lock();

            // Switch to back up chain. Will update main chain first.
            currentChain = backupChain;
        } finally {
            // Release lock to minimize the impact for each newly created invocations as much as possible.
            // Should not release lock until main chain update finished. Or this may cause long hang.
            lock.writeLock().unlock();
        }

        // Refresh main chain.
        // No one can request to use main chain. `currentChain` is backup chain. `route` method cannot access main
        // chain.
        try {
            // Lock main chain to wait all invocation end
            // To wait until no one is using main chain.
            mainChain.getLock().writeLock().lock();

            // refresh
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
        //         argument when `route`. If the current invocation use newly invokers, we should use main chain to
        // route, and
        //         this can prevent use newly invokers to route backup chain, which can only route origin invokers now.
        notifyingInvokers.set(invokers);

        // Switch the invokers reference in directory.
        // Cannot switch before update main chain or after backup chain update success. Or that will cause state
        // inconsistent.
        switchAction.run();

        try {
            // Lock to prevent directory continue list
            // The invokers reference in directory now should be the newly one and should always use the newly one once
            // lock released.
            lock.writeLock().lock();

            // Switch to main chain. Will update backup chain later.
            currentChain = mainChain;

            // Clean up temp variable.
            // `availableInvokers` check is useless now, because `route` method will no longer receive any
            // `availableInvokers` related
            // with the origin invokers. The getter of invokers reference in directory is locked now, and will return
            // newly invokers
            // once lock released.
            notifyingInvokers.set(null);
        } finally {
            // Release lock to minimize the impact for each newly created invocations as much as possible.
            // Will use newly invokers and main chain now.
            lock.writeLock().unlock();
        }

        // Refresh main chain.
        // No one can request to use main chain. `currentChain` is main chain. `route` method cannot access backup
        // chain.
        try {
            // Lock main chain to wait all invocation end
            backupChain.getLock().writeLock().lock();

            // refresh
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
