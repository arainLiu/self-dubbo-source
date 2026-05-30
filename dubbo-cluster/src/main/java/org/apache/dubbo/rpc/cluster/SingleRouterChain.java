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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.cluster.router.RouterResult;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotNode;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotSwitcher;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.StateRouter;
import org.apache.dubbo.rpc.cluster.router.state.TailStateRouter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_STOP;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_NO_VALID_PROVIDER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;

/**
 * 单一路由链实现，管理有状态路由器和无状态路由器的执行。
 * 该类负责按特定顺序执行路由规则：
 * 1. 有状态路由器（如 MeshRuleRouter、ServiceRouter），维护路由状态
 * 2. 普通路由器（如 ConfigConditionRouter、TagRouter），应用动态路由规则
 *
 * 路由过程会构建快照树来跟踪每个路由器的输入输出，
 * 当路由结果为空时提供详细的调试日志信息。
 *
 * @param <T> 服务类型
 */
public class SingleRouterChain<T> {
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(SingleRouterChain.class);

    /**
     * full list of addresses from registry, classified by method name.
     */
    private volatile BitList<Invoker<T>> invokers = BitList.emptyList();

    /**
     * containing all routers, reconstruct every time 'route://' urls change.
     */
    private volatile List<Router> routers = Collections.emptyList();

    /**
     * Fixed router instances: ConfigConditionRouter, TagRouter, e.g.,
     * the rule for each instance may change but the instance will never delete or recreate.
     */
    private volatile List<Router> builtinRouters = Collections.emptyList();

    private volatile StateRouter<T> headStateRouter;

    private volatile List<StateRouter<T>> stateRouters;

    /**
     * Should continue route if current router's result is empty
     */
    private final boolean shouldFailFast;

    private final RouterSnapshotSwitcher routerSnapshotSwitcher;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 使用指定的路由器构造单一路由链。
     *
     * @param routers 来自 'router://' 规则的无状态路由器列表
     * @param stateRouters 维护路由状态的有状态路由器列表
     * @param shouldFailFast 如果为 true，当路由结果为空时立即停止路由
     * @param routerSnapshotSwitcher 控制是否捕获和存储路由快照的开关
     */
    public SingleRouterChain(
            List<Router> routers,
            List<StateRouter<T>> stateRouters,
            boolean shouldFailFast,
            RouterSnapshotSwitcher routerSnapshotSwitcher) {
        initWithRouters(routers);

        initWithStateRouters(stateRouters);

        this.shouldFailFast = shouldFailFast;
        this.routerSnapshotSwitcher = routerSnapshotSwitcher;
    }

    /**
     * 初始化有状态路由器链，通过反向链接将它们连接起来。
     * 列表中的最后一个路由器成为头部，每个路由器指向下一个。
     * 链路以 TailStateRouter 实例终止。
     *
     * @param stateRouters 要初始化的有状态路由器列表，按从先到后的顺序排列
     */
    private void initWithStateRouters(List<StateRouter<T>> stateRouters) {
        // 从尾部路由器开始构建链表
        StateRouter<T> stateRouter = TailStateRouter.getInstance();
        // 逆序遍历，将每个路由器的下一个指向当前路由器
        for (int i = stateRouters.size() - 1; i >= 0; i--) {
            StateRouter<T> nextStateRouter = stateRouters.get(i);
            nextStateRouter.setNextRouter(stateRouter);
            stateRouter = nextStateRouter;
        }
        // 设置头部路由器和不可修改的路由器列表
        this.headStateRouter = stateRouter;
        this.stateRouters = Collections.unmodifiableList(stateRouters);
    }

    /**
     * the resident routers must being initialized before address notification.
     * only for ut
     */
    public void initWithRouters(List<Router> builtinRouters) {
        this.builtinRouters = builtinRouters;
        this.routers = new LinkedList<>(builtinRouters);
    }

    /**
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     */
    public void addRouters(List<Router> routers) {
        // 创建新的路由器列表，包含内置路由器和新增路由器
        List<Router> newRouters = new LinkedList<>();
        newRouters.addAll(builtinRouters);
        newRouters.addAll(routers);
        // 对路由器进行排序
        CollectionUtils.sort(newRouters);
        this.routers = newRouters;
    }

    public List<Router> getRouters() {
        return routers;
    }

    public StateRouter<T> getHeadStateRouter() {
        return headStateRouter;
    }

    /**
     * 根据配置的路由规则对可用的调用者进行路由。
     * 检查调用者是否已更改，以防止使用过期数据进行路由。
     * 根据配置，执行简单路由或捕获详细快照。
     *
     * @param url 用于路由的消费者 URL
     * @param availableInvokers 当前可用的服务提供者列表
     * @param invocation 包含方法名和参数的 RPC 调用信息
     * @return 应用所有路由规则后过滤的调用者列表
     * @throws IllegalStateException 如果调用者自初始化以来已更改
     */
    public List<Invoker<T>> route(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // 验证调用者列表是否一致，防止使用过期数据
        if (invokers.getOriginList() != availableInvokers.getOriginList()) {
            logger.error(
                    INTERNAL_ERROR,
                    "",
                    "Router's invoker size: " + invokers.getOriginList().size() + " Invocation's invoker size: "
                            + availableInvokers.getOriginList().size(),
                    "Reject to route, because the invokers has changed.");
            throw new IllegalStateException("reject to route, because the invokers has changed.");
        }
        // 根据是否需要打印快照选择不同的路由策略
        if (RpcContext.getServiceContext().isNeedPrintRouterSnapshot()) {
            return routeAndPrint(url, availableInvokers, invocation);
        } else {
            return simpleRoute(url, availableInvokers, invocation);
        }
    }

    /**
     * 执行路由并捕获每个路由步骤的详细快照信息。
     * 构建完整的路由快照树，显示每个路由器的输入和输出，
     * 然后记录日志，无论结果是否为空。
     *
     * @param url 用于路由的消费者 URL
     * @param availableInvokers 当前可用的服务提供者列表
     * @param invocation 包含方法名和参数的 RPC 调用信息
     * @return 应用所有路由规则后过滤的调用者列表
     */
    public List<Invoker<T>> routeAndPrint(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // 构建路由快照树
        RouterSnapshotNode<T> snapshot = buildRouterSnapshot(url, availableInvokers, invocation);
        // 记录路由快照日志
        logRouterSnapshot(url, invocation, snapshot);
        return snapshot.getChainOutputInvokers();
    }

    /**
     * 执行路由，除非发生错误，否则不捕获详细快照。
     * 首先执行有状态路由器，然后按顺序执行普通路由器。
     * 如果结果为空且启用了快速失败，则提前停止。
     *
     * @param url 用于路由的消费者 URL
     * @param availableInvokers 当前可用的服务提供者列表
     * @param invocation 包含方法名和参数的 RPC 调用信息
     * @return 应用所有路由规则后过滤的调用者列表
     */
    public List<Invoker<T>> simpleRoute(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // 克隆可用的调用者列表
        BitList<Invoker<T>> resultInvokers = availableInvokers.clone();

        // 1. 执行有状态路由器路由
        resultInvokers = headStateRouter.route(resultInvokers, url, invocation, false, null);
        // 如果结果为空且需要快速失败或没有普通路由器，则打印快照并返回空列表
        if (resultInvokers.isEmpty() && (shouldFailFast || routers.isEmpty())) {
            printRouterSnapshot(url, availableInvokers, invocation);
            return BitList.emptyList();
        }

        // 如果没有普通路由器，直接返回有状态路由器的结果
        if (routers.isEmpty()) {
            return resultInvokers;
        }
        // 将结果转换为 ArrayList 以便普通路由器处理
        List<Invoker<T>> commonRouterResult = resultInvokers.cloneToArrayList();
        // 2. 执行普通路由器路由
        for (Router router : routers) {
            // Copy resultInvokers to a arrayList. BitList not support
            RouterResult<Invoker<T>> routeResult = router.route(commonRouterResult, url, invocation, false);
            commonRouterResult = routeResult.getResult();
            // 如果结果为空且需要快速失败，则打印快照并返回空列表
            if (CollectionUtils.isEmpty(commonRouterResult) && shouldFailFast) {
                printRouterSnapshot(url, availableInvokers, invocation);
                return BitList.emptyList();
            }

            // 如果不需要继续路由，则提前返回结果
            if (!routeResult.isNeedContinueRoute()) {
                return commonRouterResult;
            }
        }

        // 如果最终结果为空，打印快照并返回空列表
        if (commonRouterResult.isEmpty()) {
            printRouterSnapshot(url, availableInvokers, invocation);
            return BitList.emptyList();
        }

        return commonRouterResult;
    }

    /**
     * store each router's input and output, log out if empty
     */
    private void printRouterSnapshot(URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        if (logger.isWarnEnabled()) {
            logRouterSnapshot(url, invocation, buildRouterSnapshot(url, availableInvokers, invocation));
        }
    }

    /**
     * Build each router's result
     */
    public RouterSnapshotNode<T> buildRouterSnapshot(
            URL url, BitList<Invoker<T>> availableInvokers, Invocation invocation) {
        // 克隆可用的调用者列表
        BitList<Invoker<T>> resultInvokers = availableInvokers.clone();
        // 创建父节点，记录初始调用者列表
        RouterSnapshotNode<T> parentNode = new RouterSnapshotNode<>("Parent", resultInvokers.clone());
        parentNode.setNodeOutputInvokers(resultInvokers.clone());

        // 1. 执行有状态路由器路由，并构建快照节点
        Holder<RouterSnapshotNode<T>> nodeHolder = new Holder<>();
        nodeHolder.set(parentNode);

        resultInvokers = headStateRouter.route(resultInvokers, url, invocation, true, nodeHolder);

        // 如果结果为空或没有普通路由器且需要快速失败，则设置最终结果并返回
        if (routers.isEmpty() || (resultInvokers.isEmpty() && shouldFailFast)) {
            parentNode.setChainOutputInvokers(resultInvokers.clone());
            return parentNode;
        }

        // 创建普通路由器节点
        RouterSnapshotNode<T> commonRouterNode = new RouterSnapshotNode<>("CommonRouter", resultInvokers.clone());
        parentNode.appendNode(commonRouterNode);
        List<Invoker<T>> commonRouterResult = resultInvokers;

        // 2. 执行普通路由器路由
        for (Router router : routers) {
            // 将结果转换为 ArrayList，因为 BitList 不支持某些操作
            List<Invoker<T>> inputInvokers = new ArrayList<>(commonRouterResult);

            // 创建当前路由器的快照节点
            RouterSnapshotNode<T> currentNode =
                    new RouterSnapshotNode<>(router.getClass().getSimpleName(), inputInvokers);

            // 将当前节点追加到路由器节点链
            commonRouterNode.appendNode(currentNode);
            commonRouterNode = currentNode;

            // 执行路由器逻辑
            RouterResult<Invoker<T>> routeStateResult = router.route(inputInvokers, url, invocation, true);
            List<Invoker<T>> routeResult = routeStateResult.getResult();
            String routerMessage = routeStateResult.getMessage();

            // 设置当前节点的输出和消息
            currentNode.setNodeOutputInvokers(routeResult);
            currentNode.setRouterMessage(routerMessage);

            commonRouterResult = routeResult;

            // 如果结果为空且需要快速失败，则跳出循环
            if (CollectionUtils.isEmpty(routeResult) && shouldFailFast) {
                break;
            }

            // 如果不需要继续路由，则跳出循环
            if (!routeStateResult.isNeedContinueRoute()) {
                break;
            }
        }
        // 设置普通路由器节点的最终输出
        commonRouterNode.setChainOutputInvokers(commonRouterNode.getNodeOutputInvokers());

        // 3. 反向设置路由器链的输出，从子节点向父节点传递
        RouterSnapshotNode<T> currentNode = commonRouterNode;
        while (currentNode != null) {
            RouterSnapshotNode<T> parent = currentNode.getParentNode();
            if (parent != null) {
                // 普通路由器只有一个子节点，将子节点的输出设置为父节点的输出
                parent.setChainOutputInvokers(currentNode.getChainOutputInvokers());
            }
            currentNode = parent;
        }
        return parentNode;
    }

    /**
     * 记录路由快照，显示调用者流经每个路由器的情况。
     * 如果最终结果为空，则以警告级别记录；否则以信息级别记录。
     * 如果启用了快照开关，还会将快照消息存储到开关中。
     *
     * @param url 消费者 URL
     * @param invocation RPC 调用信息
     * @param snapshotNode 包含完整路由信息的快照节点
     */
    private void logRouterSnapshot(URL url, Invocation invocation, RouterSnapshotNode<T> snapshotNode) {
        // 判断最终结果是否为空
        if (snapshotNode.getChainOutputInvokers() == null
                || snapshotNode.getChainOutputInvokers().isEmpty()) {
            // 结果为空，以警告级别记录
            if (logger.isWarnEnabled()) {
                String message = "No provider available after route for the service " + url.getServiceKey()
                        + " from registry " + url.getAddress()
                        + " on the consumer " + NetUtils.getLocalHost()
                        + " using the dubbo version " + Version.getVersion() + ". Router snapshot is below: \n"
                        + snapshotNode.toString();
                if (routerSnapshotSwitcher.isEnable()) {
                    routerSnapshotSwitcher.setSnapshot(message);
                }
                logger.warn(
                        CLUSTER_NO_VALID_PROVIDER, "No provider available after route for the service", "", message);
            }
        } else {
            // 结果不为空，以信息级别记录
            if (logger.isInfoEnabled()) {
                String message = "Router snapshot service " + url.getServiceKey()
                        + " from registry " + url.getAddress()
                        + " on the consumer " + NetUtils.getLocalHost()
                        + " using the dubbo version " + Version.getVersion() + " is below: \n"
                        + snapshotNode.toString();
                if (routerSnapshotSwitcher.isEnable()) {
                    routerSnapshotSwitcher.setSnapshot(message);
                }
                logger.info(message);
            }
        }
    }

    /**
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(BitList<Invoker<T>> invokers) {
        // 设置调用者列表，如果为空则设置为空列表
        this.invokers = (invokers == null ? BitList.emptyList() : invokers);
        // 通知所有普通路由器调用者列表已更新
        routers.forEach(router -> router.notify(this.invokers));
        // 通知所有有状态路由器调用者列表已更新
        stateRouters.forEach(router -> router.notify(this.invokers));
    }

    /**
     * for uts only
     */
    @Deprecated
    public void setHeadStateRouter(StateRouter<T> headStateRouter) {
        this.headStateRouter = headStateRouter;
    }

    /**
     * for uts only
     */
    @Deprecated
    public List<StateRouter<T>> getStateRouters() {
        return stateRouters;
    }

    public ReadWriteLock getLock() {
        return lock;
    }

    /**
     * 销毁路由链，清理所有资源。
     * 清空调用者列表，并停止所有路由器（包括普通路由器和有状态路由器）。
     * 如果停止过程中发生异常，会记录错误日志但继续处理其他路由器。
     */
    public void destroy() {
        // 清空调用者列表
        invokers = BitList.emptyList();
        // 停止所有普通路由器
        for (Router router : routers) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error(
                        CLUSTER_FAILED_STOP,
                        "route stop failed",
                        "",
                        "Error trying to stop router " + router.getClass(),
                        e);
            }
        }
        // 清空普通路由器列表
        routers = Collections.emptyList();
        builtinRouters = Collections.emptyList();

        // 停止所有有状态路由器
        for (StateRouter<T> router : stateRouters) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error(
                        CLUSTER_FAILED_STOP,
                        "StateRouter stop failed",
                        "",
                        "Error trying to stop StateRouter " + router.getClass(),
                        e);
            }
        }
        // 清空有状态路由器列表，重置头部路由器
        stateRouters = Collections.emptyList();
        headStateRouter = TailStateRouter.getInstance();
    }
}
