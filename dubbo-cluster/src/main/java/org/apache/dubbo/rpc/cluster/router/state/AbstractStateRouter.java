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
package org.apache.dubbo.rpc.cluster.router.state;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotNode;
import org.apache.dubbo.rpc.model.ModuleModel;

/***
 * The abstract class of StateRoute.
 * @since 3.0
 */
/**
 * 抽象有状态路由器基类，实现了 StateRouter 接口的核心逻辑。
 * 该类提供了路由链的模板方法模式实现，负责：
 * 1. 管理路由器的链接关系（nextRouter）
 * 2. 控制路由快照的构建和记录
 * 3. 协调当前路由器与下一个路由器的执行流程
 * 4. 提供快速失败机制的配置支持
 *
 * @param <T> 服务类型
 */
public abstract class AbstractStateRouter<T> implements StateRouter<T> {
    private volatile boolean force = false;
    private volatile URL url;
    private volatile StateRouter<T> nextRouter = null;

    private final GovernanceRuleRepository ruleRepository;

    /**
     * Should continue route if current router's result is empty
     */
    private final boolean shouldFailFast;

    protected ModuleModel moduleModel;

    /**
     * 构造抽象有状态路由器实例。
     *
     * @param url 消费者 URL，用于获取模块模型和配置信息
     */
    public AbstractStateRouter(URL url) {
        // 获取模块模型并初始化规则仓库
        moduleModel = url.getOrDefaultModuleModel();
        this.ruleRepository =
                moduleModel.getExtensionLoader(GovernanceRuleRepository.class).getDefaultExtension();
        this.url = url;
        // 从配置中读取是否启用快速失败机制，默认为 true
        this.shouldFailFast = Boolean.parseBoolean(
                ConfigurationUtils.getProperty(moduleModel, Constants.SHOULD_FAIL_FAST_KEY, "true"));
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public boolean isRuntime() {
        return true;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public GovernanceRuleRepository getRuleRepository() {
        return this.ruleRepository;
    }

    public StateRouter<T> getNextRouter() {
        return nextRouter;
    }

    @Override
    public void notify(BitList<Invoker<T>> invokers) {
        // default empty implement
    }

    /**
     * 执行路由逻辑的模板方法。
     * 该方法定义了路由执行的完整流程：
     * 1. 如果需要打印消息，则构建当前路由器对应的快照节点
     * 2. 调用子类实现的 doRoute 方法执行实际的路由逻辑
     * 3. 将路由结果与原始调用者列表进行交集运算
     * 4. 如果路由器不支持自行继续路由，则调用下一个路由器
     * 5. 完成后更新快照节点的输出信息
     *
     * @param invokers 待路由的调用者列表
     * @param url 消费者 URL
     * @param invocation RPC 调用信息
     * @param needToPrintMessage 是否需要打印路由快照消息
     * @param nodeHolder 用于传递和返回路由快照节点的持有器
     * @return 路由后的调用者列表
     * @throws RpcException 路由过程中发生的异常
     */
    @Override
    public final BitList<Invoker<T>> route(
            BitList<Invoker<T>> invokers,
            URL url,
            Invocation invocation,
            boolean needToPrintMessage,
            Holder<RouterSnapshotNode<T>> nodeHolder)
            throws RpcException {
        // 验证打印消息的参数有效性
        if (needToPrintMessage && (nodeHolder == null || nodeHolder.get() == null)) {
            needToPrintMessage = false;
        }

        RouterSnapshotNode<T> currentNode = null;
        RouterSnapshotNode<T> parentNode = null;
        Holder<String> messageHolder = null;

        // 如果需要打印消息，则预先构建当前节点和父节点的关系
        if (needToPrintMessage) {
            parentNode = nodeHolder.get();
            currentNode = new RouterSnapshotNode<>(this.getClass().getSimpleName(), invokers.clone());
            parentNode.appendNode(currentNode);

            // 在第一个子节点调用时设置父节点的输出大小
            // 初始节点输出大小为零，第一个子节点会覆盖它
            if (parentNode.getNodeOutputSize() < invokers.size()) {
                parentNode.setNodeOutputInvokers(invokers.clone());
            }

            messageHolder = new Holder<>();
            nodeHolder.set(currentNode);
        }
        BitList<Invoker<T>> routeResult;

        // 调用子类实现的具体路由逻辑
        routeResult = doRoute(invokers, url, invocation, needToPrintMessage, nodeHolder, messageHolder);
        // 将路由结果与原始调用者列表取交集，确保结果的正确性
        if (routeResult != invokers) {
            routeResult = invokers.and(routeResult);
        }
        // 检查路由器是否支持自行调用继续路由
        if (!supportContinueRoute()) {
            // 使用当前节点的结果作为下一个节点的参数
            if (!shouldFailFast || !routeResult.isEmpty()) {
                routeResult = continueRoute(routeResult, url, invocation, needToPrintMessage, nodeHolder);
            }
        }

        // 如果需要打印消息，则后置处理当前节点的快照信息
        if (needToPrintMessage) {
            currentNode.setRouterMessage(messageHolder.get());
            if (currentNode.getNodeOutputSize() == 0) {
                // 没有子节点调用，设置当前节点的输出
                currentNode.setNodeOutputInvokers(routeResult.clone());
            }
            currentNode.setChainOutputInvokers(routeResult.clone());
            nodeHolder.set(parentNode);
        }
        return routeResult;
    }

    /**
     * 使用当前路由规则过滤调用者，只返回符合规则的调用者。
     * 子类必须实现此方法来定义具体的路由逻辑。
     *
     * @param invokers 待路由的所有调用者
     * @param url 消费者 URL
     * @param invocation RPC 调用信息
     * @param needToPrintMessage 当前路由器是否需要打印消息
     * @param nodeHolder 路由快照节点持有器，一般路由器本身无需关心此参数，只需传递给 continueRoute
     * @param messageHolder 当路由器需要打印消息时的消息持有器
     * @return 路由后的结果
     * @throws RpcException 路由过程中发生的异常
     */
    protected abstract BitList<Invoker<T>> doRoute(
            BitList<Invoker<T>> invokers,
            URL url,
            Invocation invocation,
            boolean needToPrintMessage,
            Holder<RouterSnapshotNode<T>> nodeHolder,
            Holder<String> messageHolder)
            throws RpcException;

    /**
     * 调用下一个路由器获取结果。
     * 如果存在下一个路由器，则将当前路由结果传递给下一个路由器继续处理；
     * 否则直接返回当前的调用者列表。
     *
     * @param invokers 当前路由器过滤后的调用者列表
     * @param url 消费者 URL
     * @param invocation RPC 调用信息
     * @param needToPrintMessage 是否需要打印消息
     * @param nodeHolder 路由快照节点持有器
     * @return 继续路由后的调用者列表
     */
    protected final BitList<Invoker<T>> continueRoute(
            BitList<Invoker<T>> invokers,
            URL url,
            Invocation invocation,
            boolean needToPrintMessage,
            Holder<RouterSnapshotNode<T>> nodeHolder) {
        // 如果存在下一个路由器，则调用其 route 方法
        if (nextRouter != null) {
            return nextRouter.route(invokers, url, invocation, needToPrintMessage, nodeHolder);
        } else {
            // 否则直接返回当前的调用者列表
            return invokers;
        }
    }

    /**
     * 判断当前路由器的实现是否支持自行调用继续路由。
     * 如果返回 false，则由 AbstractStateRouter 的 route 方法负责调用 continueRoute；
     * 如果返回 true，则子类需要在 doRoute 中自行处理继续路由的逻辑。
     *
     * @return 是否支持自行继续路由，默认为 false
     */
    protected boolean supportContinueRoute() {
        return false;
    }

    /**
     * 设置下一个路由器节点。
     * 该方法由 AbstractStateRouter 维护，不允许子类重写。
     * 如果指定的路由器想要控制继续路由的行为，
     * 请重写 {@link AbstractStateRouter#supportContinueRoute()} 方法。
     *
     * @param nextRouter 下一个路由器实例
     */
    @Override
    public final void setNextRouter(StateRouter<T> nextRouter) {
        this.nextRouter = nextRouter;
    }

    @Override
    public final String buildSnapshot() {
        return doBuildSnapshot() + "            v \n" + nextRouter.buildSnapshot();
    }

    /**
     * 构建当前路由器的快照字符串表示。
     * 默认实现表明当前路由器不支持快照功能。
     * 子类可以重写此方法以提供自定义的快照信息。
     *
     * @return 当前路由器的快照字符串
     */
    protected String doBuildSnapshot() {
        return this.getClass().getSimpleName() + " not support\n";
    }
}
