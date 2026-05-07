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
package org.apache.dubbo.rpc.cluster.router;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.FrameworkModel;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;

@Activate(group = {CONSUMER})
public class RouterSnapshotFilter implements ClusterFilter, BaseFilter.Listener {

    private final RouterSnapshotSwitcher switcher;
    private static final Logger logger = LoggerFactory.getLogger(RouterSnapshotFilter.class);

    public RouterSnapshotFilter(FrameworkModel frameworkModel) {
        this.switcher = frameworkModel.getBeanFactory().getBean(RouterSnapshotSwitcher.class);
    }

    /**
     * 在RPC调用前检查路由快照打印开关，并在满足条件时标记当前调用需要输出路由快照日志。
     * <p>
     * 该过滤器用于调试和监控路由决策过程。它通过三层检查来决定是否开启路由快照功能：
     * <ol>
     *   <li>全局开关检查：如果路由快照功能整体未启用，则直接跳过</li>
     *   <li>日志级别检查：如果INFO级别日志未开启，则无需执行后续逻辑以减少开销</li>
     *   <li>服务级开关检查：根据当前服务的ServiceKey判断是否针对该特定服务启用了快照功能</li>
     * </ol>
     * 只有当所有检查都通过时，才会在RpcContext中设置needPrintRouterSnapshot标志位，
     * 通知后续的路由组件在执行完路由逻辑后输出当前的路由状态快照。
     * </p>
     *
     * @param invoker 当前调用的执行器，代表远程服务的代理
     * @param invocation 封装了服务模型、方法名及参数的调用对象
     * @return 远程调用的执行结果
     * @throws RpcException 当调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if (!switcher.isEnable()) {
            return invoker.invoke(invocation);
        }

        if (!logger.isInfoEnabled()) {
            return invoker.invoke(invocation);
        }

        if (!switcher.isEnable(invocation.getServiceModel().getServiceKey())) {
            return invoker.invoke(invocation);
        }

        /*
         * 标记当前调用需要在执行结束后打印路由选择过程的快照信息
         */
        RpcContext.getServiceContext().setNeedPrintRouterSnapshot(true);
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        RpcContext.getServiceContext().setNeedPrintRouterSnapshot(false);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        RpcContext.getServiceContext().setNeedPrintRouterSnapshot(false);
    }
}
