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
package org.apache.dubbo.rpc.cluster.filter.support;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.metrics.collector.DefaultMetricsCollector;
import org.apache.dubbo.metrics.event.MetricsDispatcher;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.event.RequestEvent;
import org.apache.dubbo.metrics.model.MethodMetric;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;

@Activate(group = CONSUMER, onClass = "org.apache.dubbo.metrics.collector.DefaultMetricsCollector")
public class MetricsClusterFilter implements ClusterFilter, BaseFilter.Listener, ScopeModelAware {

    private ApplicationModel applicationModel;
    private DefaultMetricsCollector collector;
    private String appName;
    private MetricsDispatcher metricsDispatcher;
    private boolean serviceLevel;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        this.collector = applicationModel.getBeanFactory().getBean(DefaultMetricsCollector.class);
        this.appName = applicationModel.tryGetApplicationName();
        this.metricsDispatcher = applicationModel.getBeanFactory().getBean(MetricsDispatcher.class);
        this.serviceLevel = MethodMetric.isServiceLevel(applicationModel);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        handleMethodException(result.getException(), invocation);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        handleMethodException(t, invocation);
    }

    /**
     * 处理调用过程中的异常并上报特定的监控事件，重点关注权限拒绝类错误。
     * <p>
     * 该方法在捕获到远程调用异常时被调用。它首先检查指标采集器是否处于启用状态。
     * 如果异常类型为 {@link RpcException} 且属于禁止访问（Forbidden）错误，
     * 则构建一个包含应用信息、调用上下文及错误码的请求错误事件（RequestErrorEvent），
     * 并通过 MetricsEventBus 发布该事件，以便监控系统记录服务调用的失败原因和分布情况。
     * </p>
     *
     * @param t 调用过程中捕获的原始异常对象
     * @param invocation 当前的 RPC 调用对象，用于提取服务名、方法名等元数据
     */
    private void handleMethodException(Throwable t, Invocation invocation) {
        if (collector == null || !collector.isCollectEnabled()) {
            return;
        }
        if (t instanceof RpcException) {
            RpcException e = (RpcException) t;
            /*
             * 针对权限拒绝类错误（Forbidden）发布请求异常监控事件
             */
            if (e.isForbidden()) {
                MetricsEventBus.publish(RequestEvent.toRequestErrorEvent(
                        applicationModel,
                        appName,
                        metricsDispatcher,
                        invocation,
                        CONSUMER_SIDE,
                        e.getCode(),
                        serviceLevel));
            }
        }
    }
}
