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
package org.apache.dubbo.metrics.filter;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.metrics.collector.DefaultMetricsCollector;
import org.apache.dubbo.metrics.event.MetricsDispatcher;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.event.RequestEvent;
import org.apache.dubbo.metrics.model.MethodMetric;
import org.apache.dubbo.metrics.model.MetricsSupport;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.metrics.DefaultConstants.METRIC_FILTER_EVENT;
import static org.apache.dubbo.metrics.DefaultConstants.METRIC_THROWABLE;

public class MetricsFilter implements ScopeModelAware {

    private ApplicationModel applicationModel;
    private static final ErrorTypeAwareLogger LOGGER = LoggerFactory.getErrorTypeAwareLogger(MetricsFilter.class);
    private boolean rpcMetricsEnable;
    private String appName;
    private MetricsDispatcher metricsDispatcher;
    private DefaultMetricsCollector defaultMetricsCollector;
    private boolean serviceLevel;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        this.rpcMetricsEnable = applicationModel
                .getApplicationConfigManager()
                .getMetrics()
                .map(MetricsConfig::getEnableRpc)
                .orElse(false);
        this.appName = applicationModel.tryGetApplicationName();
        this.metricsDispatcher = applicationModel.getBeanFactory().getBean(MetricsDispatcher.class);
        this.defaultMetricsCollector = applicationModel.getBeanFactory().getBean(DefaultMetricsCollector.class);
        serviceLevel = MethodMetric.isServiceLevel(applicationModel);
    }

    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoke(invoker, invocation, PROVIDER.equals(MetricsSupport.getSide(invocation)));
    }

    /**
     * 在RPC调用执行前采集监控指标，记录请求开始事件并初始化上下文信息。
     * <p>
     * 该方法是Dubbo监控体系的核心入口点之一，负责在调用链路的最前端捕获请求信息。
     * 如果启用了RPC监控功能（rpcMetricsEnable=true），它会构建一个RequestEvent对象，
     * 其中包含应用模型、应用名称、调用类型（提供者或消费者）以及服务层级等元数据。
     * 随后通过MetricsEventBus.before()触发“调用前”的指标统计（如QPS、并发数等），
     * 并将该事件对象存入Invocation上下文中，以便在调用结束后关联后续的耗时和结果统计。
     * </p>
     * <p>
     * 异常处理：监控逻辑的执行被包裹在try-catch中，确保即使指标采集发生异常，
     * 也不会影响核心业务调用的正常进行，仅记录警告日志。
     * </p>
     *
     * @param invoker 当前调用的执行器，代表远程服务的代理
     * @param invocation 封装了方法名、参数及附件的调用对象
     * @param isProvider 标识当前是服务端（true）还是客户端（false）视角
     * @return 远程调用的执行结果
     * @throws RpcException 当底层调用发生异常时抛出
     */
    public Result invoke(Invoker<?> invoker, Invocation invocation, boolean isProvider) throws RpcException {
        if (rpcMetricsEnable) {
            try {
                /*
                 * 构建请求事件并触发调用前的指标统计逻辑
                 */
                RequestEvent requestEvent = RequestEvent.toRequestEvent(
                        applicationModel,
                        appName,
                        metricsDispatcher,
                        defaultMetricsCollector,
                        invocation,
                        isProvider ? PROVIDER : CONSUMER,
                        serviceLevel);
                MetricsEventBus.before(requestEvent);
                /*
                 * 将事件对象存入上下文，供调用结束后的后置处理使用
                 */
                invocation.put(METRIC_FILTER_EVENT, requestEvent);
            } catch (Throwable t) {
                LOGGER.warn(INTERNAL_ERROR, "", "", "Error occurred when invoke.", t);
            }
        }
        return invoker.invoke(invocation);
    }

    public void onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        if (rpcMetricsEnable) {
            onResponse(result, invoker, invocation, PROVIDER.equals(MetricsSupport.getSide(invocation)));
        }
    }

    public void onResponse(Result result, Invoker<?> invoker, Invocation invocation, boolean isProvider) {
        Object eventObj = invocation.get(METRIC_FILTER_EVENT);
        if (eventObj != null) {
            try {
                MetricsEventBus.after((RequestEvent) eventObj, result);
            } catch (Throwable t) {
                LOGGER.warn(INTERNAL_ERROR, "", "", "Error occurred when onResponse.", t);
            }
        }
    }

    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (rpcMetricsEnable) {
            onError(t, invoker, invocation, PROVIDER.equals(MetricsSupport.getSide(invocation)));
        }
    }

    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation, boolean isProvider) {
        Object eventObj = invocation.get(METRIC_FILTER_EVENT);
        if (eventObj != null) {
            try {
                RequestEvent requestEvent = (RequestEvent) eventObj;
                requestEvent.putAttachment(METRIC_THROWABLE, t);
                MetricsEventBus.error(requestEvent);
            } catch (Throwable throwable) {
                LOGGER.warn(INTERNAL_ERROR, "", "", "Error occurred when onResponse.", throwable);
            }
        }
    }
}
