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
package org.apache.dubbo.rpc.cluster.support.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.support.MockInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.List;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CLUSTER_FAILED_MOCK_REQUEST;
import static org.apache.dubbo.rpc.Constants.MOCK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.FORCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.INVOCATION_NEED_MOCK;

/**
 * 具备服务降级（Mock）能力的集群调用器包装类
 * 当远程调用失败或配置了强制 Mock 时，通过本地模拟逻辑返回预设结果，提高系统的容错性和可用性
 *
 * @param <T> 服务接口类型
 */
public class MockClusterInvoker<T> implements ClusterInvoker<T> {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(MockClusterInvoker.class);

    /** 控制同步模式下是否设置 Future 的系统属性，默认为 true */
    private static final boolean setFutureWhenSync = Boolean.parseBoolean(SystemPropertyConfigUtils.getSystemProperty(
            CommonConstants.ThirdPartyProperty.SET_FUTURE_IN_SYNC_MODE, "true"));

    /** 服务目录，用于获取服务列表和配置信息 */
    private final Directory<T> directory;

    /** 底层的真实集群调用器，负责执行正常的 RPC 调用 */
    private final Invoker<T> invoker;

    public MockClusterInvoker(Directory<T> directory, Invoker<T> invoker) {
        this.directory = directory;
        this.invoker = invoker;
    }

    @Override
    public URL getUrl() {
        return directory.getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return directory.getUrl();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public boolean isDestroyed() {
        return directory.isDestroyed();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        this.invoker.destroy();
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    /**
     * 执行带有服务降级逻辑的 RPC 调用
     * 根据配置的 Mock 策略（force/fail）决定是直接返回 Mock 结果还是在调用失败后尝试 Mock
     *
     * @param invocation 调用上下文，包含方法名、参数及附件信息
     * @return 调用结果，可能是远程调用的真实结果，也可能是本地 Mock 的结果
     * @throws RpcException 当发生业务异常或不可恢复的系统错误时抛出
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        Result result;

        // 获取当前方法的 Mock 配置值，默认为 false
        String value = getUrl().getMethodParameter(
                        RpcUtils.getMethodName(invocation), MOCK_KEY, Boolean.FALSE.toString())
                .trim();

        if (ConfigUtils.isEmpty(value)) {
            // 未配置 Mock，直接执行正常的远程调用
            result = this.invoker.invoke(invocation);
        } else if (value.startsWith(FORCE_KEY)) {
            // 配置了 force:xxx，表示强制走 Mock 逻辑，不发起远程调用
            if (logger.isWarnEnabled()) {
                logger.warn(
                        CLUSTER_FAILED_MOCK_REQUEST,
                        "force mock",
                        "",
                        "force-mock: " + RpcUtils.getMethodName(invocation) + " force-mock enabled , url : "
                                + getUrl());
            }
            // force:direct mock
            result = doMockInvoke(invocation, null);
        } else {
            // 配置了 fail:xxx 或其他值，表示在调用失败时尝试 Mock
            try {
                result = this.invoker.invoke(invocation);

                // fix:#4585
                // 如果远程调用返回了非业务类的 RpcException，则触发 Mock 逻辑
                if (result.getException() != null && result.getException() instanceof RpcException) {
                    RpcException rpcException = (RpcException) result.getException();
                    if (rpcException.isBiz()) {
                        // 业务异常直接抛出，不走 Mock
                        throw rpcException;
                    } else {
                        result = doMockInvoke(invocation, rpcException);
                    }
                }

            } catch (RpcException e) {
                if (e.isBiz()) {
                    // 业务异常直接抛出，不走 Mock
                    throw e;
                }

                if (logger.isWarnEnabled()) {
                    logger.warn(
                            CLUSTER_FAILED_MOCK_REQUEST,
                            "failed to mock invoke",
                            "",
                            "fail-mock: " + RpcUtils.getMethodName(invocation) + " fail-mock enabled , url : "
                                    + getUrl(),
                            e);
                }
                // 系统级异常触发 Mock 逻辑
                result = doMockInvoke(invocation, e);
            }
        }
        return result;
    }

    /**
     * 执行具体的 Mock 调用逻辑
     * 从目录中查找专门的 Mock Invoker，如果没有则动态创建一个默认的 MockInvoker
     *
     * @param invocation 调用上下文
     * @param e 触发 Mock 的原始异常，如果为 null 则表示是强制 Mock
     * @return Mock 执行后的结果对象
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Result doMockInvoke(Invocation invocation, RpcException e) {
        Result result;
        Invoker<T> mockInvoker;

        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        // 设置调用模式，确保后续处理能正确识别是同步还是异步
        rpcInvocation.setInvokeMode(RpcUtils.getInvokeMode(getUrl(), invocation));

        // 从目录中筛选出标记为 Mock 的 Invoker
        List<Invoker<T>> mockInvokers = selectMockInvoker(invocation);
        if (CollectionUtils.isEmpty(mockInvokers)) {
            // 如果没有找到专门的 Mock Invoker，则使用默认的通用 Mock 实现
            mockInvoker = (Invoker<T>) new MockInvoker(getUrl(), directory.getInterface());
        } else {
            mockInvoker = mockInvokers.get(0);
        }
        try {
            result = mockInvoker.invoke(invocation);
        } catch (RpcException mockException) {
            if (mockException.isBiz()) {
                // 如果 Mock 内部抛出了业务异常，将其封装为异步结果返回
                result = AsyncRpcResult.newDefaultAsyncResult(mockException.getCause(), invocation);
            } else {
                // 如果 Mock 内部发生系统错误，则抛出包含原始异常信息的 RpcException
                throw new RpcException(
                        mockException.getCode(), getMockExceptionMessage(e, mockException), mockException.getCause());
            }
        } catch (Throwable me) {
            // 捕获其他未知异常并包装抛出
            throw new RpcException(getMockExceptionMessage(e, me), me.getCause());
        }

        // 根据配置或调用模式决定是否将响应 Future 设置到 RpcContext 中
        if (setFutureWhenSync || rpcInvocation.getInvokeMode() != InvokeMode.SYNC) {
            // set server context
            RpcContext.getServiceContext()
                    .setFuture(new FutureAdapter<>(((AsyncRpcResult) result).getResponseFuture()));
        }
        return result;
    }

    /**
     * 组装 Mock 异常与原始调用异常的错误信息
     *
     * @param t 原始调用异常
     * @param mt Mock 执行过程中的异常
     * @return 组合后的错误描述字符串
     */
    private String getMockExceptionMessage(Throwable t, Throwable mt) {
        String msg = "mock error : " + mt.getMessage();
        if (t != null) {
            msg = msg + ", invoke error is :" + StringUtils.toString(t);
        }
        return msg;
    }

    /**
     * Return MockInvoker
     * Contract：
     * directory.list() will return a list of normal invokers if Constants.INVOCATION_NEED_MOCK is absent or not true in invocation, otherwise, a list of mock invokers will return.
     * if directory.list() returns more than one mock invoker, only one of them will be used.
     *
     * @param invocation 调用上下文
     * @return Mock Invoker 列表，如果未找到则返回 null
     */
    private List<Invoker<T>> selectMockInvoker(Invocation invocation) {
        List<Invoker<T>> invokers = null;
        // TODO generic invoker？
        if (invocation instanceof RpcInvocation) {
            // Note the implicit contract (although the description is added to the interface declaration, but
            // extensibility is a problem. The practice placed in the attachment needs to be improved)
            // 通过在附件中标记 INVOCATION_NEED_MOCK，告知 Directory 返回专门用于 Mock 的 Invoker 列表
            invocation.setAttachment(INVOCATION_NEED_MOCK, Boolean.TRUE.toString());
            // directory will return a list of normal invokers if Constants.INVOCATION_NEED_MOCK is absent or not true
            // in invocation, otherwise, a list of mock invokers will return.
            try {
                RpcContext.getServiceContext().setConsumerUrl(getUrl());
                invokers = directory.list(invocation);
            } catch (RpcException e) {
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "Exception when try to invoke mock. Get mock invokers error for service:"
                                    + getUrl().getServiceInterface() + ", method:" + RpcUtils.getMethodName(invocation)
                                    + ", will construct a new mock with 'new MockInvoker()'.",
                            e);
                }
            }
        }
        return invokers;
    }

    @Override
    public String toString() {
        return "invoker :" + this.invoker + ",directory: " + this.directory;
    }
}
