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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.serialize.SerializationException;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.FutureContext;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PAYLOAD;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

/**
 * DubboInvoker
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    private final ClientsProvider clientsProvider;

    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    private final ReentrantLock destroyLock = new ReentrantLock();

    private final Set<Invoker<?>> invokers;

    private final int serverShutdownTimeout;

    private static final boolean setFutureWhenSync = Boolean.parseBoolean(SystemPropertyConfigUtils.getSystemProperty(
            CommonConstants.ThirdPartyProperty.SET_FUTURE_IN_SYNC_MODE, "true"));

    public DubboInvoker(Class<T> serviceType, URL url, ClientsProvider clientsProvider) {
        this(serviceType, url, clientsProvider, null);
    }

    public DubboInvoker(Class<T> serviceType, URL url, ClientsProvider clientsProvider, Set<Invoker<?>> invokers) {
        super(serviceType, url, new String[] {INTERFACE_KEY, GROUP_KEY, TOKEN_KEY});
        this.clientsProvider = clientsProvider;
        this.invokers = invokers;
        this.serverShutdownTimeout = ConfigurationUtils.getServerShutdownTimeout(getUrl().getScopeModel());
    }

    /**
     * 执行Dubbo协议的RPC调用，处理单向和双向通信模式。
     * <p>
     * 该方法的处理流程：
     * 1. 设置调用的基本附件信息（服务路径、版本号）；
     * 2. 从客户端列表中选择ExchangeClient（单客户端直接使用，多客户端通过轮询负载均衡）；
     * 3. 计算超时时间并检查是否已超时，超时时直接返回异常结果；
     * 4. 创建Request对象并设置数据内容和payload限制；
     * 5. 根据是否为单向调用分别处理：
     *    - 单向调用：直接发送请求不等待响应；
     *    - 双向调用：发送请求并等待异步响应，封装为AsyncRpcResult；
     * 6. 捕获并转换异常类型（超时、网络、序列化等）。
     * </p>
     *
     * @param invocation RPC调用信息，包含方法名、参数类型、参数值等
     * @return Result 调用结果，可能是单向的默认结果或双向的异步结果
     * @throws Throwable 当远程调用失败、超时或发生网络异常时抛出
     */
    @Override
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        final String methodName = RpcUtils.getMethodName(invocation);
        // 设置调用的基本附件信息：服务路径和版本号，供服务端识别和路由
        inv.setAttachment(PATH_KEY, getUrl().getPath());
        inv.setAttachment(VERSION_KEY, version);

        ExchangeClient currentClient;
        List<? extends ExchangeClient> exchangeClients = clientsProvider.getClients();
        // 选择ExchangeClient：单客户端直接使用，多客户端通过原子计数器实现轮询负载均衡
        if (exchangeClients.size() == 1) {
            currentClient = exchangeClients.get(0);
        } else {
            currentClient = exchangeClients.get(index.getAndIncrement() % exchangeClients.size());
        }

        // 设置本地地址到RpcContext，供业务逻辑获取当前使用的网络连接信息
        RpcContext.getServiceContext().setLocalAddress(currentClient.getLocalAddress());

        try {
            // 判断是否为单向调用（不需要返回结果的方法）
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);

            // 计算本次调用的超时时间，如果超时时间<=0说明已无剩余时间，直接返回超时异常
            int timeout = RpcUtils.calculateTimeout(getUrl(), invocation, methodName, DEFAULT_TIMEOUT);
            if (timeout <= 0) {
                return AsyncRpcResult.newDefaultAsyncResult(
                        new RpcException(
                                RpcException.TIMEOUT_TERMINATE,
                                "No time left for making the following call: " + invocation.getServiceName() + "."
                                        + RpcUtils.getMethodName(invocation) + ", terminate directly."),
                        invocation);
            }

            // 将超时时间设置到invocation附件中，供后续环节使用
            invocation.setAttachment(TIMEOUT_KEY, String.valueOf(timeout));

            // 获取payload限制参数，用于控制序列化后的数据大小
            Integer payload = getUrl().getParameter(PAYLOAD, Integer.class);

            // 创建Request对象并设置必要属性：payload限制、调用数据、协议版本
            Request request = new Request();
            if (payload != null) {
                request.setPayload(payload);
            }
            request.setData(inv);
            request.setVersion(Version.getProtocolVersion());

            // 根据是否为单向调用采取不同的处理策略
            if (isOneway) {
                // 单向调用：获取sent标识（是否需要确认发送成功），设置为false表示不需要响应
                boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                request.setTwoWay(false);
                currentClient.send(request, isSent);
                // 返回默认的异步结果（空结果），因为是单向调用无需等待响应
                return AsyncRpcResult.newDefaultAsyncResult(invocation);
            } else {
                // 双向调用：设置为true表示需要返回响应结果
                request.setTwoWay(true);
                // 获取回调执行器，用于处理响应到达后的异步回调逻辑
                ExecutorService executor = getCallbackExecutor(getUrl(), inv);
                // 发送请求并获取异步响应Future，转换为AppResponse类型
                CompletableFuture<AppResponse> appResponseFuture =
                        currentClient.request(request, timeout, executor).thenApply(AppResponse.class::cast);

                // 为了2.6.x版本的兼容性而设置Future上下文（如Zipkin的TraceFilter使用FutureAdapter）
                if (setFutureWhenSync || ((RpcInvocation) invocation).getInvokeMode() != InvokeMode.SYNC) {
                    FutureContext.getContext().setCompatibleFuture(appResponseFuture);
                }

                // 封装为AsyncRpcResult并设置执行器，返回异步调用结果
                AsyncRpcResult result = new AsyncRpcResult(appResponseFuture, inv);
                result.setExecutor(executor);
                return result;
            }
        } catch (TimeoutException e) {
            // 超时异常：转换为RpcException并附加详细的调用信息
            throw new RpcException(
                    RpcException.TIMEOUT_EXCEPTION,
                    "Invoke remote method timeout. method: " + RpcUtils.getMethodName(invocation) + ", provider: "
                            + getUrl() + ", cause: " + e.getMessage(),
                    e);
        } catch (RemotingException e) {
            // 远程调用异常：根据根本原因转换为不同类型的RpcException
            String remoteExpMsg = "Failed to invoke remote method: " + RpcUtils.getMethodName(invocation)
                    + ", provider: " + getUrl() + ", cause: " + e.getMessage();
            if (e.getCause() instanceof IOException && e.getCause().getCause() instanceof SerializationException) {
                // 序列化异常：单独分类以便排查数据类型不匹配问题
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, remoteExpMsg, e);
            } else {
                // 网络异常：包括连接断开、超时等底层通信问题
                throw new RpcException(RpcException.NETWORK_EXCEPTION, remoteExpMsg, e);
            }
        }
    }

    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        for (ExchangeClient client : clientsProvider.getClients()) {
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)) {
                // cannot write == not Available ?
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (!super.isDestroyed()) {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                if (invokers != null) {
                    invokers.remove(this);
                }
                clientsProvider.close(ConfigurationUtils.reCalShutdownTime(serverShutdownTimeout));
            } finally {
                destroyLock.unlock();
            }
        }
    }
}
