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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.Node;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.SerializationException;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.utils.UrlUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_VERSION;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_REQUEST;
import static org.apache.dubbo.rpc.Constants.SERIALIZATION_ID_KEY;

/**
 * Invoker 接口的抽象基类实现，提供了 RPC 调用的通用逻辑。
 * 该类封装了服务接口类型、URL 地址、附件信息以及生命周期管理（可用性与销毁状态）。
 *
 * 核心职责包括：
 * 1. 准备调用上下文：设置调用模式、序列化 ID、附加参数等。
 * 2. 执行远程调用：委托给子类实现的 doInvoke 方法。
 * 3. 处理调用结果：根据同步或异步模式决定是否等待结果返回，并统一处理异常。
 *
 * @param <T> 服务接口类型
 */
public abstract class AbstractInvoker<T> implements Invoker<T> {

    protected static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(AbstractInvoker.class);

    /**
     * Service interface type
     * 服务接口类型
     */
    private final Class<T> type;

    /**
     * {@link Node} url
     * 服务提供者的 URL 地址
     */
    private final URL url;

    /**
     * {@link Invoker} default attachment
     * 默认的附加参数映射
     */
    private final Map<String, Object> attachment;

    protected final String version;

    /**
     * {@link Node} available
     * 节点是否可用
     */
    private volatile boolean available = true;

    /**
     * {@link Node} destroy
     * 节点是否已销毁
     */
    private volatile boolean destroyed = false;

    /**
     * Whether set future to Thread Local when invocation mode is sync
     * 在同步调用模式下，是否将 Future 设置到 ThreadLocal 中
     */
    private static final boolean setFutureWhenSync = Boolean.parseBoolean(SystemPropertyConfigUtils.getSystemProperty(
            CommonConstants.ThirdPartyProperty.SET_FUTURE_IN_SYNC_MODE, "true"));

    // -- Constructor

    public AbstractInvoker(Class<T> type, URL url) {
        this(type, url, (Map<String, Object>) null);
    }

    public AbstractInvoker(Class<T> type, URL url, String[] keys) {
        this(type, url, convertAttachment(url, keys));
    }

    /**
     * 构造 AbstractInvoker 实例。
     *
     * @param type 服务接口类型，不能为空
     * @param url 服务提供者的 URL 地址，不能为空
     * @param attachment 默认的附加参数映射，将被设置为不可修改的副本
     */
    public AbstractInvoker(Class<T> type, URL url, Map<String, Object> attachment) {
        if (type == null) {
            throw new IllegalArgumentException("service type == null");
        }
        if (url == null) {
            throw new IllegalArgumentException("service url == null");
        }
        this.type = type;
        this.url = url;

        // 将附件映射设置为不可修改，防止外部篡改
        this.attachment = attachment == null ? null : Collections.unmodifiableMap(attachment);
        // 从 URL 中获取服务版本，默认为 DEFAULT_VERSION
        this.version = url.getVersion(DEFAULT_VERSION);
    }

    /**
     * 从 URL 中提取指定的参数作为附加参数。
     *
     * @param url 服务 URL
     * @param keys 需要提取的参数键名数组
     * @return 包含指定参数的 Map，如果 keys 为空则返回 null
     */
    private static Map<String, Object> convertAttachment(URL url, String[] keys) {
        if (ArrayUtils.isEmpty(keys)) {
            return null;
        }
        Map<String, Object> attachment = new HashMap<>(keys.length);
        for (String key : keys) {
            String value = url.getParameter(key);
            if (value != null && value.length() > 0) {
                attachment.put(key, value);
            }
        }
        return attachment;
    }

    // -- Public api

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    @Override
    public void destroy() {
        this.destroyed = true;
        setAvailable(false);
    }

    protected void setAvailable(boolean available) {
        this.available = available;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? "" : getUrl().getAddress());
    }

    /**
     * 执行 RPC 调用的入口方法。
     * 该方法定义了完整的调用流程：
     * 1. 检查 Invoker 是否已销毁
     * 2. 准备调用上下文（设置附件、调用模式、序列化 ID 等）
     * 3. 执行实际的远程调用并获取异步结果
     * 4. 如果是同步调用，则阻塞等待结果返回
     *
     * @param inv 调用信息对象
     * @return 调用结果，可能是已完成的结果或异步 Future
     * @throws RpcException 当调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invocation inv) throws RpcException {
        // if invoker is destroyed due to address refresh from registry, let's allow the current invoke to proceed
        // 如果 Invoker 因注册中心地址刷新而被销毁，记录警告但允许当前调用继续
        if (isDestroyed()) {
            logger.warn(
                    PROTOCOL_FAILED_REQUEST,
                    "",
                    "",
                    "Invoker for service " + this + " on consumer " + NetUtils.getLocalHost() + " is destroyed, "
                            + ", dubbo version is " + Version.getVersion()
                            + ", this invoker should not be used any longer");
        }

        RpcInvocation invocation = (RpcInvocation) inv;

        // prepare rpc invocation
        // 准备 RPC 调用上下文
        prepareInvocation(invocation);

        // do invoke rpc invocation and return async result
        // 执行远程调用并返回异步结果
        AsyncRpcResult asyncResult = doInvokeAndReturn(invocation);

        // wait rpc result if sync
        // 如果是同步调用，则等待结果返回
        waitForResultIfSync(asyncResult, invocation);

        return asyncResult;
    }

    /**
     * 准备调用上下文，设置必要的调用参数和属性。
     * 包括：设置当前的 Invoker、合并附件、确定调用模式、附加异步调用 ID 和序列化 ID。
     *
     * @param inv RPC 调用信息对象
     */
    private void prepareInvocation(RpcInvocation inv) {
        // 设置当前的 Invoker 到调用信息中
        inv.setInvoker(this);

        // 添加默认的附件和客户端上下文中的附件
        addInvocationAttachments(inv);

        // 根据 URL 配置设置调用模式（同步、异步或单向）
        inv.setInvokeMode(RpcUtils.getInvokeMode(url, inv));

        // 如果是异步调用，附加调用 ID 以便关联请求和响应
        RpcUtils.attachInvocationIdIfAsync(getUrl(), inv);

        // 附加序列化 ID，用于优化网络传输
        attachInvocationSerializationId(inv);
    }

    /**
     * Attach Invocation Serialization id
     * <p>
     *     <ol>
     *         <li>Obtain the value from <code>prefer_serialization</code></li>
     *         <li>If the preceding information is not obtained, obtain the value from <code>serialization</code></li>
     *         <li>If neither is obtained, use the default value</li>
     *     </ol>
     * </p>
     *
     * @param inv inv
     */
    private void attachInvocationSerializationId(RpcInvocation inv) {
        // 从 URL 中获取序列化 ID
        Byte serializationId = UrlUtils.serializationId(getUrl());

        // 如果序列化 ID 不为空，则将其放入调用信息中
        if (serializationId != null) {
            inv.put(SERIALIZATION_ID_KEY, serializationId);
        }
    }

    /**
     * 合并附加参数到调用信息中。
     * 优先级：调用信息中已有的参数 > 客户端上下文中的参数 > Invoker 默认附件。
     *
     * @param invocation RPC 调用信息对象
     */
    private void addInvocationAttachments(RpcInvocation invocation) {
        // invoker attachment
        // 添加 Invoker 级别的默认附件，如果调用信息中不存在则添加
        if (CollectionUtils.isNotEmptyMap(attachment)) {
            invocation.addObjectAttachmentsIfAbsent(attachment);
        }

        // client context attachment
        // 添加客户端上下文中的附件，同样只在调用信息中不存在时添加
        Map<String, Object> clientContextAttachments =
                RpcContext.getClientAttachment().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(clientContextAttachments)) {
            invocation.addObjectAttachmentsIfAbsent(clientContextAttachments);
        }
    }

    /**
     * 执行实际的远程调用并处理返回结果。
     * 该方法会捕获调用过程中的各种异常，并将其转换为 AsyncRpcResult。
     *
     * @param invocation RPC 调用信息对象
     * @return 异步 RPC 结果
     */
    private AsyncRpcResult doInvokeAndReturn(RpcInvocation invocation) {
        AsyncRpcResult asyncResult;
        try {
            // 调用子类实现的远程调用逻辑
            asyncResult = (AsyncRpcResult) doInvoke(invocation);
        } catch (InvocationTargetException e) {
            // 处理业务逻辑抛出的异常
            Throwable te = e.getTargetException();
            if (te != null) {
                // if biz exception
                // 如果是 RpcException，设置错误码为业务异常
                if (te instanceof RpcException) {
                    ((RpcException) te).setCode(RpcException.BIZ_EXCEPTION);
                }
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, te, invocation);
            } else {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            }
        } catch (RpcException e) {
            // if biz exception
            // 如果是业务异常，封装为异步结果；否则直接抛出
            if (e.isBiz()) {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            } else {
                throw e;
            }
        } catch (Throwable e) {
            // 捕获其他所有异常，封装为异步结果
            asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
        }

        // 根据配置或调用模式决定是否将 Future 设置到上下文中
        if (setFutureWhenSync || invocation.getInvokeMode() != InvokeMode.SYNC) {
            // set server context
            // 将异步结果的 Future 适配后设置到服务上下文中
            RpcContext.getServiceContext().setFuture(new FutureAdapter<>(asyncResult.getResponseFuture()));
        }

        return asyncResult;
    }

    /**
     * 如果是同步调用，则阻塞等待结果返回。
     * 该方法会处理超时、中断、网络异常、序列化异常等各种情况，并转换为对应的 RpcException。
     *
     * @param asyncResult 异步 RPC 结果
     * @param invocation RPC 调用信息对象
     */
    private void waitForResultIfSync(AsyncRpcResult asyncResult, RpcInvocation invocation) {
        // 如果不是同步调用模式，直接返回
        if (InvokeMode.SYNC != invocation.getInvokeMode()) {
            return;
        }
        try {
            /*
             * NOTICE!
             * must call {@link java.util.concurrent.CompletableFuture#get(long, TimeUnit)} because
             * {@link java.util.concurrent.CompletableFuture#get()} was proved to have serious performance drop.
             */
            // 从调用信息中获取超时时间，默认为 Integer.MAX_VALUE
            Object timeoutKey = invocation.getObjectAttachmentWithoutConvert(TIMEOUT_KEY);
            long timeout = RpcUtils.convertToNumber(timeoutKey, Integer.MAX_VALUE);

            // 阻塞等待结果，带有超时时间
            asyncResult.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // 处理线程中断异常
            Thread.currentThread().interrupt();
            throw new RpcException(
                    "Interrupted unexpectedly while waiting for remote result to return! method: "
                            + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(),
                    e);
        } catch (ExecutionException e) {
            // 处理执行异常，根据根本原因抛出不同类型的 RpcException
            Throwable rootCause = e.getCause();
            if (rootCause instanceof TimeoutException) {
                throw new RpcException(
                        RpcException.TIMEOUT_EXCEPTION,
                        "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: "
                                + getUrl() + ", cause: " + e.getMessage(),
                        e);
            } else if (rootCause instanceof RemotingException) {
                throw new RpcException(
                        RpcException.NETWORK_EXCEPTION,
                        "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl()
                                + ", cause: " + e.getMessage(),
                        e);
            } else if (rootCause instanceof SerializationException) {
                throw new RpcException(
                        RpcException.SERIALIZATION_EXCEPTION,
                        "Invoke remote method failed cause by serialization error.  remote method: "
                                + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(),
                        e);
            } else {
                throw new RpcException(
                        RpcException.UNKNOWN_EXCEPTION,
                        "Fail to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl()
                                + ", cause: " + e.getMessage(),
                        e);
            }
        } catch (java.util.concurrent.TimeoutException e) {
            // 处理超时异常
            throw new RpcException(
                    RpcException.TIMEOUT_EXCEPTION,
                    "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl()
                            + ", cause: " + e.getMessage(),
                    e);
        } catch (Throwable e) {
            // 处理其他未知异常
            throw new RpcException(e.getMessage(), e);
        }
    }

    // -- Protected api

    /**
     * 获取回调执行器。
     * 如果是同步调用，返回 ThreadlessExecutor 以避免线程切换；
     * 否则从 ExecutorRepository 中获取配置的线程池。
     *
     * @param url 服务 URL
     * @param inv 调用信息对象
     * @return 回调执行器
     */
    protected ExecutorService getCallbackExecutor(URL url, Invocation inv) {
        if (InvokeMode.SYNC == RpcUtils.getInvokeMode(getUrl(), inv)) {
            return new ThreadlessExecutor();
        }
        return ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
                .getExecutor(url);
    }

    /**
     * Specific implementation of the {@link #invoke(Invocation)} method
     * 具体的远程调用实现，由子类负责与底层通信框架交互
     */
    protected abstract Result doInvoke(Invocation invocation) throws Throwable;
}
