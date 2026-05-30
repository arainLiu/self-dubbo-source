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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_CLIENT_THREADPOOL;
import static org.apache.dubbo.common.constants.CommonConstants.LAZY_CONNECT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADPOOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREAD_NAME_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CONNECT_PROVIDER;
import static org.apache.dubbo.config.Constants.CLIENT_THREAD_POOL_NAME;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_CHECK_TICK;
import static org.apache.dubbo.remoting.Constants.LEAST_HEARTBEAT_DURATION;
import static org.apache.dubbo.remoting.Constants.LEAST_RECONNECT_DURATION;
import static org.apache.dubbo.remoting.Constants.LEAST_RECONNECT_DURATION_KEY;
import static org.apache.dubbo.remoting.utils.UrlUtils.getIdleTimeout;

/**
 * 抽象客户端实现类，继承自 AbstractEndpoint 并实现 Client 接口。
 * 该类提供了客户端连接管理的核心逻辑，包括：
 * 1. 连接的建立、断开和重连机制
 * 2. 线程池的初始化与管理
 * 3. 懒加载（Lazy Connect）支持
 * 4. 连接状态的同步控制（使用 ReentrantLock）
 * 5. 消息发送前的连通性检查
 *
 * 具体的通信协议实现（如 Netty, Mina 等）需要继承此类并实现 doOpen, doClose, doConnect, doDisConnect 等抽象方法。
 */
public abstract class AbstractClient extends AbstractEndpoint implements Client {

    private Lock connectLock;

    private final boolean needReconnect;

    private final FrameworkModel frameworkModel;

    protected volatile ExecutorService executor;

    protected volatile ScheduledExecutorService connectivityExecutor;

    protected long reconnectDuration;

    /**
     * 构造 AbstractClient 实例并尝试建立连接。
     *
     * @param url 客户端配置 URL
     * @param handler 处理网络事件的 ChannelHandler
     * @throws RemotingException 如果启动或连接失败且未配置懒加载或忽略检查
     */
    public AbstractClient(URL url, ChannelHandler handler) throws RemotingException {
        super(url, handler);

        // 在调用 connect() 之前初始化连接锁
        connectLock = new ReentrantLock();

        // 默认设置需要重连，除非显式配置为 false
        needReconnect = url.getParameter(Constants.SEND_RECONNECT_KEY, true);

        frameworkModel = url.getOrDefaultFrameworkModel();

        // 初始化客户端执行器线程池
        initExecutor(url);

        reconnectDuration = getReconnectDuration(url);

        try {
            // 打开底层通信组件（如 Netty Bootstrap）
            doOpen();
        } catch (Throwable t) {
            close();
            throw new RemotingException(
                    url.toInetSocketAddress(),
                    null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(),
                    t);
        }

        try {
            // 尝试连接到服务端
            connect();
            if (logger.isInfoEnabled()) {
                logger.info("Start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                        + " connect to the server " + getRemoteAddress());
            }
        } catch (RemotingException t) {
            // 如果是懒加载客户端，连接失败不抛出异常，由后台重连任务负责重试
            if (url.getParameter(LAZY_CONNECT_KEY, false)) {
                logger.warn(
                        TRANSPORT_FAILED_CONNECT_PROVIDER,
                        "",
                        "",
                        "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                                + " connect to the server "
                                + getRemoteAddress()
                                + " (the connection request is initiated by lazy connect client, ignore and retry later!), cause: "
                                + t.getMessage(),
                        t);
                return;
            }

            // 如果配置了启动时检查（check=true），则关闭客户端并抛出异常
            if (url.getParameter(Constants.CHECK_KEY, true)) {
                close();
                throw t;
            } else {
                // 如果 check=false，则记录警告日志，允许客户端继续运行并稍后重试
                logger.warn(
                        TRANSPORT_FAILED_CONNECT_PROVIDER,
                        "",
                        "",
                        "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                                + " connect to the server " + getRemoteAddress()
                                + " (check == false, ignore and retry later!), cause: " + t.getMessage(),
                        t);
            }
        } catch (Throwable t) {
            close();
            throw new RemotingException(
                    url.toInetSocketAddress(),
                    null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(),
                    t);
        }
    }

    protected AbstractClient() {
        needReconnect = false;
        frameworkModel = null;
    }

    /**
     * 初始化客户端使用的线程池。
     * 根据 URL 配置创建或获取共享的执行器，并初始化用于连通性检查的调度线程池。
     *
     * @param url 配置 URL
     */
    private void initExecutor(URL url) {
        ExecutorRepository executorRepository = ExecutorRepository.getInstance(url.getOrDefaultApplicationModel());

        /*
         * Consumer's executor is shared globally, provider ip doesn't need to be part of the thread name.
         *
         * Instance of url is InstanceAddressURL, so addParameter actually adds parameters into ServiceInstance,
         * which means params are shared among different services. Since client is shared among services this is currently not a problem.
         */
        // 设置线程名称和默认的线程池类型
        url = url.addParameter(THREAD_NAME_KEY, CLIENT_THREAD_POOL_NAME)
                .addParameterIfAbsent(THREADPOOL_KEY, DEFAULT_CLIENT_THREADPOOL);
        executor = executorRepository.createExecutorIfAbsent(url);

        // 从框架模型中获取用于连通性检查的调度线程池
        connectivityExecutor = frameworkModel
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getConnectivityScheduledExecutor();
    }

    protected static ChannelHandler wrapChannelHandler(URL url, ChannelHandler handler) {
        return ChannelHandlers.wrap(handler, url);
    }

    public InetSocketAddress getConnectAddress() {
        return new InetSocketAddress(NetUtils.filterLocalHost(getUrl().getHost()), getUrl().getPort());
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return getUrl().toInetSocketAddress();
        }
        return channel.getRemoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return InetSocketAddress.createUnresolved(NetUtils.getLocalHost(), 0);
        }
        return channel.getLocalAddress();
    }

    @Override
    public boolean isConnected() {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.isConnected();
    }

    @Override
    public Object getAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return null;
        }
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.hasAttribute(key);
    }

    /**
     * 发送消息到服务端。
     * 如果配置了需要重连且当前未连接，则先尝试建立连接。
     *
     * @param message 要发送的消息对象
     * @param sent 是否等待消息真正发送成功
     * @throws RemotingException 如果通道已关闭或发送失败
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        // 如果需要重连且当前未连接，则触发连接操作
        if (needReconnect && !isConnected()) {
            connect();
        }
        Channel channel = getChannel();
        // TODO Can the value returned by getChannel() be null? need improvement.
        // 再次检查通道状态，确保消息能正常发送
        if (channel == null || !channel.isConnected()) {
            throw new RemotingException(this, "message can not send, because channel is closed . url:" + getUrl());
        }
        channel.send(message, sent);
    }

    /**
     * 建立到服务端的连接。
     * 该方法使用锁保证线程安全，防止并发连接操作。
     *
     * @throws RemotingException 如果连接失败
     */
    protected void connect() throws RemotingException {
        connectLock.lock();

        try {
            // 如果已经连接，直接返回
            if (isConnected()) {
                return;
            }

            // 如果客户端正在关闭或已关闭，则放弃连接
            if (isClosed() || isClosing()) {
                logger.warn(
                        TRANSPORT_FAILED_CONNECT_PROVIDER,
                        "",
                        "",
                        "No need to connect to server " + getRemoteAddress() + " from "
                                + getClass().getSimpleName() + " " + NetUtils.getLocalHost() + " using dubbo version "
                                + Version.getVersion() + ", cause: client status is closed or closing.");
                return;
            }

            // 调用子类实现的具体连接逻辑
            doConnect();

            // 验证连接结果
            if (!isConnected()) {
                throw new RemotingException(
                        this,
                        "Failed to connect to server " + getRemoteAddress() + " from "
                                + getClass().getSimpleName() + " "
                                + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                                + ", cause: Connect wait timeout: " + getConnectTimeout() + "ms.");

            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully connect to server " + getRemoteAddress() + " from "
                            + getClass().getSimpleName() + " "
                            + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                            + ", channel is " + this.getChannel());
                }
            }

        } catch (RemotingException e) {
            throw e;

        } catch (Throwable e) {
            throw new RemotingException(
                    this,
                    "Failed to connect to server " + getRemoteAddress() + " from "
                            + getClass().getSimpleName() + " "
                            + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                            + ", cause: " + e.getMessage(),
                    e);

        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 断开与服务端的连接。
     * 该方法会关闭底层通道并调用子类的断开逻辑。
     */
    public void disconnect() {
        connectLock.lock();
        try {
            try {
                // 关闭底层通道
                Channel channel = getChannel();
                if (channel != null) {
                    channel.close();
                }
            } catch (Throwable e) {
                logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
            }
            try {
                // 调用子类实现的断开逻辑
                doDisConnect();
            } catch (Throwable e) {
                logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
            }
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 计算重连任务的执行间隔。
     *
     * @param url 配置 URL
     * @return 重连间隔时间（毫秒）
     */
    private long getReconnectDuration(URL url) {
        int idleTimeout = getIdleTimeout(url);
        long heartbeatTimeoutTick = calculateLeastDuration(idleTimeout);
        return calculateReconnectDuration(url, heartbeatTimeoutTick);
    }

    /**
     * 计算最小的任务执行间隔，防止间隔过小导致性能问题。
     *
     * @param time 原始时间间隔
     * @return 计算后的最小间隔
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

    /**
     * 计算最终的重连持续时间，确保不小于配置的最小值。
     *
     * @param url 配置 URL
     * @param tick 基础时间间隔
     * @return 最终的重连间隔
     */
    private long calculateReconnectDuration(URL url, long tick) {
        long leastReconnectDuration = url.getParameter(LEAST_RECONNECT_DURATION_KEY, LEAST_RECONNECT_DURATION);
        return Math.max(leastReconnectDuration, tick);
    }

    /**
     * 重新连接到服务端。
     * 该方法会先断开现有连接，然后重新发起连接。
     *
     * @throws RemotingException 如果重连失败
     */
    @Override
    public void reconnect() throws RemotingException {
        connectLock.lock();
        try {
            disconnect();
            connect();
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 关闭客户端。
     * 该方法会依次关闭父类资源、断开连接并调用子类的关闭逻辑。
     */
    @Override
    public void close() {
        if (isClosed()) {
            logger.warn(
                    TRANSPORT_FAILED_CONNECT_PROVIDER,
                    "",
                    "",
                    "No need to close connection to server " + getRemoteAddress() + " from "
                            + getClass().getSimpleName() + " " + NetUtils.getLocalHost() + " using dubbo version "
                            + Version.getVersion() + ", cause: the client status is closed.");
            return;
        }

        connectLock.lock();
        try {
            // 双重检查，防止并发关闭
            if (isClosed()) {
                logger.warn(
                        TRANSPORT_FAILED_CONNECT_PROVIDER,
                        "",
                        "",
                        "No need to close connection to server " + getRemoteAddress() + " from "
                                + getClass().getSimpleName() + " " + NetUtils.getLocalHost() + " using dubbo version "
                                + Version.getVersion() + ", cause: the client status is closed.");
                return;
            }

            try {
                // 关闭父类资源
                super.close();
            } catch (Throwable e) {
                logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
            }

            try {
                // 断开网络连接
                disconnect();
            } catch (Throwable e) {
                logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
            }

            try {
                // 调用子类实现的关闭逻辑
                doClose();
            } catch (Throwable e) {
                logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
            }

        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void close(int timeout) {
        close();
    }

    @Override
    public String toString() {
        return getClass().getName() + " [" + getLocalAddress() + " -> " + getRemoteAddress() + "]";
    }

    /**
     * Open client.
     * 打开客户端底层通信组件。
     * @throws Throwable 打开过程中发生的异常
     */
    protected abstract void doOpen() throws Throwable;

    /**
     * Close client.
     * 关闭客户端底层通信组件。
     * @throws Throwable 关闭过程中发生的异常
     */
    protected abstract void doClose() throws Throwable;

    /**
     * Connect to server.
     * 建立到服务端的物理连接。
     * @throws Throwable 连接过程中发生的异常
     */
    protected abstract void doConnect() throws Throwable;

    /**
     * disConnect to server.
     * 断开到服务端的物理连接。
     * @throws Throwable 断开过程中发生的异常
     */
    protected abstract void doDisConnect() throws Throwable;

    /**
     * Get the connected channel.
     * 获取当前已连接的通道实例。
     * @return 已连接的 Channel 对象，如果未连接则返回 null
     */
    protected abstract Channel getChannel();
}
