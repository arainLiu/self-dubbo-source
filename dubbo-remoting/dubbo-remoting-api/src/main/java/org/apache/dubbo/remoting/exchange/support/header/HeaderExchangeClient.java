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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.remoting.Constants.HEARTBEAT_CHECK_TICK;
import static org.apache.dubbo.remoting.Constants.LEAST_HEARTBEAT_DURATION;
import static org.apache.dubbo.remoting.Constants.LEAST_RECONNECT_DURATION;
import static org.apache.dubbo.remoting.Constants.LEAST_RECONNECT_DURATION_KEY;
import static org.apache.dubbo.remoting.Constants.TICKS_PER_WHEEL;
import static org.apache.dubbo.remoting.utils.UrlUtils.getHeartbeat;
import static org.apache.dubbo.remoting.utils.UrlUtils.getIdleTimeout;

/**
 * DefaultMessageClient
 * 基于 Header 协议的交换层客户端实现。
 * 该类封装了底层的 Client，提供了请求-响应模式的通信能力，并负责管理心跳检测和断线重连任务。
 *
 * 核心功能：
 * 1. 委托底层 Client 进行网络通信
 * 2. 通过 HeaderExchangeChannel 处理 Exchange 层的消息编解码
 * 3. 启动和管理 HeartbeatTimerTask（心跳检测）和 ReconnectTimerTask（断线重连）
 * 4. 提供连接状态的检查，包括空闲超时判断
 */
public class HeaderExchangeClient implements ExchangeClient {

    private final Client client;
    private final ExchangeChannel channel;

    /**
     * 全局共享的心跳与重连检查定时器。
     * 使用 HashedWheelTimer 提高定时任务的性能，所有 HeaderExchangeClient 实例共享此定时器。
     */
    public static GlobalResourceInitializer<HashedWheelTimer> IDLE_CHECK_TIMER = new GlobalResourceInitializer<>(
            () -> new HashedWheelTimer(
                    new NamedThreadFactory("dubbo-client-heartbeat-reconnect", true),
                    1,
                    TimeUnit.SECONDS,
                    TICKS_PER_WHEEL),
            HashedWheelTimer::stop);

    private ReconnectTimerTask reconnectTimerTask;
    private HeartbeatTimerTask heartBeatTimerTask;
    private final int idleTimeout;

    /**
     * 构造 HeaderExchangeClient 实例。
     *
     * @param client 底层的 Remoting Client 实例
     * @param startTimer 是否立即启动心跳和重连定时任务
     */
    public HeaderExchangeClient(Client client, boolean startTimer) {
        Assert.notNull(client, "Client can't be null");
        this.client = client;
        // 创建 HeaderExchangeChannel 包装底层通道
        this.channel = new HeaderExchangeChannel(client);

        if (startTimer) {
            URL url = client.getUrl();
            // 从 URL 中获取空闲超时时间
            idleTimeout = getIdleTimeout(url);
            // 启动断线重连任务
            startReconnectTask(url);
            // 启动心跳检测任务
            startHeartBeatTask(url);
        } else {
            idleTimeout = 0;
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return channel.request(request);
    }

    @Override
    public URL getUrl() {
        return channel.getUrl();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return channel.request(request, timeout);
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return channel.request(request, executor);
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor)
            throws RemotingException {
        return channel.request(request, timeout, executor);
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    /**
     * 检查当前客户端的连接状态。
     * 不仅检查底层连接是否建立，还会根据 idleTimeout 判断连接是否处于空闲超时状态。
     *
     * @return 如果连接正常且未超时则返回 true，否则返回 false
     */
    @Override
    public boolean isConnected() {
        if (channel.isConnected()) {
            // 如果没有设置空闲超时时间，只要底层连通即视为连接
            if (idleTimeout <= 0) {
                return true;
            }
            // 获取最后一次读取数据的时间戳
            Long lastRead = (Long) channel.getAttribute(HeartbeatHandler.KEY_READ_TIMESTAMP);
            Long now = System.currentTimeMillis();

            // 如果从未读取过数据，或者当前时间与最后读取时间的差值小于空闲超时时间，则认为连接有效
            return lastRead == null || now - lastRead < idleTimeout;
        }
        return false;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return channel.getExchangeHandler();
    }

    @Override
    public void send(Object message) throws RemotingException {
        channel.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        channel.send(message, sent);
    }

    @Override
    public boolean isClosed() {
        return channel.isClosed();
    }

    @Override
    public synchronized void close() {
        doClose();
        channel.close();
    }

    @Override
    public void close(int timeout) {
        // Mark the client into the closure process
        // 标记客户端进入关闭流程
        startClose();
        // 取消定时任务
        doClose();
        channel.close(timeout);
    }

    @Override
    public void startClose() {
        channel.startClose();
    }

    @Override
    public void reset(URL url) {
        client.reset(url);
        // FIXME, should cancel and restart timer tasks if parameters in the new URL are different?
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public synchronized void reconnect() throws RemotingException {
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        return channel.hasAttribute(key);
    }

    /**
     * 启动心跳检测任务。
     * 如果底层 Client 无法自行处理空闲检测，则创建 HeartbeatTimerTask 定期发送心跳。
     *
     * @param url 配置 URL，用于获取心跳间隔等参数
     */
    private void startHeartBeatTask(URL url) {
        if (!client.canHandleIdle()) {
            // 从 URL 中获取心跳间隔
            int heartbeat = getHeartbeat(url);
            // 计算最小的心跳检查间隔
            long heartbeatTick = calculateLeastDuration(heartbeat);
            heartBeatTimerTask = new HeartbeatTimerTask(
                    () -> Collections.singleton(this), IDLE_CHECK_TIMER.get(), heartbeatTick, heartbeat);
        }
    }

    /**
     * 启动断线重连任务。
     * 根据配置决定是否开启重连，并计算重连检查的间隔时间。
     *
     * @param url 配置 URL，用于获取重连相关参数
     */
    private void startReconnectTask(URL url) {
        if (shouldReconnect(url)) {
            // 计算心跳超时的检查间隔
            long heartbeatTimeoutTick = calculateLeastDuration(idleTimeout);
            reconnectTimerTask = new ReconnectTimerTask(
                    () -> Collections.singleton(this),
                    IDLE_CHECK_TIMER.get(),
                    calculateReconnectDuration(url, heartbeatTimeoutTick),
                    idleTimeout);
        }
    }

    /**
     * 执行关闭操作，取消所有已启动的定时任务。
     * 防止在客户端关闭后继续执行心跳或重连逻辑。
     */
    private void doClose() {
        if (heartBeatTimerTask != null) {
            heartBeatTimerTask.cancel();
            heartBeatTimerTask = null;
        }
        if (reconnectTimerTask != null) {
            reconnectTimerTask.cancel();
            reconnectTimerTask = null;
        }
    }

    /**
     * Each interval cannot be less than 1000ms.
     * 计算最小的任务执行间隔。
     * 为了保证性能，心跳检查和重连检查的间隔不能太小，这里设定了最小值为 1 秒。
     *
     * @param time 原始的时间间隔（毫秒）
     * @return 计算后的实际执行间隔
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

    /**
     * 计算断线重连的检查间隔。
     * 确保重连间隔不小于配置的最小重连持续时间。
     *
     * @param url 配置 URL
     * @param tick 基础的时间间隔
     * @return 最终的重连检查间隔
     */
    private long calculateReconnectDuration(URL url, long tick) {
        long leastReconnectDuration = url.getParameter(LEAST_RECONNECT_DURATION_KEY, LEAST_RECONNECT_DURATION);
        return Math.max(leastReconnectDuration, tick);
    }

    /**
     * 判断是否应该开启断线重连功能。
     * 默认开启，除非在 URL 中显式配置 reconnect=false。
     *
     * @param url 配置 URL
     * @return 如果应该重连则返回 true
     */
    protected boolean shouldReconnect(URL url) {
        return !Boolean.FALSE.toString().equalsIgnoreCase(url.getParameter(Constants.RECONNECT_KEY));
    }

    @Override
    public String toString() {
        return "HeaderExchangeClient [channel=" + channel + "]";
    }
}
