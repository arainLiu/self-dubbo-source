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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelEvent;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.RemotingServer;
import org.apache.dubbo.remoting.event.ReadOnlyEvent;
import org.apache.dubbo.remoting.event.WriteableEvent;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Request;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableCollection;
import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;
import static org.apache.dubbo.common.constants.CommonConstants.WRITEABLE_EVENT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_RESPONSE;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_CHECK_TICK;
import static org.apache.dubbo.remoting.Constants.LEAST_HEARTBEAT_DURATION;
import static org.apache.dubbo.remoting.Constants.TICKS_PER_WHEEL;
import static org.apache.dubbo.remoting.utils.UrlUtils.getCloseTimeout;

/**
 * ExchangeServerImpl
 */
public class HeaderExchangeServer implements ExchangeServer {

    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private final RemotingServer server;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * 全局共享的空闲检查定时器，所有HeaderExchangeServer实例共用同一个定时器实例。
     * <p>
     * 使用GlobalResourceInitializer保证线程安全的懒加载和资源的全局唯一性。该定时器用于定期检测
     * 服务器端的空闲连接，自动关闭超过配置时间的非活跃连接，防止资源泄漏。
     * </p>
     * <p>
     * 定时器配置特性：
     * <ul>
     *   <li>使用守护线程（NamedThreadFactory第二个参数为true），避免阻止JVM正常退出</li>
     *   <li>tick间隔为1秒，提供秒级的空闲检测精度</li>
     *   <li>TICKS_PER_WHEEL定义时间轮的槽位数，影响定时器的内存占用和性能</li>
     *   <li>在JVM关闭时会自动调用HashedWheelTimer::stop方法停止定时器并释放资源</li>
     * </ul>
     * </p>
     */
    public static GlobalResourceInitializer<HashedWheelTimer> IDLE_CHECK_TIMER = new GlobalResourceInitializer<>(
            () -> new HashedWheelTimer(
                    new NamedThreadFactory("dubbo-server-idleCheck", true), 1, TimeUnit.SECONDS, TICKS_PER_WHEEL),
            HashedWheelTimer::stop);

    private CloseTimerTask closeTimer;

    public HeaderExchangeServer(RemotingServer server) {
        Assert.notNull(server, "server == null");
        this.server = server;
        startIdleCheckTask(getUrl());
    }

    public RemotingServer getServer() {
        return server;
    }

    @Override
    public boolean isClosed() {
        return server.isClosed();
    }

    private boolean isRunning() {
        // If there are any client connections,
        // our server should be running.
        return getChannels().stream().anyMatch(Channel::isConnected);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        doClose();
        server.close();
    }

    @Override
    public void close(final int timeout) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        startClose();
        if (timeout > 0) {
            final long start = System.currentTimeMillis();
            if (getUrl().getParameter(Constants.CHANNEL_SEND_READONLYEVENT_KEY, true)) {
                sendChannelEvent(READONLY_EVENT);
            }
            while (HeaderExchangeServer.this.isRunning() && System.currentTimeMillis() - start < (long) timeout) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
                }
            }
        }
        doClose();
        server.close(timeout);
    }

    @Override
    public void startClose() {
        server.startClose();
    }

        /**
     * 向所有已连接的通道发送指定的通道事件（如只读/可写事件）。
     * <p>
     * 该方法会将事件封装为Request对象，异步发送给所有已连接的客户端通道。
     * 主要用于流量控制和状态通知场景，例如在关闭前发送只读事件告知客户端停止发送新请求，
     * 或发送可写事件告知客户端可以恢复发送请求。
     * </p>
     * <p>
     * 异常处理策略：
     * <ul>
     *   <li>如果通道已关闭且抛出ClosedChannelException，则忽略该异常继续处理其他通道</li>
     *   <li>其他RemotingException会记录警告日志，但不影响整体流程，保证事件的尽力投递</li>
     * </ul>
     * </p>
     *
     * @param event 要发送的事件类型字符串，如READONLY_EVENT（只读事件）或WRITEABLE_EVENT（可写事件）
     */
    private void sendChannelEvent(String event) {
        /*
         * 构造事件请求对象，设置为单向通信避免等待响应
         */
        Request request = new Request();
        request.setEvent(event);
        request.setTwoWay(false);
        request.setVersion(Version.getProtocolVersion());

        Collection<Channel> channels = getChannels();
        for (Channel channel : channels) {
            try {
                if (channel.isConnected()) {
                    channel.send(request, getUrl().getParameter(Constants.CHANNEL_READONLYEVENT_SENT_KEY, true));
                }
            } catch (RemotingException e) {
                /*
                 * 如果服务器已关闭且异常是由通道关闭引起的，则跳过该通道继续处理其他通道
                 */
                if (closed.get() && e.getCause() instanceof ClosedChannelException) {
                    // ignore ClosedChannelException which means the connection has been closed.
                    continue;
                }
                logger.warn(TRANSPORT_FAILED_RESPONSE, "", "", "send cannot write message error.", e);
            }
        }
    }


    private void doClose() {
        cancelCloseTask();
    }

    private void cancelCloseTask() {
        if (closeTimer != null) {
            closeTimer.cancel();
        }
    }

    @Override
    public Collection<ExchangeChannel> getExchangeChannels() {
        Collection<ExchangeChannel> exchangeChannels = new ArrayList<>();
        Collection<Channel> channels = server.getChannels();
        if (CollectionUtils.isNotEmpty(channels)) {
            for (Channel channel : channels) {
                exchangeChannels.add(HeaderExchangeChannel.getOrAddChannel(channel));
            }
        }
        return exchangeChannels;
    }

    @Override
    public ExchangeChannel getExchangeChannel(InetSocketAddress remoteAddress) {
        Channel channel = server.getChannel(remoteAddress);
        return HeaderExchangeChannel.getOrAddChannel(channel);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Collection<Channel> getChannels() {
        return (Collection) getExchangeChannels();
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return getExchangeChannel(remoteAddress);
    }

    @Override
    public boolean isBound() {
        return server.isBound();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return server.getLocalAddress();
    }

    @Override
    public URL getUrl() {
        return server.getUrl();
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return server.getChannelHandler();
    }

    @Override
    public void reset(URL url) {
        server.reset(url);
        try {
            int currCloseTimeout = getCloseTimeout(getUrl());
            int closeTimeout = getCloseTimeout(url);
            if (closeTimeout != currCloseTimeout) {
                cancelCloseTask();
                startIdleCheckTask(url);
            }
        } catch (Throwable t) {
            logger.error(INTERNAL_ERROR, "unknown error in remoting module", "", t.getMessage(), t);
        }
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void send(Object message) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(
                    this.getLocalAddress(),
                    null,
                    "Failed to send message " + message + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        server.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(
                    this.getLocalAddress(),
                    null,
                    "Failed to send message " + message + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        server.send(message, sent);
    }

    /**
     * Each interval cannot be less than 1000ms.
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

        /**
     * 启动空闲连接检测任务，定期检查并关闭超过指定时间的空闲连接。
     * <p>
     * 该方法首先检查底层服务器是否已具备空闲处理能力，如果底层服务器不支持自动处理空闲连接
     * （canHandleIdle返回false），则会创建一个CloseTimerTask定时任务。该任务会定期扫描所有通道，
     * 关闭那些空闲时间超过配置阈值的连接，防止资源泄漏并维持连接池健康。
     * </p>
     *
     * @param url 包含关闭超时配置的URL对象，用于提取close.timeout参数来计算检测间隔
     */
    private void startIdleCheckTask(URL url) {
        /*
         * 仅在底层服务器不具备空闲处理能力时才创建外部定时任务
         */
        if (!server.canHandleIdle()) {
            /*
             * 创建通道提供者，提供当前所有活跃通道的不可变集合供定时任务检查
             */
            AbstractTimerTask.ChannelProvider cp =
                    () -> unmodifiableCollection(HeaderExchangeServer.this.getChannels());
            int closeTimeout = getCloseTimeout(url);
            long closeTimeoutTick = calculateLeastDuration(closeTimeout);
            /*
             * 创建并注册关闭定时器任务，使用全局共享的IDLE_CHECK_TIMER执行周期性检测
             */
            this.closeTimer = new CloseTimerTask(cp, IDLE_CHECK_TIMER.get(), closeTimeoutTick, closeTimeout);
        }
    }


        /**
     * 触发通道事件处理，将特定事件转换为Request发送给对端或委托给底层服务器处理。
     * <p>
     * 该方法根据事件类型采取不同的处理策略：
     * <ul>
     *   <li>对于只读事件（ReadOnlyEvent），调用sendChannelEvent方法向所有连接的客户端发送READONLY_EVENT，
     *       告知客户端该服务器不再接受新的请求，用于优雅关闭场景</li>
     *   <li>对于可写事件（WriteableEvent），调用sendChannelEvent方法向所有连接的客户端发送WRITEABLE_EVENT，
     *       告知客户端可以恢复发送请求，用于流量控制恢复场景</li>
     *   <li>对于其他类型的事件，直接委托给底层的RemotingServer进行处理</li>
     * </ul>
     * </p>
     *
     * @param event 要处理的通道事件对象，不能为null
     */
    @Override
    public void fireChannelEvent(ChannelEvent event) {
        if (event instanceof ReadOnlyEvent) {
            sendChannelEvent(READONLY_EVENT);
        } else if (event instanceof WriteableEvent) {
            sendChannelEvent(WRITEABLE_EVENT);
        } else {
            // For other events, delegate to the underlying server
            server.fireChannelEvent(event);
        }
    }

}
