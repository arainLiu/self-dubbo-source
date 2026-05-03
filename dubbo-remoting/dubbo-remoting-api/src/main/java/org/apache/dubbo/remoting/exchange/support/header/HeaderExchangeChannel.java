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
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;

/**
 * ExchangeReceiver
 */
final class HeaderExchangeChannel implements ExchangeChannel {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(HeaderExchangeChannel.class);

    private static final String CHANNEL_KEY = HeaderExchangeChannel.class.getName() + ".CHANNEL";

    private final Channel channel;

    private final int shutdownTimeout;

    private volatile boolean closed = false;

    HeaderExchangeChannel(Channel channel) {
        if (channel == null) {
            throw new IllegalArgumentException("channel == null");
        }
        this.channel = channel;
        this.shutdownTimeout = Optional.ofNullable(channel.getUrl())
                .map(URL::getOrDefaultApplicationModel)
                .map(ConfigurationUtils::getServerShutdownTimeout)
                .orElse(DEFAULT_TIMEOUT);
    }

    static HeaderExchangeChannel getOrAddChannel(Channel ch) {
        if (ch == null) {
            return null;
        }
        HeaderExchangeChannel ret = (HeaderExchangeChannel) ch.getAttribute(CHANNEL_KEY);
        if (ret == null) {
            ret = new HeaderExchangeChannel(ch);
            if (ch.isConnected()) {
                ch.setAttribute(CHANNEL_KEY, ret);
            }
        }
        return ret;
    }

    static void removeChannelIfDisconnected(Channel ch) {
        if (ch != null && !ch.isConnected()) {
            ch.removeAttribute(CHANNEL_KEY);
        }
    }

    static void removeChannel(Channel ch) {
        if (ch != null) {
            ch.removeAttribute(CHANNEL_KEY);
        }
    }

    @Override
    public void send(Object message) throws RemotingException {
        send(message, false);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        if (closed) {
            throw new RemotingException(
                    this.getLocalAddress(),
                    null,
                    "Failed to send message " + message + ", cause: The channel " + this + " is closed!");
        }
        if (message instanceof Request || message instanceof Response || message instanceof String) {
            channel.send(message, sent);
        } else {
            Request request = new Request();
            request.setVersion(Version.getProtocolVersion());
            request.setTwoWay(false);
            request.setData(message);
            channel.send(request, sent);
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return request(request, null);
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return request(request, timeout, null);
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return request(request, channel.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT), executor);
    }

    /**
     * 发送请求并返回异步结果Future，实现请求-响应模式的网络通信。
     * <p>
     * 该方法的处理流程：
     * 1. 检查通道是否已关闭，防止在关闭状态下发送请求；
     * 2. 将请求对象包装为Request对象（如果还不是Request类型），设置协议版本、双向通信标识和数据内容；
     * 3. 创建DefaultFuture对象，用于关联请求ID和后续的响应结果；
     * 4. 通过底层Channel发送Request消息；
     * 5. 如果发送失败，取消Future并抛出异常；
     * 6. 返回DefaultFuture供调用者等待异步结果。
     * </p>
     *
     * @param request 请求数据对象，可以是任意类型（会被包装为Request）或已经是Request类型
     * @param timeout 超时时间（毫秒），用于控制等待响应的最长时间
     * @param executor 执行回调的线程池，用于处理响应到达后的异步通知逻辑
     * @return CompletableFuture<Object> 异步返回的响应结果，调用者可通过get()方法阻塞等待
     * @throws RemotingException 当通道已关闭或发送请求失败时抛出异常
     */
    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor)
            throws RemotingException {
        // 检查通道状态：如果已关闭则直接抛出异常，避免无效的网络操作
        if (closed) {
            throw new RemotingException(
                    this.getLocalAddress(),
                    null,
                    "Failed to send request " + request + ", cause: The channel " + this + " is closed!");
        }

        Request req;
        // 如果请求对象已经是Request类型则直接使用，否则包装为标准的Request对象
        if (request instanceof Request) {
            req = (Request) request;
        } else {
            // 创建Request对象并设置必要属性：协议版本、双向通信标志（true表示需要响应）、请求数据
            req = new Request();
            req.setVersion(Version.getProtocolVersion());
            req.setTwoWay(true);
            req.setData(request);
        }

        // 创建DefaultFuture对象，建立请求ID与响应的关联关系，用于异步接收响应结果
        DefaultFuture future = DefaultFuture.newFuture(channel, req, timeout, executor);
        try {
            // 通过底层Channel发送Request消息到服务端
            channel.send(req);
        } catch (RemotingException e) {
            // 发送失败时取消Future，清理等待队列中的相关资源
            future.cancel();
            throw e;
        }

        // 返回异步Future对象，调用者可以通过它等待和处理响应结果
        return future;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            // graceful close
            DefaultFuture.closeChannel(channel, ConfigurationUtils.reCalShutdownTime(shutdownTimeout));
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }

        try {
            channel.close();
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
    }

    // graceful close
    @Override
    public void close(int timeout) {
        if (closed) {
            return;
        }
        if (timeout > 0) {
            long start = System.currentTimeMillis();
            while (DefaultFuture.hasFuture(channel) && System.currentTimeMillis() - start < timeout) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
                }
            }
        }
        close();
    }

    @Override
    public void startClose() {
        channel.startClose();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    @Override
    public URL getUrl() {
        return channel.getUrl();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return (ExchangeHandler) channel.getChannelHandler();
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + channel.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HeaderExchangeChannel other = (HeaderExchangeChannel) obj;
        return channel.equals(other.channel);
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
