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
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.transport.AbstractChannel;
import org.apache.dubbo.remoting.transport.codec.CodecAdapter;
import org.apache.dubbo.remoting.utils.PayloadDropper;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.EncoderException;
import io.netty.util.ReferenceCountUtil;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_ENCODE_IN_IO_THREAD;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.ENCODE_IN_IO_THREAD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;
import static org.apache.dubbo.rpc.model.ScopeModelUtil.getFrameworkModel;

/**
 * NettyChannel maintains the cache of channel.
 * NettyChannel 维护了 Netty 通道与 Dubbo 通道之间的映射缓存。
 * 它封装了底层的 Netty Channel，提供了 Dubbo 所需的通信接口，并管理通道的生命周期和属性。
 */
final class NettyChannel extends AbstractChannel {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(NettyChannel.class);
    /**
     * the cache for netty channel and dubbo channel
     * 用于缓存 Netty Channel 与 Dubbo NettyChannel 实例的映射关系，确保一一对应
     */
    private static final ConcurrentMap<Channel, NettyChannel> CHANNEL_MAP = new ConcurrentHashMap<>();
    /**
     * netty channel
     * 底层的 Netty Channel 实例
     */
    private final Channel channel;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    /**
     * 标记通道是否处于活跃状态（已连接且可用）
     */
    private final AtomicBoolean active = new AtomicBoolean(false);

    private final Netty4BatchWriteQueue writeQueue;

    /**
     * 是否在 IO 线程中执行编码操作
     */
    private final boolean encodeInIOThread;

    private Codec2 codec;

    /**
     * The constructor of NettyChannel.
     * It is private so NettyChannel usually create by {@link NettyChannel#getOrAddChannel(Channel, URL, ChannelHandler)}
     *
     * @param channel netty channel
     * @param url     dubbo url
     * @param handler dubbo handler that contain netty handler
     * 构造 NettyChannel 实例。
     * 由于构造函数是私有的，通常通过 getOrAddChannel 方法获取或创建实例。
     */
    private NettyChannel(Channel channel, URL url, ChannelHandler handler) {
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
        // 创建批量写队列，优化网络发送性能
        this.writeQueue = Netty4BatchWriteQueue.createWriteQueue(channel);
        // 根据 URL 配置获取对应的编解码器
        this.codec = getChannelCodec(url);
        // 从 URL 中读取是否在 IO 线程中编码的配置
        this.encodeInIOThread = getUrl().getParameter(ENCODE_IN_IO_THREAD_KEY, DEFAULT_ENCODE_IN_IO_THREAD);
        AddressUtils.initAddressIfNecessary(this);
    }

    /**
     * Get dubbo channel by netty channel through channel cache.
     * Put netty channel into it if dubbo channel don't exist in the cache.
     *
     * @param ch      netty channel
     * @param url     dubbo url
     * @param handler dubbo handler that contain netty's handler
     * 通过 Netty Channel 获取或创建对应的 Dubbo NettyChannel。
     * 如果缓存中不存在，则创建新实例并放入缓存；如果存在，则更新其活跃状态。
     */
    static NettyChannel getOrAddChannel(Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }
        // 尝试从缓存中获取已有的 NettyChannel
        NettyChannel ret = CHANNEL_MAP.get(ch);
        if (ret == null) {
            // 如果缓存中没有，则创建新的 NettyChannel
            NettyChannel nettyChannel = new NettyChannel(ch, url, handler);
            // 只有当底层 Netty Channel 处于活跃状态时才加入缓存
            if (ch.isActive()) {
                nettyChannel.markActive(true);
                ret = CHANNEL_MAP.putIfAbsent(ch, nettyChannel);
            }
            if (ret == null) {
                ret = nettyChannel;
            }
        } else {
            // 如果缓存中已存在，标记为活跃状态
            ret.markActive(true);
        }
        return ret;
    }

    /**
     * Remove the inactive channel.
     *
     * @param ch netty channel
     * 如果 Netty Channel 已断开连接（非活跃），则从缓存中移除对应的 NettyChannel。
     */
    static void removeChannelIfDisconnected(Channel ch) {
        if (ch != null && !ch.isActive()) {
            NettyChannel nettyChannel = CHANNEL_MAP.remove(ch);
            if (nettyChannel != null) {
                nettyChannel.markActive(false);
            }
        }
    }

    /**
     * 强制从缓存中移除指定的 Netty Channel 关联的 NettyChannel。
     *
     * @param ch netty channel
     */
    static void removeChannel(Channel ch) {
        if (ch != null) {
            NettyChannel nettyChannel = CHANNEL_MAP.remove(ch);
            if (nettyChannel != null) {
                nettyChannel.markActive(false);
            }
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return AddressUtils.getLocalAddress(this);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return AddressUtils.getRemoteAddress(this);
    }

    public String getLocalAddressKey() {
        return AddressUtils.getLocalAddressKey(this);
    }

    public String getRemoteAddressKey() {
        return AddressUtils.getRemoteAddressKey(this);
    }

    @Override
    public boolean isConnected() {
        // 连接状态取决于是否未关闭且标记为活跃
        return !isClosed() && active.get();
    }

    public boolean isActive() {
        return active.get();
    }

    /**
     * 标记通道的活跃状态。
     *
     * @param isActive 是否活跃
     */
    public void markActive(boolean isActive) {
        active.set(isActive);
    }

    /**
     * Send message by netty and whether to wait the completion of the sending.
     *
     * @param message message that need send.
     * @param sent    whether to ack async-sent
     * @throws RemotingException throw RemotingException if wait until timeout or any exception thrown by method body that surrounded by try-catch.
     * 通过 Netty 发送消息，并根据 sent 参数决定是否等待发送完成。
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        // whether the channel is closed
        // 检查通道是否已关闭
        super.send(message, sent);

        boolean success = true;
        int timeout = 0;
        ByteBuf buf = null;
        try {
            Object outputMessage = message;
            // 如果不在 IO 线程中编码，则在此处预先进行编码操作
            if (!encodeInIOThread) {
                buf = channel.alloc().buffer();
                ChannelBuffer buffer = new NettyBackedChannelBuffer(buf);
                codec.encode(this, buffer, message);
                outputMessage = buf;
            }
            // 将消息加入写队列，并添加监听器处理发送结果
            ChannelFuture future = writeQueue.enqueue(outputMessage).addListener((ChannelFutureListener) f -> {
                // 如果不是请求对象，则不需要处理响应逻辑
                if (!(message instanceof Request)) {
                    return;
                }
                ChannelHandler handler = getChannelHandler();
                if (f.isSuccess()) {
                    // 发送成功，通知处理器
                    handler.sent(NettyChannel.this, message);
                } else {
                    Throwable t = f.cause();
                    if (t == null) {
                        return;
                    }
                    // 发送失败，构建错误响应并通知处理器
                    Response response = buildErrorResponse((Request) message, t);
                    handler.received(NettyChannel.this, response);
                }
            });

            if (sent) {
                // 如果需要同步等待，则获取超时时间并等待发送完成
                timeout = getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
                success = future.await(timeout);
            }
            Throwable cause = future.cause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {
            // 发生异常时，如果通道已断开则从缓存中移除
            removeChannelIfDisconnected(channel);
            if (buf != null) {
                // 如果发生了异常，释放预先分配的 ByteBuf 以防止内存泄漏
                ReferenceCountUtil.safeRelease(buf);
            }
            throw new RemotingException(
                    this,
                    "Failed to send message " + PayloadDropper.getRequestWithoutData(message) + " to "
                            + getRemoteAddress() + ", cause: " + e.getMessage(),
                    e);
        }
        if (!success) {
            // 如果等待超时，抛出超时异常
            throw new RemotingException(
                    this,
                    "Failed to send message " + PayloadDropper.getRequestWithoutData(message) + " to "
                            + getRemoteAddress() + "in timeout(" + timeout + "ms) limit");
        }
    }

    @Override
    public void close() {
        try {
            // 调用父类的关闭逻辑
            super.close();
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
        try {
            // 从全局缓存中移除该通道
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
        try {
            // 清空通道上附加的属性
            attributes.clear();
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            // 关闭底层的 Netty Channel
            channel.close();
        } catch (Exception e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        // The null value is not allowed in the ConcurrentHashMap.
        // ConcurrentHashMap 不允许 null 值，如果值为 null 则执行移除操作
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    protected void setUrl(URL url) {
        super.setUrl(url);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        // FIXME: a hack to make org.apache.dubbo.remoting.exchange.support.DefaultFuture.closeChannel work
        // 特殊处理 NettyClient 的比较，以兼容 DefaultFuture 的逻辑
        if (obj instanceof NettyClient) {
            NettyClient client = (NettyClient) obj;
            return channel.equals(client.getNettyChannel());
        }

        return getClass() == obj.getClass() && Objects.equals(channel, ((NettyChannel) obj).channel);
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

    public Channel getNioChannel() {
        return channel;
    }

    /**
     * build a bad request's response
     *
     * @param request the request
     * @param t       the throwable. In most cases, serialization fails.
     * @return the response
     * 构建一个错误请求的响应对象。
     * 通常用于处理序列化失败或编码器异常的情况。
     */
    private static Response buildErrorResponse(Request request, Throwable t) {
        Response response = new Response(request.getId(), request.getVersion());
        if (t instanceof EncoderException) {
            // 如果是编码器异常，设置为序列化错误状态
            response.setStatus(Response.SERIALIZATION_ERROR);
        } else {
            // 否则设置为坏请求状态
            response.setStatus(Response.BAD_REQUEST);
        }
        response.setErrorMessage(StringUtils.toString(t));
        return response;
    }

    @SuppressWarnings("deprecation")
    private static Codec2 getChannelCodec(URL url) {
        String codecName = url.getParameter(Constants.CODEC_KEY);
        if (StringUtils.isEmpty(codecName)) {
            // 如果未指定编解码器，默认使用协议名作为编解码器名称
            codecName = url.getProtocol();
        }
        FrameworkModel frameworkModel = getFrameworkModel(url.getScopeModel());
        if (frameworkModel.getExtensionLoader(Codec2.class).hasExtension(codecName)) {
            return frameworkModel.getExtensionLoader(Codec2.class).getExtension(codecName);
        } else if (frameworkModel.getExtensionLoader(Codec.class).hasExtension(codecName)) {
            return new CodecAdapter(
                    frameworkModel.getExtensionLoader(Codec.class).getExtension(codecName));
        } else {
            // 如果都找不到，回退到默认的编解码器
            return frameworkModel.getExtensionLoader(Codec2.class).getExtension("default");
        }
    }

    public void setCodec(Codec2 codec) {
        this.codec = codec;
    }
}
