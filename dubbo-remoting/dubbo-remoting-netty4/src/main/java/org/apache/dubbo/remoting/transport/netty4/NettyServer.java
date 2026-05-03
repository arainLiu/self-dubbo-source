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
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.model.key.MetricsKey;
import org.apache.dubbo.metrics.registry.event.NettyEvent;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelEvent;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.AbstractServer;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;
import org.apache.dubbo.remoting.transport.netty4.ssl.SslServerTlsHandler;
import org.apache.dubbo.remoting.utils.UrlUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.IO_THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.KEEP_ALIVE_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;
import static org.apache.dubbo.remoting.Constants.EVENT_LOOP_BOSS_POOL_NAME;
import static org.apache.dubbo.remoting.Constants.EVENT_LOOP_WORKER_POOL_NAME;

/**
 * NettyServer.
 */
public class NettyServer extends AbstractServer {

    /**
     * the cache for alive worker channel.
     * <ip:port, dubbo channel>
     */
    private Map<String, Channel> channels;
    /**
     * netty server bootstrap.
     */
    private ServerBootstrap bootstrap;
    /**
     * the boss channel that receive connections and dispatch these to worker channel.
     */
    private io.netty.channel.Channel channel;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private int serverShutdownTimeoutMills;

    public NettyServer(URL url, ChannelHandler handler) throws RemotingException {
        // you can customize name and type of client thread pool by THREAD_NAME_KEY and THREAD_POOL_KEY in
        // CommonConstants.
        // the handler will be wrapped: MultiMessageHandler->HeartbeatHandler->handler
        super(url, ChannelHandlers.wrap(handler, url));
    }

    /**
     * 初始化并启动Netty服务器，配置BossGroup、WorkerGroup和ChannelHandler。
     * <p>
     * 该方法是AbstractServer模板方法的具体实现，负责：
     * 1. 创建ServerBootstrap启动器；
     * 2. 创建BossGroup（接收连接）和WorkerGroup（处理IO事件）两个EventLoopGroup；
     * 3. 创建NettyServerHandler并初始化Channel管理容器；
     * 4. 配置ServerBootstrap的各项参数（如TCP参数、编解码器、Handler等）；
     * 5. 绑定监听地址并等待启动完成；
     * 6. 如果支持指标监控，采集Netty内存分配器的运行时指标数据。
     * </p>
     *
     * @throws Throwable 当服务器启动失败（如端口被占用、配置错误等）时抛出异常
     */
    @Override
    protected void doOpen() throws Throwable {
        // 创建Netty的ServerBootstrap启动器，用于配置和启动服务器
        bootstrap = new ServerBootstrap();

        // 在可能使用之前初始化serverShutdownTimeoutMills避免空指针异常，在销毁前读取配置
        serverShutdownTimeoutMills = ConfigurationUtils.getServerShutdownTimeout(getUrl().getOrDefaultModuleModel());

        // 创建BossGroup（负责接收客户端连接）和WorkerGroup（负责处理IO读写事件）
        bossGroup = createBossGroup();
        workerGroup = createWorkerGroup();

        // 创建NettyServerHandler处理器，获取其管理的Channel集合用于后续的连接管理
        final NettyServerHandler nettyServerHandler = createNettyServerHandler();
        channels = nettyServerHandler.getChannels();

        // 初始化ServerBootstrap，配置TCP参数、编解码器、Handler链等
        initServerBootstrap(nettyServerHandler);

        // 绑定监听地址并同步等待启动完成，如果失败则关闭Bootstrap并抛出异常
        try {
            ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
            channelFuture.syncUninterruptibly();
            channel = channelFuture.channel();
        } catch (Throwable t) {
            closeBootstrap();
            throw t;
        }

        // 如果支持指标监控，采集Netty内存分配器的各项运行时指标（堆内存、直接内存、Arena数量、缓存大小等）
        if (isSupportMetrics()) {
            ApplicationModel applicationModel = ApplicationModel.defaultModel();
            MetricsEventBus.post(NettyEvent.toNettyEvent(applicationModel), () -> {
                Map<String, Long> dataMap = new HashMap<>();
                dataMap.put(
                        MetricsKey.NETTY_ALLOCATOR_HEAP_MEMORY_USED.getName(),
                        PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory());
                dataMap.put(
                        MetricsKey.NETTY_ALLOCATOR_DIRECT_MEMORY_USED.getName(),
                        PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_HEAP_ARENAS_NUM.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.numHeapArenas());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_DIRECT_ARENAS_NUM.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.numDirectArenas());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_NORMAL_CACHE_SIZE.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.normalCacheSize());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_SMALL_CACHE_SIZE.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.smallCacheSize());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_THREAD_LOCAL_CACHES_NUM.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.numThreadLocalCaches());
                dataMap.put(MetricsKey.NETTY_ALLOCATOR_CHUNK_SIZE.getName(), (long)
                        PooledByteBufAllocator.DEFAULT.chunkSize());
                return dataMap;
            });
        }
    }

    private boolean isSupportMetrics() {
        return ClassUtils.isPresent("io.netty.buffer.PooledByteBufAllocatorMetric", NettyServer.class.getClassLoader());
    }

    protected EventLoopGroup createBossGroup() {
        return NettyEventLoopFactory.eventLoopGroup(1, EVENT_LOOP_BOSS_POOL_NAME);
    }

    /**
     * 创建Netty的Worker EventLoopGroup，负责处理IO读写事件。
     * <p>
     * Worker Group的线程数量由URL中的iothreads参数决定，默认值为Constants.DEFAULT_IO_THREADS。
     * 该线程池专门用于处理网络IO操作，如数据包的编解码、消息的发送和接收等。
     * </p>
     *
     * @return EventLoopGroup Worker线程组，用于处理客户端连接的IO事件
     */
    protected EventLoopGroup createWorkerGroup() {
        // 通过NettyEventLoopFactory创建EventLoopGroup，线程数从URL配置中获取（优先使用iothreads参数，否则使用默认值）
        return NettyEventLoopFactory.eventLoopGroup(
                getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS),
                EVENT_LOOP_WORKER_POOL_NAME);
    }

    protected NettyServerHandler createNettyServerHandler() {
        //NettyServerHandler -> NettyServer -> MultiMessageHandler
        // ->HeartbeatHandler -> AllChannelHandler -> DecodeHandler
        // -> HeaderExchangeHandler-> ExchangeHandlerAdapter
        return new NettyServerHandler(getUrl(), this);
    }

    /**
     * 初始化ServerBootstrap，配置TCP参数、Channel选项和Handler链。
     * <p>
     * 该方法的配置包括：
     * 1. 绑定BossGroup和WorkerGroup；
     * 2. 设置ServerSocketChannel类型（根据系统选择Epoll或NIO）；
     * 3. 配置TCP参数（地址复用、无延迟、保活等）；
     * 4. 使用池化内存分配器提升性能；
     * 5. 构建ChannelInitializer，为每个新连接添加编解码器、空闲检测和业务处理器。
     * </p>
     *
     * @param nettyServerHandler Netty服务端的核心业务处理器，负责处理RPC请求和连接事件
     */
    protected void initServerBootstrap(NettyServerHandler nettyServerHandler) {
        // 从URL中获取TCP保活配置，默认不启用keepalive
        boolean keepalive = getUrl().getParameter(KEEP_ALIVE_KEY, Boolean.FALSE);

        // 配置ServerBootstrap：绑定线程组、Channel类型、TCP选项和Handler链
        bootstrap
                .group(bossGroup, workerGroup)
                // 根据系统环境选择合适的ServerSocketChannel实现（EpollServerSocketChannel或NioServerSocketChannel）
                .channel(NettyEventLoopFactory.serverSocketChannelClass())
                // 允许端口复用（快速重启时避免地址已被占用的问题）
                .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                // 禁用Nagle算法，确保小包立即发送，降低延迟
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                // 根据配置启用TCP保活机制，检测死连接
                .childOption(ChannelOption.SO_KEEPALIVE, keepalive)
                // 使用池化字节缓冲区分配器，减少内存分配开销并提升性能
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                // 为每个新建立的连接初始化ChannelPipeline，按顺序添加SSL、编解码、空闲检测和业务处理器
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 获取连接关闭超时时间，用于配置空闲检测
                        int closeTimeout = UrlUtils.getCloseTimeout(getUrl());
                        // 创建编解码器适配器，将Dubbo的Codec2接口适配为Netty的ChannelHandler
                        NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyServer.this);

                        // 按顺序添加Pipeline处理器：
                        // 1. SSL/TLS协商处理器，处理加密连接的握手
                        ch.pipeline().addLast("negotiation", new SslServerTlsHandler(getUrl()));
                        // 2. 解码器，将字节流转换为Dubbo协议消息
                        ch.pipeline()
                                .addLast("decoder", adapter.getDecoder())
                                // 3. 编码器，将Dubbo协议消息转换为字节流
                                .addLast("encoder", adapter.getEncoder())
                                // 4. 空闲状态检测处理器，在指定时间内无读写操作时触发事件（用于超时关闭）
                                .addLast("server-idle-handler", new IdleStateHandler(0, 0, closeTimeout, MILLISECONDS))
                                // 5. 核心业务处理器，处理RPC请求和连接事件
                                .addLast("handler", nettyServerHandler);
                    }
                });
    }

    @Override
    protected void doClose() {
        try {
            if (channel != null) {
                // unbind.
                channel.close();
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
        try {
            Collection<Channel> channels = getChannels();
            if (CollectionUtils.isNotEmpty(channels)) {
                for (Channel channel : channels) {
                    try {
                        channel.close();
                    } catch (Throwable e) {
                        logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
        closeBootstrap();
        try {
            if (channels != null) {
                channels.clear();
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
    }

    private void closeBootstrap() {
        try {
            if (bootstrap != null) {
                long timeout = ConfigurationUtils.reCalShutdownTime(serverShutdownTimeoutMills);
                long quietPeriod = Math.min(2000L, timeout);
                Future<?> bossGroupShutdownFuture = bossGroup.shutdownGracefully(quietPeriod, timeout, MILLISECONDS);
                Future<?> workerGroupShutdownFuture =
                        workerGroup.shutdownGracefully(quietPeriod, timeout, MILLISECONDS);
                bossGroupShutdownFuture.syncUninterruptibly();
                workerGroupShutdownFuture.syncUninterruptibly();
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
    }

    @Override
    protected int getChannelsSize() {
        return channels.size();
    }

    @Override
    public Collection<Channel> getChannels() {
        return new ArrayList<>(channels.values());
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return channels.get(NetUtils.toAddressString(remoteAddress));
    }

    @Override
    public boolean canHandleIdle() {
        return true;
    }

    @Override
    public boolean isBound() {
        return channel.isActive();
    }

    protected EventLoopGroup getBossGroup() {
        return bossGroup;
    }

    protected EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    protected ServerBootstrap getServerBootstrap() {
        return bootstrap;
    }

    protected io.netty.channel.Channel getBossChannel() {
        return channel;
    }

    protected Map<String, Channel> getServerChannels() {
        return channels;
    }

    @Override
    public void fireChannelEvent(ChannelEvent event) {
        Collection<Channel> channels = getChannels();
        if (CollectionUtils.isEmpty(channels)) {
            return;
        }
        for (Channel channel : channels) {
            try {
                if (channel.isConnected()) {
                    fireChannelEventToChannel(channel, event);
                }
            } catch (Throwable e) {
                logger.warn(
                        TRANSPORT_FAILED_CLOSE,
                        "",
                        "",
                        "Failed to fire channel event to channel: " + channel + ", event: " + event,
                        e);
            }
        }
    }

    /**
     * Fire ChannelEvent to the channel.
     * The event will be handled by protocol-specific handlers.
     *
     * @param channel the Dubbo channel
     * @param event the channel event to fire
     */
    private void fireChannelEventToChannel(Channel channel, ChannelEvent event) {
        if (channel instanceof NettyChannel) {
            io.netty.channel.Channel nettyChannel = ((NettyChannel) channel).getNioChannel();
            if (nettyChannel != null && nettyChannel.isActive()) {
                nettyChannel.pipeline().fireUserEventTriggered(event);
            }
        }
    }
}
