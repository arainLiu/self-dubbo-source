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

import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.remoting.Constants;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import static org.apache.dubbo.common.constants.CommonConstants.OS_LINUX_PREFIX;
import static org.apache.dubbo.common.constants.CommonConstants.SystemProperty.SYSTEM_OS_NAME;
import static org.apache.dubbo.common.constants.CommonConstants.ThirdPartyProperty.NETTY_EPOLL_ENABLE_KEY;

public class NettyEventLoopFactory {
    /**
     * netty client bootstrap
     */
    public static final GlobalResourceInitializer<EventLoopGroup> NIO_EVENT_LOOP_GROUP =
            new GlobalResourceInitializer<>(
                    () -> eventLoopGroup(Constants.DEFAULT_IO_THREADS, "NettyClientWorker"),
                    eventLoopGroup -> eventLoopGroup.shutdownGracefully());

    /**
     * 创建EventLoopGroup，根据操作系统环境自动选择Epoll或NIO实现。
     * <p>
     * 该方法的处理逻辑：
     * 1. 创建专用的线程工厂，用于生成EventLoopGroup中的线程；
     * 2. 通过shouldEpoll()判断当前系统是否支持且启用了Epoll（Linux系统专属）；
     * 3. 如果支持Epoll，使用EpollEventLoopGroup（高性能的异步IO模型）；
     * 4. 否则使用NioEventLoopGroup（基于Java NIO的标准实现）。
     * </p>
     *
     * @param threads EventLoopGroup中的线程数量，决定并发处理能力
     * @param threadFactoryName 线程工厂名称，用于线程命名和标识
     * @return EventLoopGroup 根据系统环境选择的最佳EventLoopGroup实现（Epoll或NIO）
     */
    public static EventLoopGroup eventLoopGroup(int threads, String threadFactoryName) {
        // 创建线程工厂，第二个参数true表示守护线程模式
        ThreadFactory threadFactory = new DefaultThreadFactory(threadFactoryName, true);

        // 根据系统环境判断是否使用Epoll（Linux系统的高性能IO多路复用机制），否则降级为NIO
        if (shouldEpoll()) {
            return new EpollEventLoopGroup(threads, threadFactory);
        } else {
            return new NioEventLoopGroup(threads, threadFactory);
        }
    }

    public static Class<? extends SocketChannel> socketChannelClass() {
        return shouldEpoll() ? EpollSocketChannel.class : NioSocketChannel.class;
    }

    public static Class<? extends ServerSocketChannel> serverSocketChannelClass() {
        return shouldEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    private static boolean shouldEpoll() {
        if (Boolean.parseBoolean(SystemPropertyConfigUtils.getSystemProperty(NETTY_EPOLL_ENABLE_KEY, "false"))) {
            String osName = SystemPropertyConfigUtils.getSystemProperty(SYSTEM_OS_NAME);
            return osName.toLowerCase().contains(OS_LINUX_PREFIX) && Epoll.isAvailable();
        }

        return false;
    }
}
