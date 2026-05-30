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

import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;

/**
 * ChannelHandlerDelegate 的抽象基类实现。
 * 该类采用了委托模式（Delegate Pattern），内部持有一个 ChannelHandler 实例，
 * 并将所有的网络事件（连接、断开、发送、接收、异常）默认委托给该实例处理。
 *
 * 它的主要作用是作为 Handler 链中的中间层，允许子类通过重写特定方法来增强或拦截某些事件，
 * 而无需重新实现所有接口方法。例如 HeartbeatHandler 和 MultiMessageHandler 都继承自此类。
 */
public abstract class AbstractChannelHandlerDelegate implements ChannelHandlerDelegate {

    protected ChannelHandler handler;

    /**
     * 构造 AbstractChannelHandlerDelegate 实例。
     *
     * @param handler 被委托的下层 ChannelHandler 实例，不能为空
     * @throws IllegalArgumentException 如果 handler 为空
     */
    protected AbstractChannelHandlerDelegate(ChannelHandler handler) {
        Assert.notNull(handler, "handler == null");
        this.handler = handler;
    }

    /**
     * 获取最终的原始 ChannelHandler。
     * 如果当前的 handler 也是一个代理（Delegate），则会递归解包，直到找到最内层的非代理 Handler。
     *
     * @return 最底层的 ChannelHandler 实例
     */
    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        }
        return handler;
    }

    /**
     * 处理连接建立事件。
     * 默认直接委托给下层 handler 处理。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void connected(Channel channel) throws RemotingException {
        handler.connected(channel);
    }

    /**
     * 处理连接断开事件。
     * 默认直接委托给下层 handler 处理。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void disconnected(Channel channel) throws RemotingException {
        handler.disconnected(channel);
    }

    /**
     * 处理消息发送完成事件。
     * 默认直接委托给下层 handler 处理。
     *
     * @param channel 发生事件的通道
     * @param message 发送的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        handler.sent(channel, message);
    }

    /**
     * 处理接收到的消息事件。
     * 默认直接委托给下层 handler 处理。
     *
     * @param channel 发生事件的通道
     * @param message 接收到的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        handler.received(channel, message);
    }

    /**
     * 处理捕获到的异常事件。
     * 默认直接委托给下层 handler 处理。
     *
     * @param channel 发生事件的通道
     * @param exception 捕获到的异常对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        handler.caught(channel, exception);
    }
}
