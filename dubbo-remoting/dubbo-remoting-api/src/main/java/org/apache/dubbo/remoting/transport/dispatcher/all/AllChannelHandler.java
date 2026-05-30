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
package org.apache.dubbo.remoting.transport.dispatcher.all;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable.ChannelState;
import org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 全派发通道处理器。
 * 该处理器将所有网络事件（连接、断开、接收消息、捕获异常）都派发到线程池中异步执行。
 * 它是 Dubbo 默认的分发策略（dispatcher=all），旨在避免在 IO 线程中执行耗时业务逻辑，从而提高系统的吞吐量。
 */
public class AllChannelHandler extends WrappedChannelHandler {

    /**
     * 构造全派发通道处理器实例。
     *
     * @param handler 需要被包装的下层 ChannelHandler
     * @param url 服务 URL，包含线程池配置等信息
     */
    public AllChannelHandler(ChannelHandler handler, URL url) {
        super(handler, url);
    }

    /**
     * 处理连接建立事件。
     * 将连接事件封装为 ChannelEventRunnable 并提交到共享线程池中执行。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 如果提交任务失败或执行过程中发生异常
     */
    @Override
    public void connected(Channel channel) throws RemotingException {
        // 获取共享的线程池
        ExecutorService executor = getSharedExecutorService();
        try {
            // 将连接事件派发到线程池
            executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.CONNECTED));
        } catch (Throwable t) {
            throw new ExecutionException(
                    "connect event", channel, getClass() + " error when process connected event .", t);
        }
    }

    /**
     * 处理连接断开事件。
     * 将断开事件封装为 ChannelEventRunnable 并提交到共享线程池中执行。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 如果提交任务失败或执行过程中发生异常
     */
    @Override
    public void disconnected(Channel channel) throws RemotingException {
        // 获取共享的线程池
        ExecutorService executor = getSharedExecutorService();
        try {
            // 将断开事件派发到线程池
            executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.DISCONNECTED));
        } catch (Throwable t) {
            throw new ExecutionException(
                    "disconnect event", channel, getClass() + " error when process disconnected event .", t);
        }
    }

    /**
     * 处理接收到的消息事件。
     * 根据消息类型获取对应的线程池（通常是业务线程池），并将消息处理逻辑派发到线程池中执行。
     * 如果线程池已满且消息是 Request 类型，则会向发送方反馈拒绝执行的错误，而不是直接抛出异常。
     *
     * @param channel 发生事件的通道
     * @param message 接收到的消息对象
     * @throws RemotingException 如果提交任务失败或执行过程中发生非拒绝执行异常
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 根据消息内容获取最合适的线程池（例如根据服务名匹配专用线程池）
        ExecutorService executor = getPreferredExecutorService(message);
        try {
            // 将消息接收事件派发到线程池
            executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.RECEIVED, message));
        } catch (Throwable t) {
            // 如果是请求消息且因为线程池满被拒绝，则发送错误响应给客户端，避免客户端超时
            if (message instanceof Request && t instanceof RejectedExecutionException) {
                sendFeedback(channel, (Request) message, t);
                return;
            }
            throw new ExecutionException(message, channel, getClass() + " error when process received event .", t);
        }
    }

    /**
     * 处理捕获到的异常事件。
     * 将异常事件封装为 ChannelEventRunnable 并提交到共享线程池中执行。
     *
     * @param channel 发生事件的通道
     * @param exception 捕获到的异常对象
     * @throws RemotingException 如果提交任务失败或执行过程中发生异常
     */
    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        // 获取共享的线程池
        ExecutorService executor = getSharedExecutorService();
        try {
            // 将异常处理事件派发到线程池
            executor.execute(new ChannelEventRunnable(channel, handler, ChannelState.CAUGHT, exception));
        } catch (Throwable t) {
            throw new ExecutionException("caught event", channel, getClass() + " error when process caught event .", t);
        }
    }
}
