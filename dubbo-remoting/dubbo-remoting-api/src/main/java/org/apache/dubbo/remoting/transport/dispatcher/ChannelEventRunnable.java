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
package org.apache.dubbo.remoting.transport.dispatcher;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadlocal.InternalThreadLocalMap;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;

public class ChannelEventRunnable implements Runnable {
    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(ChannelEventRunnable.class);

    private final ChannelHandler handler;
    private final Channel channel;
    private final ChannelState state;
    private final Throwable exception;
    private final Object message;

    public ChannelEventRunnable(Channel channel, ChannelHandler handler, ChannelState state) {
        this(channel, handler, state, null);
    }

    public ChannelEventRunnable(Channel channel, ChannelHandler handler, ChannelState state, Object message) {
        this(channel, handler, state, message, null);
    }

    public ChannelEventRunnable(Channel channel, ChannelHandler handler, ChannelState state, Throwable t) {
        this(channel, handler, state, null, t);
    }

    public ChannelEventRunnable(
            Channel channel, ChannelHandler handler, ChannelState state, Object message, Throwable exception) {
        this.channel = channel;
        this.handler = handler;
        this.state = state;
        this.message = message;
        this.exception = exception;
    }

       /**
     * 执行通道事件处理逻辑
     * 根据当前通道状态（CONNECTED、DISCONNECTED、RECEIVED、SENT、CAUGHT）调用对应的处理器方法
     * 在任务执行前后管理InternalThreadLocalMap的生命周期，确保线程局部变量正确清理和恢复
     */
    @Override
    public void run() {
        // 获取并移除当前线程的ThreadLocalMap，用于后续恢复
        InternalThreadLocalMap internalThreadLocalMap = InternalThreadLocalMap.getAndRemove();
        try {
            // 处理消息接收事件，这是最频繁的事件类型
            if (state == ChannelState.RECEIVED) {
                try {
                    handler.received(channel, message);
                } catch (Exception e) {
                    logger.warn(
                            INTERNAL_ERROR,
                            "unknown error in remoting module",
                            "",
                            "ChannelEventRunnable handle " + state + " operation error, channel is " + channel
                                    + ", message is " + message,
                            e);
                }
            } else {
                // 处理其他类型的通道状态变化事件
                switch (state) {
                    case CONNECTED:
                        // 处理通道连接建立事件
                        try {
                            handler.connected(channel);
                        } catch (Exception e) {
                            logger.warn(
                                    INTERNAL_ERROR,
                                    "unknown error in remoting module",
                                    "",
                                    "ChannelEventRunnable handle " + state + " operation error, channel is " + channel,
                                    e);
                        }
                        break;
                    case DISCONNECTED:
                        // 处理通道连接断开事件
                        try {
                            handler.disconnected(channel);
                        } catch (Exception e) {
                            logger.warn(
                                    INTERNAL_ERROR,
                                    "unknown error in remoting module",
                                    "",
                                    "ChannelEventRunnable handle " + state + " operation error, channel is " + channel,
                                    e);
                        }
                        break;
                    case SENT:
                        // 处理消息发送完成事件
                        try {
                            handler.sent(channel, message);
                        } catch (Exception e) {
                            logger.warn(
                                    INTERNAL_ERROR,
                                    "unknown error in remoting module",
                                    "",
                                    "ChannelEventRunnable handle " + state + " operation error, channel is " + channel
                                            + ", message is " + message,
                                    e);
                        }
                        break;
                    case CAUGHT:
                        // 处理通道中捕获的异常事件
                        try {
                            handler.caught(channel, exception);
                        } catch (Exception e) {
                            logger.warn(
                                    INTERNAL_ERROR,
                                    "unknown error in remoting module",
                                    "",
                                    "ChannelEventRunnable handle " + state + " operation error, channel is " + channel
                                            + ", message is: " + message + ", exception is " + exception,
                                    e);
                        }
                        break;
                    default:
                        // 记录未识别的通道状态，属于异常情况
                        logger.warn(
                                INTERNAL_ERROR,
                                "unknown error in remoting module",
                                "",
                                "unknown state: " + state + ", message is " + message);
                }
            }
        } finally {
            // 恢复线程的ThreadLocalMap，确保线程复用时的上下文正确性
            InternalThreadLocalMap.set(internalThreadLocalMap);
        }
    }

    /**
     * ChannelState
     */
    public enum ChannelState {

        /**
         * CONNECTED
         */
        CONNECTED,

        /**
         * DISCONNECTED
         */
        DISCONNECTED,

        /**
         * SENT
         */
        SENT,

        /**
         * RECEIVED
         */
        RECEIVED,

        /**
         * CAUGHT
         */
        CAUGHT
    }
}
