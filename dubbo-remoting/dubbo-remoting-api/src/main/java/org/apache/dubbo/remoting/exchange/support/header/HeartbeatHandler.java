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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.HeartBeatRequest;
import org.apache.dubbo.remoting.exchange.HeartBeatResponse;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.transport.AbstractChannelHandlerDelegate;

import static org.apache.dubbo.common.constants.CommonConstants.HEARTBEAT_EVENT;

/**
 * 心跳处理器，负责管理通道的读写时间戳并处理心跳请求与响应。
 * 该类继承自 AbstractChannelHandlerDelegate，在委托给下层处理器之前或之后执行以下逻辑：
 * 1. 记录通道最后一次读取和写入消息的时间戳，用于空闲检测（Idle Check）
 * 2. 拦截心跳请求（HeartBeatRequest），如果是双向通信则自动回复心跳响应
 * 3. 拦截心跳响应（HeartBeatResponse），防止其继续向上传递给业务处理器
 */
public class HeartbeatHandler extends AbstractChannelHandlerDelegate {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

    /**
     * 通道属性键：最后一次成功读取数据的时间戳
     */
    public static final String KEY_READ_TIMESTAMP = "READ_TIMESTAMP";

    /**
     * 通道属性键：最后一次成功写入数据的时间戳
     */
    public static final String KEY_WRITE_TIMESTAMP = "WRITE_TIMESTAMP";

    /**
     * 构造心跳处理器实例。
     *
     * @param handler 需要被包装的下层 ChannelHandler
     */
    public HeartbeatHandler(ChannelHandler handler) {
        super(handler);
    }

    /**
     * 处理连接建立事件。
     * 在连接建立时初始化读写时间戳，确保后续的空闲检测能正常工作。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void connected(Channel channel) throws RemotingException {
        setReadTimestamp(channel);
        setWriteTimestamp(channel);
        handler.connected(channel);
    }

    /**
     * 处理连接断开事件。
     * 在连接断开时清除读写时间戳，释放资源。
     *
     * @param channel 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void disconnected(Channel channel) throws RemotingException {
        clearReadTimestamp(channel);
        clearWriteTimestamp(channel);
        handler.disconnected(channel);
    }

    /**
     * 处理消息发送完成事件。
     * 每次发送消息后更新写时间戳，用于监控通道的活跃状态。
     *
     * @param channel 发生事件的通道
     * @param message 发送的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        setWriteTimestamp(channel);
        handler.sent(channel, message);
    }

    /**
     * 处理接收到的消息事件。
     * 该方法会首先更新读时间戳，然后判断消息类型：
     * 1. 如果是心跳请求且为双向通信，则构造并发送心跳响应，不再向下传递。
     * 2. 如果是心跳响应，则直接拦截，不再向下传递。
     * 3. 如果是普通业务消息，则委托给下层处理器处理。
     *
     * @param channel 发生事件的通道
     * @param message 接收到的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 更新最后读取时间戳
        setReadTimestamp(channel);
        // 判断是否为心跳请求
        if (isHeartbeatRequest(message)) {
            HeartBeatRequest req = (HeartBeatRequest) message;
            // 如果是双向心跳请求，则需要回复响应
            if (req.isTwoWay()) {
                HeartBeatResponse res;
                res = new HeartBeatResponse(req.getId(), req.getVersion());
                res.setEvent(HEARTBEAT_EVENT);
                res.setProto(req.getProto());
                channel.send(res);
                if (logger.isDebugEnabled()) {
                    int heartbeat = channel.getUrl().getParameter(Constants.HEARTBEAT_KEY, 0);
                    logger.debug("Received heartbeat from remote channel " + channel.getRemoteAddress()
                            + ", cause: The channel has no data-transmission exceeds a heartbeat period"
                            + (heartbeat > 0 ? ": " + heartbeat + "ms" : ""));
                }
            }
            return;
        }
        // 判断是否为心跳响应，如果是则直接消费掉，不传递给业务层
        if (isHeartbeatResponse(message)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Receive heartbeat response in thread "
                        + Thread.currentThread().getName());
            }
            return;
        }
        // 普通业务消息，委托给下层处理器
        handler.received(channel, message);
    }

    /**
     * 设置通道的最后读取时间戳为当前时间。
     *
     * @param channel 目标通道
     */
    private void setReadTimestamp(Channel channel) {
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
    }

    /**
     * 设置通道的最后写入时间戳为当前时间。
     *
     * @param channel 目标通道
     */
    private void setWriteTimestamp(Channel channel) {
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
    }

    /**
     * 清除通道的读取时间戳。
     *
     * @param channel 目标通道
     */
    private void clearReadTimestamp(Channel channel) {
        channel.removeAttribute(KEY_READ_TIMESTAMP);
    }

    /**
     * 清除通道的写入时间戳。
     *
     * @param channel 目标通道
     */
    private void clearWriteTimestamp(Channel channel) {
        channel.removeAttribute(KEY_WRITE_TIMESTAMP);
    }

    /**
     * 判断消息是否为心跳请求。
     *
     * @param message 待判断的消息对象
     * @return 如果是心跳请求返回 true，否则返回 false
     */
    private boolean isHeartbeatRequest(Object message) {
        return message instanceof HeartBeatRequest && ((Request) message).isHeartbeat();
    }

    /**
     * 判断消息是否为心跳响应。
     *
     * @param message 待判断的消息对象
     * @return 如果是心跳响应返回 true，否则返回 false
     */
    private boolean isHeartbeatResponse(Object message) {
        return message instanceof Response && ((Response) message).isHeartbeat();
    }
}
