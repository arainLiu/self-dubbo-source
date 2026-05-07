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
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;
import static org.apache.dubbo.common.constants.CommonConstants.WRITEABLE_EVENT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_RESPONSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_UNSUPPORTED_MESSAGE;

/**
 * ExchangeReceiver
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(HeaderExchangeHandler.class);

    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    static void handleResponse(Channel channel, Response response) throws RemotingException {
        if (response != null && !response.isHeartbeat()) {
            DefaultFuture.received(channel, response);
        }
    }

    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort()
                && NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    void handlerEvent(Channel channel, Request req) throws RemotingException {
        if (req.getData() != null && req.getData().equals(READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
            logger.info("ChannelReadOnly set true for channel: " + channel);
        }
        if (req.getData() != null && req.getData().equals(WRITEABLE_EVENT)) {
            channel.removeAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY);
            logger.info("ChannelReadOnly set false for channel: " + channel);
        }
    }

    /**
     * 处理接收到的请求消息，构建响应并返回给调用方
     * 支持对损坏请求的错误反馈，以及通过异步回调处理正常的业务逻辑调用
     *
     * @param channel 交换通道对象，用于发送响应消息
     * @param req 请求对象，包含请求ID、版本信息及具体的业务数据
     * @throws RemotingException 当远程通信过程中发生异常时抛出
     */
    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        // 创建与请求对应的响应对象，关联请求ID和协议版本
        Response res = new Response(req.getId(), req.getVersion());

        // 如果请求在解码阶段已标记为损坏，则构建错误响应并直接返回
        if (req.isBroken()) {
            Object data = req.getData();

            String msg;
            if (data == null) {
                msg = null;
            } else if (data instanceof Throwable) {
                msg = StringUtils.toString((Throwable) data);
            } else {
                msg = data.toString();
            }
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            channel.send(res);
            return;
        }

        // 获取请求中的业务数据，并交由下层处理器执行
        Object msg = req.getData();
        try {
            // 触发业务逻辑处理，获取异步执行结果
            CompletionStage<Object> future = handler.reply(channel, msg);

            // 注册异步回调，在业务逻辑执行完成后构建并发送响应
            future.whenComplete((appResult, t) -> {
                try {
                    if (t == null) {
                        // 业务执行成功，设置响应状态和结果值
                        res.setStatus(Response.OK);
                        res.setResult(appResult);
                    } else {
                        // 业务执行抛出异常，设置服务错误状态和异常信息
                        res.setStatus(Response.SERVICE_ERROR);
                        res.setErrorMessage(StringUtils.toString(t));
                    }
                    channel.send(res);
                } catch (RemotingException e) {
                    // 记录响应发送失败的警告日志，避免影响主流程
                    logger.warn(
                            TRANSPORT_FAILED_RESPONSE,
                            "",
                            "",
                            "Send result to consumer failed, channel is " + channel + ", msg is " + e);
                }
            });
        } catch (Throwable e) {
            // 捕获同步执行阶段的异常（如找不到处理器），立即返回服务错误响应
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        handler.connected(exchangeChannel);
        channel.setAttribute(
                Constants.CHANNEL_SHUTDOWN_TIMEOUT_KEY,
                ConfigurationUtils.getServerShutdownTimeout(channel.getUrl().getOrDefaultApplicationModel()));
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            int shutdownTimeout = 0;
            Object timeoutObj = channel.getAttribute(Constants.CHANNEL_SHUTDOWN_TIMEOUT_KEY);
            if (timeoutObj instanceof Integer) {
                shutdownTimeout = (Integer) timeoutObj;
            }
            DefaultFuture.closeChannel(channel, ConfigurationUtils.reCalShutdownTime(shutdownTimeout));
            HeaderExchangeChannel.removeChannel(channel);
        }
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            handler.sent(exchangeChannel, message);
        } catch (Throwable t) {
            exception = t;
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
        if (message instanceof Request) {
            Request request = (Request) message;
            DefaultFuture.sent(channel, request);
        }
        if (message instanceof MultiMessage) {
            MultiMessage multiMessage = (MultiMessage) message;
            for (Object single : multiMessage) {
                if (single instanceof Request) {
                    DefaultFuture.sent(channel, ((Request) single));
                }
            }
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(
                        channel.getLocalAddress(), channel.getRemoteAddress(), exception.getMessage(), exception);
            }
        }
    }

    /**
     * 处理接收到的网络消息，根据消息类型分发到不同的处理逻辑。
     * <p>
     * 该方法支持的消息类型和处理策略：
     * 1. Request（请求）：
     *    - 事件消息（如心跳、连接事件）：调用handlerEvent处理；
     *    - 双向请求（需要响应）：调用handleRequest执行业务逻辑并返回响应；
     *    - 单向请求（不需要响应）：直接调用handler.received处理。
     * 2. Response（响应）：调用handleResponse处理服务端返回的结果。
     * 3. String（字符串）：
     *    - 客户端：不支持字符串消息，记录错误日志；
     *    - 服务端：作为telnet命令处理，支持运维调试功能。
     * 4. 其他类型：直接交给下层handler处理。
     * </p>
     *
     * @param channel 网络通道对象，代表与对端的连接
     * @param message 接收到的消息对象，可能是Request、Response、String或其他类型
     * @throws RemotingException 当消息处理失败时抛出异常
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        // 从底层Channel获取或创建ExchangeChannel，提供请求-响应模式的高级抽象
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);

        // 根据消息类型执行不同的处理逻辑
        if (message instanceof Request) {
            // 处理Request请求消息
            Request request = (Request) message;
            if (request.isEvent()) {
                // 处理事件消息（如心跳检测、连接/断开事件等），不需要业务响应
                handlerEvent(channel, request);
            } else {
                // 处理业务请求消息
                if (request.isTwoWay()) {
                    // 双向请求：需要执行业务逻辑并返回响应结果给调用方
                    handleRequest(exchangeChannel, request);
                } else {
                    // 单向请求：不需要返回响应，直接交给下层处理器处理
                    handler.received(exchangeChannel, request.getData());
                }
            }
        } else if (message instanceof Response) {
            // 处理Response响应消息（客户端接收服务端的返回结果）
            handleResponse(channel, (Response) message);
        } else if (message instanceof String) {
            // 处理字符串消息：主要用于telnet命令行交互
            if (isClientSide(channel)) {
                // 客户端侧：Dubbo协议不支持接收字符串消息，记录错误日志
                Exception e = new Exception("Dubbo client can not supported string message: " + message
                        + " in channel: " + channel + ", url: " + channel.getUrl());
                logger.error(TRANSPORT_UNSUPPORTED_MESSAGE, "", "", e.getMessage(), e);
            } else {
                // 服务端侧：将字符串作为telnet命令处理，支持运维人员远程调试和管理
                String echo = handler.telnet(channel, (String) message);
                if (StringUtils.isNotEmpty(echo)) {
                    // 如果有输出结果，发送回客户端
                    channel.send(echo);
                }
            }
        } else {
            // 处理其他类型的消息，直接委托给下层handler处理
            handler.received(exchangeChannel, message);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
