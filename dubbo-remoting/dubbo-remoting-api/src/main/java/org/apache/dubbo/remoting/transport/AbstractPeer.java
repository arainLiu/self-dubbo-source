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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.Endpoint;
import org.apache.dubbo.remoting.RemotingException;

/**
 * Endpoint 和 ChannelHandler 的抽象基类实现。
 * 该类提供了端点（Endpoint）的基本属性和生命周期管理（如 URL、关闭状态），
 * 并实现了 ChannelHandler 接口，将网络连接事件委托给内部持有的 handler 处理。
 *
 * 它是 Dubbo Remoting 层中 Client 和 Server 实现的通用父类，负责：
 * 1. 管理配置 URL 和处理器 Handler
 * 2. 维护端点的关闭状态（closing/closed）
 * 3. 在处理网络事件前进行状态检查，防止在关闭状态下处理业务逻辑
 */
public abstract class AbstractPeer implements Endpoint, ChannelHandler {

    private final ChannelHandler handler;

    private volatile URL url;

    // closing closed means the process is being closed and close is finished
    // closing 表示正在关闭过程中，closed 表示关闭已完成
    private volatile boolean closing;

    private volatile boolean closed;

    /**
     * 构造 AbstractPeer 实例。
     *
     * @param url 端点的配置 URL，不能为空
     * @param handler 处理网络事件的 ChannelHandler，不能为空
     * @throws IllegalArgumentException 如果 url 或 handler 为空
     */
    public AbstractPeer(URL url, ChannelHandler handler) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.url = url;
        this.handler = handler;
    }

    protected AbstractPeer() {
        handler = null;
    }

    @Override
    public void send(Object message) throws RemotingException {
        send(message, url.getParameter(Constants.SENT_KEY, false));
    }

    /**
     * 关闭当前端点。
     * 该方法仅设置关闭标志位，具体的资源释放由子类实现。
     */
    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void close(int timeout) {
        close();
    }

    /**
     * 启动关闭流程。
     * 将端点标记为正在关闭状态（closing），如果已经关闭则直接返回。
     */
    @Override
    public void startClose() {
        if (isClosed()) {
            return;
        }
        closing = true;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    /**
     * 设置端点的配置 URL。
     *
     * @param url 新的配置 URL，不能为空
     * @throws IllegalArgumentException 如果 url 为空
     */
    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        this.url = url;
    }

    /**
     * 获取当前端点的 ChannelHandler。
     * 如果 handler 是代理类型，则解包获取最内层的原始 handler。
     *
     * @return 实际的 ChannelHandler 实例
     */
    @Override
    public ChannelHandler getChannelHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }

    /**
     * @return ChannelHandler
     * @deprecated 使用 {@link #getChannelHandler()} 或 {@link #getDelegateHandler()} 代替
     */
    @Deprecated
    public ChannelHandler getHandler() {
        return getDelegateHandler();
    }

    /**
     * Return the final handler (which may have been wrapped). This method should be distinguished with getChannelHandler() method
     * 获取原始的处理器实例，即使它被包装过也不进行解包。
     *
     * @return 原始的 ChannelHandler 实例
     */
    public ChannelHandler getDelegateHandler() {
        return handler;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * 判断端点是否正处于关闭过程中但尚未完全关闭。
     *
     * @return 如果处于 closing 状态且未 closed 则返回 true
     */
    public boolean isClosing() {
        return closing && !closed;
    }

    /**
     * 处理连接建立事件。
     * 如果端点已关闭，则忽略该事件；否则委托给内部 handler 处理。
     *
     * @param ch 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void connected(Channel ch) throws RemotingException {
        if (closed) {
            return;
        }
        handler.connected(ch);
    }

    /**
     * 处理连接断开事件。
     *
     * @param ch 发生事件的通道
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void disconnected(Channel ch) throws RemotingException {
        handler.disconnected(ch);
    }

    /**
     * 处理消息发送完成事件。
     * 如果端点已关闭，则忽略该事件；否则委托给内部 handler 处理。
     *
     * @param ch 发生事件的通道
     * @param msg 发送的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void sent(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.sent(ch, msg);
    }

    /**
     * 处理接收到的消息事件。
     * 如果端点已关闭，则忽略该事件；否则委托给内部 handler 处理。
     *
     * @param ch 发生事件的通道
     * @param msg 接收到的消息对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void received(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.received(ch, msg);
    }

    /**
     * 处理捕获到的异常事件。
     * 无论端点是否关闭，都会将异常传递给 handler，以便进行必要的清理或日志记录。
     *
     * @param ch 发生事件的通道
     * @param ex 捕获到的异常对象
     * @throws RemotingException 处理过程中发生的异常
     */
    @Override
    public void caught(Channel ch, Throwable ex) throws RemotingException {
        handler.caught(ch, ex);
    }
}
