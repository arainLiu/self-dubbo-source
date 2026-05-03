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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;
import org.apache.dubbo.remoting.transport.ChannelHandlerDispatcher;

/**
 * Transporter facade. (API, Static, ThreadSafe)
 */
public class Transporters {

    private Transporters() {}

    public static RemotingServer bind(String url, ChannelHandler... handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 绑定URL地址并创建RemotingServer，提供底层的网络通信能力。
     * <p>
     * 该方法是Transport层的工厂方法，负责：
     * 1. 验证参数合法性（URL和handlers不能为空）；
     * 2. 将多个ChannelHandler合并为单个处理器（使用ChannelHandlerDispatcher）；
     * 3. 根据URL配置选择对应的Transporter扩展实现；
     * 4. 调用Transporter.bind完成服务器绑定。
     * </p>
     *
     * @param url 服务器绑定的URL地址，包含host、port、编解码器等配置信息
     * @param handlers ChannelHandler数组，支持传入多个处理器用于责任链处理
     * @return RemotingServer对象，提供基础网络通信能力的服务器实例
     * @throws IllegalArgumentException 当URL为空或handlers数组为空/长度为0时抛出
     * @throws RemotingException 当服务器绑定失败（如端口被占用、网络异常等）时抛出
     */
    public static RemotingServer bind(URL url, ChannelHandler... handlers) throws RemotingException {
        // 参数校验：确保URL和handlers不为空
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers == null");
        }

        ChannelHandler handler;
        // 根据handlers数量决定直接使用或包装为ChannelHandlerDispatcher（支持多处理器分发）
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            handler = new ChannelHandlerDispatcher(handlers);
        }

        // 根据URL配置获取对应的Transporter扩展实现（如NettyTransporter），执行实际的服务器绑定操作
        return getTransporter(url).bind(url, handler);
    }


    public static Client connect(String url, ChannelHandler... handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    public static Client connect(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        ChannelHandler handler;
        if (handlers == null || handlers.length == 0) {
            handler = new ChannelHandlerAdapter();
        } else if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            handler = new ChannelHandlerDispatcher(handlers);
        }
        return getTransporter(url).connect(url, handler);
    }

    public static Transporter getTransporter(URL url) {
        return url.getOrDefaultFrameworkModel()
                .getExtensionLoader(Transporter.class)
                .getAdaptiveExtension();
    }
}
