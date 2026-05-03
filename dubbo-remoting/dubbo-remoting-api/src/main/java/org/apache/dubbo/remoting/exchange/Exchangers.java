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
package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerDispatcher;
import org.apache.dubbo.remoting.exchange.support.Replier;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;

/**
 * Exchanger facade. (API, Static, ThreadSafe)
 */
public class Exchangers {
    private Exchangers() {}

    public static ExchangeServer bind(String url, Replier<?> replier) throws RemotingException {
        return bind(URL.valueOf(url), replier);
    }

    public static ExchangeServer bind(URL url, Replier<?> replier) throws RemotingException {
        return bind(url, new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeServer bind(String url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return bind(URL.valueOf(url), handler, replier);
    }

    public static ExchangeServer bind(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return bind(url, new ExchangeHandlerDispatcher(url.getOrDefaultFrameworkModel(), replier, handler));
    }

    public static ExchangeServer bind(String url, ExchangeHandler handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 绑定URL地址并创建ExchangeServer，提供请求-响应模式的网络服务。
     * <p>
     * 该方法是ExchangeServer的工厂方法，负责：
     * 1. 验证参数合法性（URL和Handler不能为空）；
     * 2. 为URL添加默认的exchange编解码器标识；
     * 3. 根据URL配置选择对应的Exchanger扩展实现；
     * 4. 调用Exchanger.bind完成服务器绑定。
     * </p>
     *
     * @param url 服务器绑定的URL地址，包含host、port、编解码器等配置信息
     * @param handler 交换层处理器，负责处理客户端请求并返回响应结果
     * @return ExchangeServer对象，支持双向通信的网络服务器实例
     * @throws IllegalArgumentException 当URL或handler参数为空时抛出
     * @throws RemotingException 当服务器绑定失败（如端口被占用、网络异常等）时抛出
     */
    public static ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        // 参数校验：确保URL和ExchangeHandler不为空
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }

        // 为URL添加默认的exchange编解码器（用于区分传输层和交换层的编码逻辑）
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        // 根据URL配置获取对应的Exchanger扩展实现（如HeaderExchanger），执行实际的服务器绑定操作
        return getExchanger(url).bind(url, handler);
    }


    public static ExchangeClient connect(String url) throws RemotingException {
        return connect(URL.valueOf(url));
    }

    public static ExchangeClient connect(URL url) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), null);
    }

    public static ExchangeClient connect(String url, Replier<?> replier) throws RemotingException {
        return connect(URL.valueOf(url), new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeClient connect(URL url, Replier<?> replier) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeClient connect(String url, ChannelHandler handler, Replier<?> replier)
            throws RemotingException {
        return connect(URL.valueOf(url), handler, replier);
    }

    public static ExchangeClient connect(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return connect(url, new ExchangeHandlerDispatcher(url.getOrDefaultFrameworkModel(), replier, handler));
    }

    public static ExchangeClient connect(String url, ExchangeHandler handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    public static ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        return getExchanger(url).connect(url, handler);
    }

    public static Exchanger getExchanger(URL url) {
        String type = url.getParameter(Constants.EXCHANGER_KEY, Constants.DEFAULT_EXCHANGER);
        return url.getOrDefaultFrameworkModel()
                .getExtensionLoader(Exchanger.class)
                .getExtension(type);
    }
}
