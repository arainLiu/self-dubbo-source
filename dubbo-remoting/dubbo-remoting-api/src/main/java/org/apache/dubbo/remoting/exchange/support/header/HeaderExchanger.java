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
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporters;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchanger;
import org.apache.dubbo.remoting.exchange.PortUnificationExchanger;
import org.apache.dubbo.remoting.transport.DecodeHandler;

import static org.apache.dubbo.remoting.Constants.IS_PU_SERVER_KEY;

/**
 * DefaultMessenger
 *
 *
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        return new HeaderExchangeClient(
                Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))), true);
    }

    /**
     * 创建HeaderExchangeServer，包装底层Transport层并添加请求-响应交换能力。
     * <p>
     * 该方法根据URL配置选择两种不同的服务器绑定策略：
     * 1. 端口统一服务器（PortUnification）：支持多种协议的自动识别和切换；
     * 2. 标准Transport服务器：直接使用Transporter实现进行绑定。
     * 两种策略都会通过DecodeHandler和HeaderExchangeHandler构建完整的消息处理链。
     * </p>
     *
     * @param url 服务器配置的URL对象，包含地址、端口、协议类型等信息
     * @param handler 交换层处理器，负责处理具体的RPC请求和响应逻辑
     * @return HeaderExchangeServer对象，提供双向通信能力的网络服务器
     * @throws RemotingException 当服务器启动失败（如端口占用、协议不支持等）时抛出
     */
    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        ExchangeServer server;
        // 判断是否启用端口统一服务器模式（支持多协议共存和动态切换）
        boolean isPuServerKey = url.getParameter(IS_PU_SERVER_KEY, false);
        if (isPuServerKey) {
            // 使用PortUnificationExchanger绑定服务器，支持运行时协议检测和升级
            server = new HeaderExchangeServer(
                    PortUnificationExchanger.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
        } else {
            // 使用标准Transporters绑定服务器，适用于单一协议场景
            //这里 创建服务 同时对requestHandler进行了包装ExchangeHandlerAdapter ->  HeaderExchangeHandler ->DecodeHandler
            server = new HeaderExchangeServer(
                    Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
        }
        return server;
    }

}
