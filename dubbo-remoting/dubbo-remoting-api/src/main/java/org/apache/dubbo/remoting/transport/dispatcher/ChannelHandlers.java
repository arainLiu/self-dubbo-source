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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Dispatcher;
import org.apache.dubbo.remoting.exchange.support.header.HeartbeatHandler;
import org.apache.dubbo.remoting.transport.MultiMessageHandler;

public class ChannelHandlers {

    private static ChannelHandlers INSTANCE = new ChannelHandlers();

    protected ChannelHandlers() {}

    public static ChannelHandler wrap(ChannelHandler handler, URL url) {
        return ChannelHandlers.getInstance().wrapInternal(handler, url);
    }

    public static ChannelHandlers getInstance() {
        return INSTANCE;
    }

    static void setTestingChannelHandlers(ChannelHandlers instance) {
        INSTANCE = instance;
    }

    /**
     * 包装ChannelHandler，构建包含消息分发、心跳检测和批量处理的多层处理器链。
     * <p>
     * 该方法采用责任链模式，从内到外依次构建三层包装：
     * 1. Dispatcher层：根据URL配置选择线程派发策略（如all、direct、message、execution、connection）；
     * 2. HeartbeatHandler层：处理心跳请求和响应，维护连接活跃状态；
     * 3. MultiMessageHandler层：支持批量消息处理，提升吞吐量。
     * </p>
     *
     * @param handler 原始的ChannelHandler对象，通常是业务逻辑处理器或更底层的网络处理器
     * @param url 配置URL，用于获取Dispatcher扩展的自适应配置信息
     * @return ChannelHandler 经过三层包装后的增强处理器，具备线程派发、心跳处理和批量处理能力
     */
    protected ChannelHandler wrapInternal(ChannelHandler handler, URL url) {
        // 通过自适应扩展机制获取Dispatcher实现，将handler包装为多线程派发模式，然后依次添加心跳处理和批量消息处理能力
        // 这里ExtensionLoader.getExtensionLoader(Dispatcher.class).getAdaptiveExtension().dispatch(handler, url)
        //这里的dispatch默认是通过AllDispatcher.dispatch来处理的,从而获取得到一个AllChannelHandler(handler,url),
        //然后把AllChannelHandler 包装成HeartbeatHardler,HeartbeatHandler 包装成MultiMessageHandler
        //所以当Netty接收到一个数据时，会经历
        // MultiMessageHandler--->HeartbeatHandle>AllChannelHandler ->ExchangeHandlerAdapter ->DecodeHandler-HeaderExchangeHandle
        return new MultiMessageHandler(new HeartbeatHandler(url.getOrDefaultFrameworkModel()
                .getExtensionLoader(Dispatcher.class)
                .getAdaptiveExtension()
                .dispatch(handler, url)));
    }

}
