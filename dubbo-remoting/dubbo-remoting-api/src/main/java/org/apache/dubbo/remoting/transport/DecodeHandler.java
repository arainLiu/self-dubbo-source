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

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Decodeable;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_DECODE;

public class DecodeHandler extends AbstractChannelHandlerDelegate {

    private static final ErrorTypeAwareLogger log = LoggerFactory.getErrorTypeAwareLogger(DecodeHandler.class);

    public DecodeHandler(ChannelHandler handler) {
        super(handler);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        if (message instanceof Decodeable) {
            decode(message);
        }

        if (message instanceof Request) {
            decode(((Request) message).getData());
        }

        if (message instanceof Response) {
            decode(((Response) message).getResult());
        }

        handler.received(channel, message);
    }

        /**
     * 解码可解码的消息对象
     * 如果消息实现了Decodeable接口，则触发其解码逻辑；否则直接返回
     * 该方法会捕获解码过程中的所有异常，避免影响后续处理流程
     *
     * @param message 待解码的消息对象，可能为Request、Response或其他类型
     */
    private void decode(Object message) {
        // 检查消息是否支持解码，不支持则直接返回
        if (!(message instanceof Decodeable)) {
            return;
        }

        try {
            // 执行解码操作，将字节数据转换为业务对象
            ((Decodeable) message).decode();
            if (log.isDebugEnabled()) {
                log.debug("Decode decodeable message " + message.getClass().getName());
            }
        } catch (Throwable e) {
            // 捕获解码异常并记录警告日志，不向上抛出以避免中断处理流程
            if (log.isWarnEnabled()) {
                log.warn(TRANSPORT_FAILED_DECODE, "", "", "Call Decodeable.decode failed: " + e.getMessage(), e);
            }
        }
    }
}
