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
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * NettyCodecAdapter.
 * Netty 编解码器适配器，将 Dubbo 的 Codec2 接口适配为 Netty 的 ChannelHandler（编码器和解码器）。
 * 该类内部维护了 InternalEncoder 和 InternalDecoder 两个私有类，分别处理出站消息的序列化和入站消息的反序列化。
 */
public final class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();

    private final ChannelHandler decoder = new InternalDecoder();

    private final Codec2 codec;

    private final URL url;

    private final org.apache.dubbo.remoting.ChannelHandler handler;

    /**
     * 构造 NettyCodecAdapter 实例。
     *
     * @param codec Dubbo 编解码器实现
     * @param url 服务 URL，包含配置信息
     * @param handler Dubbo 通道处理器
     */
    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * 内部编码器，继承自 Netty 的 MessageToByteEncoder。
     * 负责将 Dubbo 的消息对象转换为 ByteBuf 字节流。
     */
    private class InternalEncoder extends MessageToByteEncoder {

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            boolean encoded = false;
            // 如果消息已经是 ByteBuf 类型，直接写入输出缓冲区
            if (msg instanceof ByteBuf) {
                out.writeBytes(((ByteBuf) msg));
                encoded = true;
            } else if (msg instanceof MultiMessage) {
                // 如果是批量消息，循环处理其中的每个子消息
                for (Object singleMessage : ((MultiMessage) msg)) {
                    if (singleMessage instanceof ByteBuf) {
                        ByteBuf buf = (ByteBuf) singleMessage;
                        out.writeBytes(buf);
                        encoded = true;
                        // 释放已处理的 ByteBuf 引用计数
                        buf.release();
                    }
                }
            }

            // 如果尚未被处理，则调用 Dubbo Codec2 进行标准编码
            if (!encoded) {
                // 将 Netty 的 ByteBuf 包装为 Dubbo 的 ChannelBuffer
                ChannelBuffer buffer = new NettyBackedChannelBuffer(out);
                Channel ch = ctx.channel();
                // 获取或创建关联的 Dubbo NettyChannel
                NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
                codec.encode(channel, buffer, msg);
            }
        }
    }

    /**
     * 内部解码器，继承自 Netty 的 ByteToMessageDecoder。
     * 负责将接收到的 ByteBuf 字节流解码为 Dubbo 的消息对象（Request 或 Response）。
     */
    private class InternalDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> out) throws Exception {

            // 将 Netty 的 ByteBuf 包装为 Dubbo 的 ChannelBuffer
            ChannelBuffer message = new NettyBackedChannelBuffer(input);
            try {
                // 获取或创建关联的 Dubbo NettyChannel
                NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);

                // decode object.
                // 循环解码，直到缓冲区中没有足够的数据为止
                do {
                    // 记录当前的读取位置，以便在数据不足时回退
                    int saveReaderIndex = message.readerIndex();
                    Object msg = codec.decode(channel, message);
                    // 如果数据不足，回退指针并跳出循环，等待更多数据
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        message.readerIndex(saveReaderIndex);
                        break;
                    } else {
                        // 检查是否发生了没有读取任何数据的异常情况
                        if (saveReaderIndex == message.readerIndex()) {
                            throw new IOException("Decode without read data.");
                        }
                        // 将解码成功的消息加入输出列表，交给下一个 Handler 处理
                        if (msg != null) {
                            out.add(msg);
                        }
                    }
                } while (message.readable());
            } catch (Throwable t) {
                // 发生异常时，跳过剩余的所有字节，防止脏数据影响后续连接
                message.skipBytes(message.readableBytes());
                throw t;
            }
        }
    }
}
