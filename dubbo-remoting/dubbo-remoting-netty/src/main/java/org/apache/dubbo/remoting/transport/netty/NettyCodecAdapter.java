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
package org.apache.dubbo.remoting.transport.netty;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.DynamicChannelBuffer;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import static org.apache.dubbo.remoting.Constants.BUFFER_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_BUFFER_SIZE;
import static org.apache.dubbo.remoting.Constants.MAX_BUFFER_SIZE;
import static org.apache.dubbo.remoting.Constants.MIN_BUFFER_SIZE;

/**
 * NettyCodecAdapter.
 */
final class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();

    private final ChannelHandler decoder = new InternalDecoder();

    private final Codec2 codec;

    private final URL url;

    private final int bufferSize;

    private final org.apache.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
        int b = url.getPositiveParameter(BUFFER_KEY, DEFAULT_BUFFER_SIZE);
        this.bufferSize = b >= MIN_BUFFER_SIZE && b <= MAX_BUFFER_SIZE ? b : DEFAULT_BUFFER_SIZE;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    @Sharable
    private class InternalEncoder extends OneToOneEncoder {

        /**
         * 将业务对象编码为Netty可传输的ChannelBuffer
         * 通过Dubbo内部的Codec2实现序列化与协议封装，并将结果转换为Netty的缓冲区格式
         *
         * @param ctx Netty通道上下文，用于获取链路信息
         * @param ch Netty底层通信通道，用于关联Dubbo的NettyChannel
         * @param msg 待编码的业务消息对象
         * @return 编码后的Netty ChannelBuffer对象
         * @throws Exception 当编码过程发生IO异常或序列化失败时抛出
         */
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel ch, Object msg) throws Exception {
            // 创建初始容量为1024字节的动态缓冲区，用于承载编码后的字节数据
            org.apache.dubbo.remoting.buffer.ChannelBuffer buffer =
                    org.apache.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(1024);

            // 获取或创建与Netty Channel绑定的Dubbo NettyChannel对象
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            try {
                // 调用Dubbo内部编码器执行实际的序列化和协议头写入逻辑
                codec.encode(channel, buffer, msg);
            } finally {
                // 无论编码成功与否，都检查并清理已断开连接的通道缓存
                NettyChannel.removeChannelIfDisconnected(ch);
            }

            // 将Dubbo的ChannelBuffer转换为Netty的ChannelBuffer并返回
            return ChannelBuffers.wrappedBuffer(buffer.toByteBuffer());
        }
    }

    private class InternalDecoder extends SimpleChannelUpstreamHandler {

        private org.apache.dubbo.remoting.buffer.ChannelBuffer buffer =
                org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;

        /**
         * 处理接收到的网络消息，执行粘包/拆包处理并解码为业务对象
         * 将Netty的ChannelBuffer转换为Dubbo的ChannelBuffer，并通过循环解码支持一次接收多个完整数据包
         *
         * @param ctx Netty通道上下文，用于获取通道状态和向上传播事件
         * @param event 消息事件对象，包含接收到的原始字节数据
         * @throws Exception 当解码过程发生IO异常或数据异常时抛出
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
            Object o = event.getMessage();

            // 如果接收到的消息不是ChannelBuffer类型，则直接向上游传播该事件
            if (!(o instanceof ChannelBuffer)) {
                ctx.sendUpstream(event);
                return;
            }

            ChannelBuffer input = (ChannelBuffer) o;
            int readable = input.readableBytes();
            if (readable <= 0) {
                return;
            }

            // 合并缓冲区数据：将新到达的数据追加到未处理完的缓冲区中，以处理粘包/拆包场景
            org.apache.dubbo.remoting.buffer.ChannelBuffer message;
            if (buffer.readable()) {
                if (buffer instanceof DynamicChannelBuffer) {
                    buffer.writeBytes(input.toByteBuffer());
                    message = buffer;
                } else {
                    int size = buffer.readableBytes() + input.readableBytes();
                    message = org.apache.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(
                            size > bufferSize ? size : bufferSize);
                    message.writeBytes(buffer, buffer.readableBytes());
                    message.writeBytes(input.toByteBuffer());
                }
            } else {
                // 如果没有残留数据，则直接包装当前输入数据进行解码
                message = org.apache.dubbo.remoting.buffer.ChannelBuffers.wrappedBuffer(input.toByteBuffer());
            }

            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.getChannel(), url, handler);
            Object msg;
            int saveReaderIndex;

            try {
                // 循环解码：在一个TCP数据包可能包含多个完整请求的场景下，持续提取并解码消息
                do {
                    saveReaderIndex = message.readerIndex();
                    try {
                        msg = codec.decode(channel, message);
                    } catch (IOException e) {
                        // 发生IO异常时重置缓冲区，防止脏数据影响后续解码
                        buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                        throw e;
                    }
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        // 如果数据不完整（拆包），恢复读取索引并退出循环等待更多数据
                        message.readerIndex(saveReaderIndex);
                        break;
                    } else {
                        // 校验解码逻辑的正确性，确保解码过程确实消耗了字节数据
                        if (saveReaderIndex == message.readerIndex()) {
                            buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                            throw new IOException("Decode without read data.");
                        }
                        if (msg != null) {
                            // 将解码成功的业务对象向上传播给Handler处理
                            Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
                        }
                    }
                } while (message.readable());
            } finally {
                // 处理剩余的未读完数据，将其暂存到成员变量buffer中供下一次使用
                if (message.readable()) {
                    message.discardReadBytes();
                    buffer = message;
                } else {
                    buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                }

                // 清理已断开的通道缓存，避免内存泄漏
                NettyChannel.removeChannelIfDisconnected(ctx.getChannel());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            ctx.sendUpstream(e);
        }
    }
}
