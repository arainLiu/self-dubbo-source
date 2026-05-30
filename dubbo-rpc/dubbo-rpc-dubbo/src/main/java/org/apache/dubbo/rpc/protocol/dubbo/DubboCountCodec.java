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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.io.IOException;

import static org.apache.dubbo.rpc.Constants.INPUT_KEY;
import static org.apache.dubbo.rpc.Constants.OUTPUT_KEY;

/**
 * Dubbo 计数编解码器，用于支持批量消息的编码和解码，并统计输入输出的字节数。
 * 该编解码器包装了 DubboCodec，主要增强了以下功能：
 * 1. 支持 MultiMessage（批量消息）的循环编码和解码
 * 2. 在解码过程中记录每个请求或响应的字节长度，并将其存入附件中（INPUT_KEY/OUTPUT_KEY）
 * 3. 用于监控和统计网络传输的数据量
 */
public final class DubboCountCodec implements Codec2 {

    private final DubboCodec codec;

    /**
     * 构造 DubboCountCodec 实例。
     *
     * @param frameworkModel 框架模型，用于初始化内部的 DubboCodec
     */
    public DubboCountCodec(FrameworkModel frameworkModel) {
        codec = new DubboCodec(frameworkModel);
    }

    /**
     * 编码消息到缓冲区。
     * 如果消息是 MultiMessage 类型，则遍历其中的每个子消息并依次编码；否则直接委托给 DubboCodec 编码。
     *
     * @param channel 通信通道
     * @param buffer 输出缓冲区
     * @param msg 要编码的消息对象
     * @throws IOException 编码过程中发生的 IO 异常
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        // 如果是批量消息，则循环编码每个子消息
        if (msg instanceof MultiMessage) {
            MultiMessage multiMessage = (MultiMessage) msg;
            for (Object singleMessage : multiMessage) {
                codec.encode(channel, buffer, singleMessage);
            }
        } else {
            // 普通消息直接编码
            codec.encode(channel, buffer, msg);
        }
    }

    /**
     * 从缓冲区解码消息。
     * 该方法会尝试从缓冲区中解码出一个或多个消息。如果缓冲区中有足够的数据，可能会一次性解码出多个消息并封装为 MultiMessage 返回。
     * 如果数据不足，则返回 NEED_MORE_INPUT。
     *
     * @param channel 通信通道
     * @param buffer 输入缓冲区
     * @return 解码后的消息对象（可能是单个消息、MultiMessage 或 NEED_MORE_INPUT）
     * @throws IOException 解码过程中发生的 IO 异常
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        // 记录当前的读取位置，以便在数据不足时回退
        int save = buffer.readerIndex();
        MultiMessage result = MultiMessage.create();
        do {
            // 尝试解码一个消息
            Object obj = codec.decode(channel, buffer);
            // 如果数据不足，回退读取指针并退出循环
            if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {
                buffer.readerIndex(save);
                break;
            } else {
                // 将解码成功的消息加入结果集
                result.addMessage(obj);
                // 记录该消息消耗的字节数
                logMessageLength(obj, buffer.readerIndex() - save);
                // 更新保存点，准备尝试解码下一个消息
                save = buffer.readerIndex();
            }
        } while (true);
        // 如果没有解码出任何消息，返回需要更多输入
        if (result.isEmpty()) {
            return Codec2.DecodeResult.NEED_MORE_INPUT;
        }
        // 如果只解码出一个消息，直接返回该消息以简化处理
        if (result.size() == 1) {
            return result.get(0);
        }
        // 否则返回包含多个消息的 MultiMessage
        return result;
    }

    /**
     * 记录消息的长度并保存到附件中。
     * 对于 Request，记录输入字节数到 INPUT_KEY；对于 Response，记录输出字节数到 OUTPUT_KEY。
     * 这些信息通常用于服务监控和流量统计。
     *
     * @param result 解码后的消息对象
     * @param bytes 该消息占用的字节数
     */
    private void logMessageLength(Object result, int bytes) {
        if (bytes <= 0) {
            return;
        }
        if (result instanceof Request) {
            try {
                // 将输入字节数设置到 RpcInvocation 的附件中
                ((RpcInvocation) ((Request) result).getData()).setAttachment(INPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        } else if (result instanceof Response) {
            try {
                // 将输出字节数设置到 AppResponse 的附件中
                ((AppResponse) ((Response) result).getResult()).setAttachment(OUTPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        }
    }
}
