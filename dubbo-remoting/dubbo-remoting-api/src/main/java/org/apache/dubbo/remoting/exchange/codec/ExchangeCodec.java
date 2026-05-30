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
package org.apache.dubbo.remoting.exchange.codec;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.StreamUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.exchange.HeartBeatRequest;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.telnet.codec.TelnetCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_TIMEOUT_SERVER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_EXCEED_PAYLOAD_LIMIT;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_RESPONSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_SKIP_UNUSED_STREAM;

/**
 * ExchangeCodec.
 * Dubbo 交换层编解码器，负责 Request 和 Response 对象的序列化与反序列化。
 * 它定义了 Dubbo 协议的二进制帧结构：
 * 1. 16字节的固定消息头（包含魔数、标志位、状态、请求ID、数据长度）
 * 2. 可变长度的消息体（由具体的序列化协议决定）
 *
 * 该编解码器还支持心跳检测、事件标记以及 payload 大小限制检查。
 */
public class ExchangeCodec extends TelnetCodec {

    // header length.
    // 消息头的固定长度为 16 字节
    protected static final int HEADER_LENGTH = 16;
    // magic header.
    // 魔数，用于快速识别 Dubbo 协议包
    protected static final short MAGIC = (short) 0xdabb;
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];
    // message flag.
    // 消息标志位：请求包
    protected static final byte FLAG_REQUEST = (byte) 0x80;
    // 消息标志位：双向通信（需要响应）
    protected static final byte FLAG_TWOWAY = (byte) 0x40;
    // 消息标志位：事件包（如心跳）
    protected static final byte FLAG_EVENT = (byte) 0x20;
    // 序列化 ID 的掩码，低 5 位用于存储序列化类型 ID
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        } else if (msg instanceof Response) {
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            super.encode(channel, buffer, msg);
        }
    }

    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        return decode(channel, buffer, readable, header);
    }

    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // check magic number.
        // 检查魔数，如果不匹配则尝试寻找下一个魔数位置或回退到父类处理
        if (readable > 0 && header[0] != MAGIC_HIGH || readable > 1 && header[1] != MAGIC_LOW) {
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            return super.decode(channel, buffer, readable, header);
        }
        // check length.
        // 如果可读字节数小于消息头长度，说明数据不全，返回 NEED_MORE_INPUT
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // get data length.
        // 从消息头中获取消息体的长度
        int len = Bytes.bytes2int(header, 12);

        // When receiving response, how to exceed the length, then directly construct a response to the client.
        // see more detail from https://github.com/apache/dubbo/issues/7021.
        // 检查响应数据是否超过 payload 限制，如果超过则直接构造错误响应
        Object obj = finishRespWhenOverPayload(channel, len, header);
        if (null != obj) {
            return obj;
        }

        int tt = len + HEADER_LENGTH;
        // 如果当前缓冲区的数据不足以容纳整个消息，返回 NEED_MORE_INPUT
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // limit input stream.
        // 创建一个受限的输入流，确保只读取当前消息的长度
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            return decodeBody(channel, is, header);
        } finally {
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn(TRANSPORT_SKIP_UNUSED_STREAM, "", "", "Skip input stream " + is.available());
                    }
                    // 跳过未使用的字节，防止影响后续消息的解码
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(TRANSPORT_SKIP_UNUSED_STREAM, "", "", e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 解码消息体内容。
     * 根据消息头中的标志位判断是 Request 还是 Response，并调用相应的解码逻辑。
     *
     * @param channel 通信通道
     * @param is 消息体输入流
     * @param header 16字节的消息头
     * @return 解码后的 Request 或 Response 对象
     * @throws IOException 解码过程中发生的 IO 异常
     */
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.
        // 从消息头中获取请求 ID
        long id = Bytes.bytes2long(header, 4);
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            // 解码响应对象
            Response res = new Response(id);
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }
            // get status.
            // 获取响应状态码
            byte status = header[3];
            res.setStatus(status);
            try {
                if (status == Response.OK) {
                    Object data;
                    if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                            // heart beat response data is always null;
                            // 心跳响应的数据始终为空
                            data = null;
                        } else {
                            data = decodeEventData(
                                    channel,
                                    CodecSupport.deserialize(
                                            channel.getUrl(), new ByteArrayInputStream(eventPayload), proto),
                                    eventPayload);
                        }
                    } else {
                        // 解码正常的响应数据，需要关联原始的请求数据以支持泛化调用等场景
                        data = decodeResponseData(
                                channel,
                                CodecSupport.deserialize(channel.getUrl(), is, proto),
                                getRequestData(channel, res, id));
                    }
                    res.setResult(data);
                } else {
                    // 如果状态不为 OK，则读取错误信息字符串
                    res.setErrorMessage(CodecSupport.deserialize(channel.getUrl(), is, proto)
                            .readUTF());
                }
            } catch (Throwable t) {
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.
            // 解码请求对象
            Request req;
            try {
                Object data;
                if ((flag & FLAG_EVENT) != 0) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                        // heart beat response data is always null;
                        // 创建心跳请求对象
                        req = new HeartBeatRequest(id);
                        ((HeartBeatRequest) req).setProto(proto);
                        data = null;
                    } else {
                        req = new Request(id);
                        data = decodeEventData(
                                channel,
                                CodecSupport.deserialize(
                                        channel.getUrl(), new ByteArrayInputStream(eventPayload), proto),
                                eventPayload);
                    }
                    req.setEvent(true);
                } else {
                    req = new Request(id);
                    // 解码请求参数数据
                    data = decodeRequestData(channel, CodecSupport.deserialize(channel.getUrl(), is, proto));
                }
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                // 如果解码失败，创建一个损坏的请求对象并记录异常
                req = new Request(id);
                req.setBroken(true);
                req.setData(t);
            }
            req.setVersion(Version.getProtocolVersion());
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            return req;
        }
    }

    /**
     * 获取与响应 ID 匹配的原始请求数据。
     * 主要用于在解码响应时恢复请求上下文（如方法名、参数类型等）。
     *
     * @param channel 通信通道
     * @param response 当前正在解码的响应
     * @param id 请求/响应 ID
     * @return 原始请求的数据对象
     * @throws IllegalArgumentException 如果找不到对应的请求
     */
    protected Object getRequestData(Channel channel, Response response, long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future != null) {
            Request req = future.getRequest();
            if (req != null) {
                return req.getData();
            }
        }

        logger.warn(
                PROTOCOL_TIMEOUT_SERVER,
                "",
                "",
                "The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response status is " + response.getStatus() + ", response id is " + response.getId()
                        + (channel == null
                                ? ""
                                : ", channel: " + channel.getLocalAddress() + " -> " + channel.getRemoteAddress())
                        + ", please check provider side for detailed result.");
        throw new IllegalArgumentException("Failed to find any request match the response, response id: " + id);
    }

    /**
     * 编码请求对象。
     * 构建 16 字节的消息头，并将请求数据序列化后写入缓冲区。
     *
     * @param channel 通信通道
     * @param buffer 输出缓冲区
     * @param req 要编码的请求对象
     * @throws IOException 编码过程中发生的 IO 异常
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        Serialization serialization = getSerialization(channel, req);
        // header.
        byte[] header = new byte[HEADER_LENGTH];
        // set magic number.
        // 设置魔数
        Bytes.short2bytes(MAGIC, header);

        // set request and serialization flag.
        // 设置请求标志位和序列化 ID
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());

        if (req.isTwoWay()) {
            header[2] |= FLAG_TWOWAY;
        }
        if (req.isEvent()) {
            header[2] |= FLAG_EVENT;
        }

        // set request id.
        // 设置请求 ID
        Bytes.long2bytes(req.getId(), header, 4);

        // encode request data.
        // 先预留消息头的位置，直接跳到消息体位置进行序列化
        int savedWriteIndex = buffer.writerIndex();
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);

        if (req.isHeartbeat()) {
            // heartbeat request data is always null
            // 心跳请求的数据部分为空
            bos.write(CodecSupport.getNullBytesOf(serialization));
        } else {
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
            if (req.isEvent()) {
                encodeEventData(channel, out, req.getData());
            } else {
                encodeRequestData(channel, out, req.getData(), req.getVersion());
            }
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
        }

        bos.flush();
        bos.close();
        int len = bos.writtenBytes();
        // 检查序列化后的数据大小是否超过 payload 限制
        checkPayload(channel, req.getPayload(), len);
        // 将消息体长度写入消息头
        Bytes.int2bytes(len, header, 12);

        // write
        // 回到预留位置写入消息头
        buffer.writerIndex(savedWriteIndex);
        buffer.writeBytes(header); // write header.
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 编码响应对象。
     * 构建 16 字节的消息头，并将响应结果或错误信息序列化后写入缓冲区。
     *
     * @param channel 通信通道
     * @param buffer 输出缓冲区
     * @param res 要编码的响应对象
     * @throws IOException 编码过程中发生的 IO 异常
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        int savedWriteIndex = buffer.writerIndex();
        try {
            Serialization serialization = getSerialization(channel, res);
            // header.
            byte[] header = new byte[HEADER_LENGTH];
            // set magic number.
            // 设置魔数
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            // 设置序列化 ID（响应包不需要设置 REQUEST 标志位）
            header[2] = serialization.getContentTypeId();
            if (res.isHeartbeat()) {
                header[2] |= FLAG_EVENT;
            }
            // set response status.
            // 设置响应状态码
            byte status = res.getStatus();
            header[3] = status;
            // set request id.
            // 设置请求 ID
            Bytes.long2bytes(res.getId(), header, 4);

            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            int len;
            try (ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer)) {

                // encode response data or error message.
                if (status == Response.OK) {
                    if (res.isHeartbeat()) {
                        // heartbeat response data is always null
                        // 心跳响应的数据部分为空
                        bos.write(CodecSupport.getNullBytesOf(serialization));
                    } else {
                        ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                        if (res.isEvent()) {
                            encodeEventData(channel, out, res.getResult());
                        } else {
                            encodeResponseData(channel, out, res.getResult(), res.getVersion());
                        }
                        out.flushBuffer();
                        if (out instanceof Cleanable) {
                            ((Cleanable) out).cleanup();
                        }
                    }
                } else {
                    // 如果状态异常，序列化错误信息字符串
                    ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                    out.writeUTF(res.getErrorMessage());
                    out.flushBuffer();
                    if (out instanceof Cleanable) {
                        ((Cleanable) out).cleanup();
                    }
                }

                bos.flush();
                len = bos.writtenBytes();
            }

            checkPayload(channel, len);
            Bytes.int2bytes(len, header, 12);
            // write
            // 回到预留位置写入消息头
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // write header.
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            // 发生异常时重置写指针
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            // 尝试向消费者发送一个 BAD_RESPONSE，避免客户端超时等待
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(TRANSPORT_EXCEED_PAYLOAD_LIMIT, "", "", t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        r.setStatus(Response.SERIALIZATION_ERROR);
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn(
                                TRANSPORT_FAILED_RESPONSE,
                                "",
                                "",
                                "Failed to send bad_response info back: " + t.getMessage() + ", cause: "
                                        + e.getMessage(),
                                e);
                    }
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn(
                            TRANSPORT_FAILED_RESPONSE,
                            "",
                            "",
                            "Fail to encode response: " + res + ", send bad_response info instead, cause: "
                                    + t.getMessage(),
                            t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn(
                                TRANSPORT_FAILED_RESPONSE,
                                "",
                                "",
                                "Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(),
                                e);
                    }
                }
            }

            // Rethrow exception
            // 重新抛出异常，交由上层处理器捕获
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeEvent((String) data);
    }

    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in, byte[] eventBytes) throws IOException {
        try {
            if (eventBytes != null) {
                int dataLen = eventBytes.length;
                int threshold = ConfigurationUtils.getSystemConfiguration(
                                channel.getUrl().getScopeModel())
                        .getInt("deserialization.event.size", 15);
                if (dataLen > threshold) {
                    throw new IllegalArgumentException("Event data too long, actual size " + threshold + ", threshold "
                            + threshold + " rejected for security consideration.");
                }
            }
            return in.readEvent();
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Decode dubbo protocol event failed.", e));
        }
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version)
            throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version)
            throws IOException {
        encodeResponseData(out, data);
    }

    /**
     * 当响应数据长度超过 payload 限制时，提前构造一个错误响应。
     * 这种机制可以防止服务端反序列化过大的数据包导致内存溢出。
     *
     * @param channel 通信通道
     * @param size 消息体长度
     * @param header 消息头
     * @return 如果超限则返回错误响应，否则返回 null
     */
    private Object finishRespWhenOverPayload(Channel channel, long size, byte[] header) {
        byte flag = header[2];
        // 只有响应包才在此处检查 payload 限制
        if ((flag & FLAG_REQUEST) == 0) {
            int payload = getPayload(channel);
            boolean overPayload = isOverPayload(payload, size);
            if (overPayload) {
                long reqId = Bytes.bytes2long(header, 4);
                Response res = new Response(reqId);
                if ((flag & FLAG_EVENT) != 0) {
                    res.setEvent(true);
                }
                res.setStatus(Response.CLIENT_ERROR);
                String errorMsg =
                        "Data length too large: " + size + ", max payload: " + payload + ", channel: " + channel;
                logger.error(TRANSPORT_EXCEED_PAYLOAD_LIMIT, "", "", errorMsg);
                res.setErrorMessage(errorMsg);
                return res;
            }
        }
        return null;
    }
}
