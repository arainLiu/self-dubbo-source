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

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.exchange.HeartBeatRequest;
import org.apache.dubbo.remoting.exchange.HeartBeatResponse;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.codec.ExchangeCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.EXECUTOR_MANAGEMENT_MODE_ISOLATION;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SystemProperty.SYSTEM_BYTE_ACCESSOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_DECODE;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_DECODE_IN_IO_THREAD;

/**
 * Dubbo codec.
 */
public class DubboCodec extends ExchangeCodec {

    public static final String NAME = "dubbo";
    public static final String DUBBO_VERSION = Version.getProtocolVersion();
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    public static final byte RESPONSE_VALUE = 1;
    public static final byte RESPONSE_NULL_VALUE = 2;
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final ErrorTypeAwareLogger log = LoggerFactory.getErrorTypeAwareLogger(DubboCodec.class);

    private static final AtomicBoolean decodeInUserThreadLogged = new AtomicBoolean(false);
    private final CallbackServiceCodec callbackServiceCodec;
    private final FrameworkModel frameworkModel;
    private final ByteAccessor customByteAccessor;
    private static final String DECODE_IN_IO_THREAD_KEY = "decode.in.io.thread";

    public DubboCodec(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
        callbackServiceCodec = new CallbackServiceCodec(frameworkModel);
        customByteAccessor = Optional.ofNullable(SystemPropertyConfigUtils.getSystemProperty(SYSTEM_BYTE_ACCESSOR_KEY))
                .filter(StringUtils::isNotBlank)
                .map(key ->
                        frameworkModel.getExtensionLoader(ByteAccessor.class).getExtension(key))
                .orElse(null);
    }

        /**
     * 解码消息体，根据请求标志位区分处理请求和响应
     *
     * @param channel 网络通道，用于获取URL配置等信息
     * @param is 输入流，包含待解码的消息体数据
     * @param header 消息头字节数组，长度为16字节，包含标志位、请求ID、状态码等元数据
     * @return 解码后的对象，可能是Request（请求）或Response（响应）对象
     * @throws IOException 当读取输入流或反序列化失败时抛出
     */
    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 提取标志位和序列化协议类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);

        // 从消息头中解析请求ID（第4-11字节）
        long id = Bytes.bytes2long(header, 4);

        // 判断是响应还是请求（检查标志位的第7位）
        if ((flag & FLAG_REQUEST) == 0) {
            // 解码响应消息

            // 创建响应对象并设置基本信息
            Response res = new Response(id);
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }

            // 从消息头获取响应状态码（第3字节）
            byte status = header[3];
            res.setStatus(status);
            try {
                // 根据响应状态码进行不同处理
                if (status == Response.OK) {
                    Object data;

                    // 处理事件类型的响应（如心跳）
                    if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                            // 心跳响应的数据始终为null
                            data = null;
                        } else {
                            // 反序列化事件数据
                            ObjectInput in = CodecSupport.deserialize(
                                    channel.getUrl(), new ByteArrayInputStream(eventPayload), proto);
                            data = decodeEventData(channel, in, eventPayload);
                        }
                    } else {
                        // 处理普通RPC响应结果

                        // 获取关联的请求信息用于结果解码
                        DecodeableRpcResult result;
                        Invocation inv = (Invocation) getRequestData(channel, res, id);

                        // 根据配置决定是否在IO线程中解码
                        if (channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD)) {
                            // 在IO线程中立即解码
                            if (customByteAccessor != null) {
                                result = customByteAccessor.getRpcResult(
                                        channel, res, new UnsafeByteArrayInputStream(readMessageData(is)), inv, proto);
                            } else {
                                result = new DecodeableRpcResult(
                                        channel, res, new UnsafeByteArrayInputStream(readMessageData(is)), inv, proto);
                            }
                            result.decode();
                        } else {
                            // 延迟到业务线程解码，仅创建对象
                            if (customByteAccessor != null) {
                                result = customByteAccessor.getRpcResult(
                                        channel, res, new UnsafeByteArrayInputStream(readMessageData(is)), inv, proto);
                            } else {
                                result = new DecodeableRpcResult(
                                        channel, res, new UnsafeByteArrayInputStream(readMessageData(is)), inv, proto);
                            }
                        }
                        data = result;
                    }
                    res.setResult(data);
                } else {
                    // 处理异常响应，读取错误消息
                    ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                // 捕获解码过程中的所有异常，设置客户端错误状态
                if (log.isWarnEnabled()) {
                    log.warn(PROTOCOL_FAILED_DECODE, "", "", "Decode response failed: " + t.getMessage(), t);
                }
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // 解码请求消息

            Request req;
            try {
                Object data;

                // 处理事件类型的请求（如心跳）
                if ((flag & FLAG_EVENT) != 0) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                        // 创建心跳请求对象
                        req = new HeartBeatRequest(id);
                        req.setVersion(Version.getProtocolVersion());
                        req.setTwoWay((flag & FLAG_TWOWAY) != 0);
                        ((HeartBeatRequest) req).setProto(proto);
                        data = null;
                    } else {
                        // 创建普通事件请求并反序列化事件数据
                        req = new Request(id);
                        req.setVersion(Version.getProtocolVersion());
                        req.setTwoWay((flag & FLAG_TWOWAY) != 0);

                        ObjectInput in = CodecSupport.deserialize(
                                channel.getUrl(), new ByteArrayInputStream(eventPayload), proto);
                        data = decodeEventData(channel, in, eventPayload);
                    }
                    req.setEvent(true);
                } else {
                    // 处理普通RPC请求

                    // 创建请求对象并设置基本属性
                    req = new Request(id);
                    req.setVersion(Version.getProtocolVersion());
                    req.setTwoWay((flag & FLAG_TWOWAY) != 0);

                    // 从消息头读取数据长度（第12-15字节）
                    int len = Bytes.bytes2int(header, 12);
                    req.setPayload(len);

                    // 创建可解码的RPC调用对象
                    DecodeableRpcInvocation inv;
                    if (isDecodeDataInIoThread(channel)) {
                        // 在IO线程中立即解码
                        if (customByteAccessor != null) {
                            inv = customByteAccessor.getRpcInvocation(
                                    channel, req, new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                        } else {
                            inv = new DecodeableRpcInvocation(
                                    frameworkModel,
                                    channel,
                                    req,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    proto);
                        }
                        inv.decode();
                    } else {
                        // 延迟到业务线程解码，仅创建对象
                        if (customByteAccessor != null) {
                            inv = customByteAccessor.getRpcInvocation(
                                    channel, req, new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                        } else {
                            inv = new DecodeableRpcInvocation(
                                    frameworkModel,
                                    channel,
                                    req,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    proto);
                        }
                    }
                    data = inv;
                }
                req.setData(data);
            } catch (Throwable t) {
                // 捕获请求解码异常，创建损坏的请求对象
                if (log.isWarnEnabled()) {
                    log.warn(PROTOCOL_FAILED_DECODE, "", "", "Decode request failed: " + t.getMessage(), t);
                }
                req = new HeartBeatRequest(id);
                req.setBroken(true);
                req.setData(t);
            }

            return req;
        }
    }

    private boolean isDecodeDataInIoThread(Channel channel) {
        Object obj = channel.getAttribute(DECODE_IN_IO_THREAD_KEY);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        String mode = ExecutorRepository.getMode(channel.getUrl().getOrDefaultApplicationModel());
        boolean isIsolated = EXECUTOR_MANAGEMENT_MODE_ISOLATION.equals(mode);

        if (isIsolated && !decodeInUserThreadLogged.compareAndSet(false, true)) {
            channel.setAttribute(DECODE_IN_IO_THREAD_KEY, true);
            return true;
        }

        boolean decodeDataInIoThread =
                channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD);
        if (isIsolated && !decodeDataInIoThread) {
            log.info("Because thread pool isolation is enabled on the dubbo protocol, the body can only be decoded "
                    + "on the io thread, and the parameter[" + DECODE_IN_IO_THREAD_KEY + "] will be ignored");
            // Why? because obtaining the isolated thread pool requires the serviceKey of the service,
            // and this part must be decoded before it can be obtained (more see DubboExecutorSupport)
            channel.setAttribute(DECODE_IN_IO_THREAD_KEY, true);
            return true;
        }
        channel.setAttribute(DECODE_IN_IO_THREAD_KEY, decodeDataInIoThread);
        return decodeDataInIoThread;
    }

    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[] {};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 编码请求数据到输出流中
     * 按照Dubbo协议格式，依次写入服务元数据、方法信息和参数数据
     *
     * @param channel 网络通道，用于获取上下文信息和处理回调服务
     * @param out 输出流，用于写入编码后的请求数据
     * @param data 请求数据对象，必须为RpcInvocation类型，包含方法调用所需的所有信息
     * @param version 协议版本号，用于服务端版本兼容性检查
     * @throws IOException 当写入输出流失败时抛出
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version)
            throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        // 写入协议版本信息
        out.writeUTF(version);

        // 获取服务名称，优先使用接口名，降级使用路径名（兼容历史问题）
        String serviceName = inv.getAttachment(INTERFACE_KEY);
        if (serviceName == null) {
            serviceName = inv.getAttachment(PATH_KEY);
        }
        out.writeUTF(serviceName);

        // 写入服务版本信息
        out.writeUTF(inv.getAttachment(VERSION_KEY));

        // 写入方法名和参数类型描述，用于服务端方法定位
        out.writeUTF(inv.getMethodName());
        out.writeUTF(inv.getParameterTypesDesc());

        // 遍历并编码方法参数，处理回调服务的特殊逻辑
        Object[] args = inv.getArguments();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                out.writeObject(callbackServiceCodec.encodeInvocationArgument(channel, inv, i));
            }
        }

        // 写入附件信息，包含额外的配置和元数据
        out.writeAttachments(inv.getObjectAttachments());
    }


        /**
     * 编码响应数据到输出流中
     * 根据响应结果类型（正常返回值或异常）和版本兼容性，写入不同的标识符和数据
     *
     * @param channel 网络通道，用于获取上下文信息
     * @param out 输出流，用于写入编码后的响应数据
     * @param data 响应数据对象，必须为Result类型，包含返回值或异常信息
     * @param version 请求的版本号，用于判断是否支持携带attachments信息
     * @throws IOException 当写入输出流失败时抛出
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version)
            throws IOException {
        Result result = (Result) data;

        // 判断当前版本是否支持在响应中携带attachments信息
        boolean attach = Version.isSupportResponseAttachment(version);

        // 获取响应中的异常信息，用于区分正常返回和异常返回
        Throwable th = result.getException();
        if (th == null) {
            // 处理正常返回值的情况

            Object ret = result.getValue();
            if (ret == null) {
                // 返回值为null时，根据版本兼容性写入不同的标识符
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                // 返回值不为null时，写入标识符和实际的返回值对象
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            // 处理异常情况，写入异常标识符和异常对象
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeThrowable(th);
        }

        // 如果版本支持attachments，将当前协议版本和attachments信息写入输出流
        if (attach) {
            result.getObjectAttachments().put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeAttachments(result.getObjectAttachments());
        }
    }

    @Override
    protected Serialization getSerialization(Channel channel, Request req) {
        if (!(req.getData() instanceof Invocation)) {
            return super.getSerialization(channel, req);
        }
        return DubboCodecSupport.getRequestSerialization(channel.getUrl(), (Invocation) req.getData());
    }

    @Override
    protected Serialization getSerialization(Channel channel, Response res) {
        if (res instanceof HeartBeatResponse) {
            return CodecSupport.getSerializationById(((HeartBeatResponse) res).getProto());
        }
        if (!(res.getResult() instanceof AppResponse)) {
            return super.getSerialization(channel, res);
        }
        return DubboCodecSupport.getResponseSerialization(channel.getUrl(), (AppResponse) res.getResult());
    }
}
