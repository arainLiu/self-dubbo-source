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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.RemotingServer;
import org.apache.dubbo.remoting.Transporter;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.PortUnificationExchanger;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.apache.dubbo.remoting.utils.UrlUtils;
import org.apache.dubbo.rpc.DefaultProtocolServer;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LAZY_CONNECT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.STUB_EVENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_ERROR_CLOSE_SERVER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_REFER_INVOKER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_UNSUPPORTED;
import static org.apache.dubbo.remoting.Constants.CHANNEL_READONLYEVENT_SENT_KEY;
import static org.apache.dubbo.remoting.Constants.CLIENT_KEY;
import static org.apache.dubbo.remoting.Constants.CODEC_KEY;
import static org.apache.dubbo.remoting.Constants.CONNECTIONS_KEY;
import static org.apache.dubbo.remoting.Constants.DEFAULT_HEARTBEAT;
import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_CLIENT;
import static org.apache.dubbo.remoting.Constants.HEARTBEAT_KEY;
import static org.apache.dubbo.remoting.Constants.SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.DEFAULT_REMOTING_SERVER;
import static org.apache.dubbo.rpc.Constants.DEFAULT_STUB_EVENT;
import static org.apache.dubbo.rpc.Constants.IS_SERVER_KEY;
import static org.apache.dubbo.rpc.Constants.STUB_EVENT_METHODS_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.CALLBACK_SERVICE_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_SHARE_CONNECTIONS;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.IS_CALLBACK_SERVICE;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_CONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.ON_DISCONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.SHARE_CONNECTIONS_KEY;

/**
 * dubbo protocol support.
 */
public class DubboProtocol extends AbstractProtocol {

    public static final String NAME = "dubbo";

    public static final int DEFAULT_PORT = 20880;

    private static final String IS_CALLBACK_SERVICE_INVOKE = "_isCallBackServiceInvoke";

    /**
     * <host:port,Exchanger>
     * Map<String, List<ReferenceCountExchangeClient>
     */
    private final Map<String, SharedClientsProvider> referenceClientMap = new ConcurrentHashMap<>();

    private final AtomicBoolean destroyed = new AtomicBoolean();

    private final ExchangeHandler requestHandler;

    public DubboProtocol(FrameworkModel frameworkModel) {

        /**
         * Dubbo协议的核心请求处理器，负责处理来自客户端的RPC调用和连接事件。
         * <p>
         * 该处理器继承自ExchangeHandlerAdapter，实现了以下关键功能：
         * 1. reply方法：处理远程服务调用请求，执行Invoker.invoke并返回结果；
         * 2. received方法：接收消息并区分Invocation类型进行分发处理；
         * 3. connected/disconnected方法：处理连接建立和断开事件，触发onConnect/onDisconnect回调；
         * 4. invoke方法：执行事件方法调用，支持stub服务的特殊处理；
         * 5. createInvocation方法：根据URL配置创建事件调用的Invocation对象。
         * </p>
         */
        requestHandler = new ExchangeHandlerAdapter(frameworkModel) {

            /**
             * 处理远程服务调用请求，执行Invoker链并返回异步结果。
             * <p>
             * 该方法的处理流程包括：
             * 1. 验证消息类型必须为Invocation；
             * 2. 获取对应的Invoker实例（优先使用inv中缓存的，否则从channel中查找）；
             * 3. 切换线程上下文类加载器（TCCL）以支持多模块隔离；
             * 4. 验证回调服务的方法合法性；
             * 5. 执行Invoker.invoke调用并返回CompletableFuture结果。
             * </p>
             *
             * @param channel 网络通信通道，用于获取客户端地址和服务端地址信息
             * @param message 请求消息对象，必须是Invocation类型
             * @return CompletableFuture<Object> 异步返回的服务调用结果
             * @throws RemotingException 当消息类型不支持或获取Invoker失败时抛出
             */
            @Override
            public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) throws RemotingException {

                // 验证消息类型：仅支持Invocation类型的请求
                if (!(message instanceof Invocation)) {
                    throw new RemotingException(
                            channel,
                            "Unsupported request: "
                                    + (message == null
                                            ? null
                                            : (message.getClass().getName() + ": " + message))
                                    + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: "
                                    + channel.getLocalAddress());
                }

                Invocation inv = (Invocation) message;
                // 获取Invoker：优先使用inv中已绑定的，否则根据channel和inv信息从exporterMap中查找
                Invoker<?> invoker = inv.getInvoker() == null ? getInvoker(channel, inv) : inv.getInvoker();

                // 切换线程上下文类加载器（TCCL），确保使用服务模型指定的类加载器以支持模块化隔离
                if (invoker.getUrl().getServiceModel() != null) {
                    Thread.currentThread()
                            .setContextClassLoader(
                                    invoker.getUrl().getServiceModel().getClassLoader());
                }

                // 处理向后兼容的回调服务场景：验证回调方法是否在接口中声明
                if (Boolean.TRUE.toString().equals(inv.getObjectAttachmentWithoutConvert(IS_CALLBACK_SERVICE_INVOKE))) {
                    String methodsStr = invoker.getUrl().getParameters().get("methods");
                    boolean hasMethod = false;
                    if (methodsStr == null || !methodsStr.contains(",")) {
                        hasMethod = inv.getMethodName().equals(methodsStr);
                    } else {
                        String[] methods = methodsStr.split(",");
                        for (String method : methods) {
                            if (inv.getMethodName().equals(method)) {
                                hasMethod = true;
                                break;
                            }
                        }
                    }
                    if (!hasMethod) {
                        logger.warn(
                                PROTOCOL_FAILED_REFER_INVOKER,
                                "",
                                "",
                                new IllegalStateException("The methodName " + inv.getMethodName()
                                                + " not found in callback service interface ,invoke will be ignored."
                                                + " please update the api interface. url is:"
                                                + invoker.getUrl())
                                        + " ,invocation is :" + inv);
                        return null;
                    }
                }

                // 设置远程地址到RpcContext，然后执行Invoker调用链并返回异步结果
                RpcContext.getServiceContext().setRemoteAddress(channel.getRemoteAddress());
                // 执行invoke
                Result result = invoker.invoke(inv);
                return result.thenApply(Function.identity());
            }

            /**
             * 接收通道消息并进行分发处理，区分Invocation类型和其他消息类型。
             *
             * @param channel 网络通信通道
             * @param message 接收到的消息对象
             * @throws RemotingException 当消息处理失败时抛出
             */
            @Override
            public void received(Channel channel, Object message) throws RemotingException {
                // 对于Invocation类型的消息，直接调用reply方法执行服务调用
                if (message instanceof Invocation) {
                    reply((ExchangeChannel) channel, message);

                } else {
                    // 其他类型消息交给父类处理（如心跳、事件等）
                    super.received(channel, message);
                }
            }

            /**
             * 处理连接建立事件，触发onConnect回调方法的执行。
             *
             * @param channel 新建立的网络通道
             * @throws RemotingException 当回调方法执行失败时抛出
             */
            @Override
            public void connected(Channel channel) throws RemotingException {
                invoke(channel, ON_CONNECT_KEY);
            }

            /**
             * 处理连接断开事件，记录调试日志并触发onDisconnect回调方法的执行。
             *
             * @param channel 即将断开的网络通道
             * @throws RemotingException 当回调方法执行失败时抛出
             */
            @Override
            public void disconnected(Channel channel) throws RemotingException {
                if (logger.isDebugEnabled()) {
                    logger.debug("disconnected from " + channel.getRemoteAddress() + ",url:" + channel.getUrl());
                }
                invoke(channel, ON_DISCONNECT_KEY);
            }

            /**
             * 执行事件方法调用（如onConnect/onDisconnect），支持stub服务的特殊处理。
             * <p>
             * 该方法的处理逻辑：
             * 1. 创建事件调用对象（根据methodKey从URL中获取方法名）；
             * 2. 如果启用了stub事件，尝试获取stub服务验证其存在性；
             * 3. 调用received方法触发事件方法的执行。
             * </p>
             *
             * @param channel 网络通道对象
             * @param methodKey URL参数键名（如"onconnect"或"ondisconnect"）
             */
            private void invoke(Channel channel, String methodKey) {
                Invocation invocation = createInvocation(channel, channel.getUrl(), methodKey);
                if (invocation != null) {
                    try {
                        // 如果启用了stub事件，先尝试获取stub服务以确保其已导出
                        if (Boolean.TRUE.toString().equals(invocation.getAttachment(STUB_EVENT_KEY))) {
                            tryToGetStubService(channel, invocation);
                        }
                        received(channel, invocation);
                    } catch (Throwable t) {
                        logger.warn(
                                PROTOCOL_FAILED_REFER_INVOKER,
                                "",
                                "",
                                "Failed to invoke event method " + invocation.getMethodName() + "(), cause: "
                                        + t.getMessage(),
                                t);
                    }
                }
            }

            /**
             * 尝试验证stub服务是否已导出，用于提前发现未导出的stub服务问题。
             *
             * @param channel 网络通道对象
             * @param invocation 调用对象，包含服务路径、版本、分组等信息
             * @throws RemotingException 当stub服务未找到时抛出异常
             */
            private void tryToGetStubService(Channel channel, Invocation invocation) throws RemotingException {
                try {
                    Invoker<?> invoker = getInvoker(channel, invocation);
                } catch (RemotingException e) {
                    String serviceKey = serviceKey(
                            0,
                            (String) invocation.getObjectAttachmentWithoutConvert(PATH_KEY),
                            (String) invocation.getObjectAttachmentWithoutConvert(VERSION_KEY),
                            (String) invocation.getObjectAttachmentWithoutConvert(GROUP_KEY));
                    throw new RemotingException(
                            channel, "The stub service[" + serviceKey + "] is not found, it may not be exported yet");
                }
            }

            /**
             * 根据URL配置创建事件调用的Invocation对象（如onConnect/onDisconnect方法调用）。
             * <p>
             * 注意：channel.getUrl()始终绑定到一个固定的服务，这个服务是随机的。
             * 如果无法获取当前连接绑定的具体服务，可以选择使用通用服务来承载连接事件。
             * </p>
             *
             * @param channel 网络通道对象
             * @param url 服务配置的URL对象
             * @param methodKey URL参数键名（如"onconnect"或"ondisconnect"）
             * @return Invocation对象，如果URL中未配置对应方法则返回null
             */
            private Invocation createInvocation(Channel channel, URL url, String methodKey) {
                // 从URL中获取事件方法名（如onconnect或ondisconnect）
                String method = url.getParameter(methodKey);
                if (method == null || method.length() == 0) {
                    return null;
                }

                // 创建RpcInvocation对象，设置服务模型、方法名、接口名等基本信息
                RpcInvocation invocation = new RpcInvocation(
                        url.getServiceModel(),
                        method,
                        url.getParameter(INTERFACE_KEY),
                        "",
                        new Class<?>[0],
                        new Object[0]);
                // 设置必要的附件信息：路径、分组、接口名、版本号
                invocation.setAttachment(PATH_KEY, url.getPath());
                invocation.setAttachment(GROUP_KEY, url.getGroup());
                invocation.setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
                invocation.setAttachment(VERSION_KEY, url.getVersion());

                // 如果启用了stub事件，添加stub事件标识
                if (url.getParameter(STUB_EVENT_KEY, false)) {
                    invocation.setAttachment(STUB_EVENT_KEY, Boolean.TRUE.toString());
                }

                return invocation;
            }
        };

        this.frameworkModel = frameworkModel;
        this.frameworkModel.getBeanFactory().registerBean(new DubboGracefulShutdown(this));
    }

    /**
     * @deprecated Use {@link DubboProtocol#getDubboProtocol(ScopeModel)} instead
     */
    @Deprecated
    public static DubboProtocol getDubboProtocol() {
        return (DubboProtocol) FrameworkModel.defaultModel()
                .getExtensionLoader(Protocol.class)
                .getExtension(DubboProtocol.NAME, false);
    }

    public static DubboProtocol getDubboProtocol(ScopeModel scopeModel) {
        return (DubboProtocol) scopeModel.getExtensionLoader(Protocol.class).getExtension(DubboProtocol.NAME, false);
    }

    private boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort()
                && NetUtils.filterLocalHost(channel.getUrl().getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    /**
     * 根据网络通道和调用信息查找对应的Invoker实例。
     * <p>
     * 该方法的处理逻辑：
     * 1. 判断是否为stub服务调用（客户端启用stub事件时的onConnect/onDisconnect回调）；
     * 2. 判断是否为回调服务调用（客户端调用的服务，需要特殊处理路径）；
     * 3. 生成服务唯一标识（serviceKey），从exporterMap中查找对应的Exporter；
     * 4. 将服务模型设置到invocation中，返回Invoker实例。
     * </p>
     *
     * @param channel 网络通信通道，用于获取本地地址和判断客户端/服务端角色
     * @param inv 调用信息对象，包含服务路径、版本、分组等附件信息
     * @return Invoker<?> 找到的服务调用器实例
     * @throws RemotingException 当未找到导出的服务时抛出异常，包含详细的服务键和通道信息
     */
    Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
        boolean isCallBackServiceInvoke;
        boolean isStubServiceInvoke;
        int port = channel.getLocalAddress().getPort();
        String path = (String) inv.getObjectAttachmentWithoutConvert(PATH_KEY);

        // 判断是否为stub服务调用：客户端启用stub事件后，通常会设置onConnect或onDisconnect方法
        isStubServiceInvoke = Boolean.TRUE.toString().equals(inv.getObjectAttachmentWithoutConvert(STUB_EVENT_KEY));
        if (isStubServiceInvoke) {
            // stub服务导出到本地时，通常不会暴露端口，因此将端口设置为0
            port = 0;
        }

        // 判断是否为客户端侧的回调服务调用（排除stub服务场景）
        isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;
        if (isCallBackServiceInvoke) {
            // 为回调服务添加特殊路径后缀（原路径 + "." + 回调服务标识）
            path += "." + inv.getObjectAttachmentWithoutConvert(CALLBACK_SERVICE_KEY);
            // 在inv中标记这是回调服务调用，供后续处理使用
            inv.setObjectAttachment(IS_CALLBACK_SERVICE_INVOKE, Boolean.TRUE.toString());
        }

        // 生成服务唯一标识（端口+路径+版本+分组），从exporterMap中查找对应的DubboExporter
        String serviceKey = serviceKey(port, path, (String) inv.getObjectAttachmentWithoutConvert(VERSION_KEY), (String)
                inv.getObjectAttachmentWithoutConvert(GROUP_KEY));
        // 从exporterMap中获取对应的DubboExporter实例
        DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);

        // 如果未找到Exporter，抛出异常并提供详细的诊断信息（包括已导出的服务列表）
        if (exporter == null) {
            throw new RemotingException(
                    channel,
                    "Not found exported service: " + serviceKey + " in " + exporterMap.keySet()
                            + ", may be version or group mismatch " + ", channel: consumer: "
                            + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress() + ", message:"
                            + getInvocationWithoutData(inv));
        }

        // 获取Exporter中的Invoker，并将服务模型设置到invocation中以便后续使用
        Invoker<?> invoker = exporter.getInvoker();
        inv.setServiceModel(invoker.getUrl().getServiceModel());
        return invoker;
    }


    public Collection<Invoker<?>> getInvokers() {
        return Collections.unmodifiableCollection(invokers);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * 导出Dubbo协议的RPC服务，创建Exporter并启动底层网络服务器。
     * <p>
     * 该方法是Dubbo协议层面的服务导出入口，负责：
     * 1. 创建DubboExporter对象，将Invoker注册到本地缓存；
     * 2. 验证stub事件配置的有效性；
     * 3. 启动或复用ExchangeServer监听网络请求；
     * 4. 优化序列化方式以提升性能。
     * </p>
     *
     * @param invoker 待导出的服务Invoker，封装了服务接口的代理实现和URL配置信息
     * @return DubboExporter对象，包含Invoker引用和服务唯一标识key
     * @throws RpcException 当协议配置错误、服务器启动失败或序列化优化异常时抛出
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        checkDestroyed();
        URL url = invoker.getUrl();

        // 根据URL生成服务唯一标识（接口名:版本号:分组:端口），并创建DubboExporter注册到exporterMap
        String key = serviceKey(url);
        DubboExporter<T> exporter = new DubboExporter<>(invoker, key, exporterMap);

        // 验证stub事件支持配置：检查是否启用了stub事件但缺少对应的方法声明
        boolean isStubSupportEvent = url.getParameter(STUB_EVENT_KEY, DEFAULT_STUB_EVENT);
        boolean isCallbackService = url.getParameter(IS_CALLBACK_SERVICE, false);
        if (isStubSupportEvent && !isCallbackService) {
            String stubServiceMethods = url.getParameter(STUB_EVENT_METHODS_KEY);
            if (stubServiceMethods == null || stubServiceMethods.length() == 0) {
                if (logger.isWarnEnabled()) {
                    logger.warn(
                            PROTOCOL_UNSUPPORTED,
                            "",
                            "",
                            "consumer [" + url.getParameter(INTERFACE_KEY)
                                    + "], has set stub proxy support event ,but no stub methods founded.");
                }
            }
        }

        // 启动或复用ExchangeServer服务器实例，绑定指定端口监听客户端连接
        //当请求调发送过来 --> invocation -> key -> exporterMap.get(key)
        // -> exporter -> invoker -> invoker.invoke (invocation) -> 返回结果
        openServer(url);
        // 针对特定序列化方式（如Kryo、FST）进行优化配置，提升序列化性能
        optimizeSerialization(url);

        return exporter;
    }

    /**
     * 启动或复用Dubbo协议的网络服务器，实现单端口服务多个URL的能力。
     * <p>
     * 该方法采用双重检查锁（DCL）机制确保同一地址只创建一个服务器实例，
     * 支持服务器配置动态重置功能，用于处理override规则覆盖场景。
     * </p>
     *
     * @param url 服务配置的URL对象，包含服务器地址、端口、线程池等参数信息
     */
    private void openServer(URL url) {
        checkDestroyed();
        // 根据URL地址（host:port）生成服务器唯一标识，判断是否为服务端模式
        String key = url.getAddress();
        boolean isServer = url.getParameter(IS_SERVER_KEY, true);

        // 仅在服务端模式下执行服务器创建或复用逻辑
        if (isServer) {
            ProtocolServer server = serverMap.get(key);
            if (server == null) {
                // 使用双重检查锁防止并发创建多个服务器实例
                synchronized (this) {
                    server = serverMap.get(key);
                    if (server == null) {
                        // 首次创建该地址的服务器实例并缓存到serverMap
                        serverMap.put(key, createServer(url));
                        return;
                    }
                }
            }

            // 服务器已存在时执行重置操作，用于应用override规则动态更新配置（如线程池参数、超时时间等）
            server.reset(url);
        }
    }


    private void checkDestroyed() {
        if (destroyed.get()) {
            throw new IllegalStateException(getClass().getSimpleName() + " is destroyed");
        }
    }

    /**
     * 创建并启动Dubbo协议的网络服务器，配置底层通信和编解码器。
     * <p>
     * 该方法负责：
     * 1. 增强URL配置，添加通道关闭事件、心跳检测、编解码器等默认参数；
     * 2. 验证Transporter扩展类型（服务端和客户端）的合法性；
     * 3. 绑定网络端口并创建ExchangeServer实例；
     * 4. 封装为DefaultProtocolServer并加载服务器属性配置。
     * </p>
     *
     * @param url 服务配置的URL对象，包含服务器地址、端口、线程池等参数信息
     * @return DefaultProtocolServer对象，封装了底层的ExchangeServer实例
     * @throws RpcException 当服务器类型不支持、绑定失败或客户端类型不合法时抛出
     */
    private ProtocolServer createServer(URL url) {
        // 通过URLBuilder增强配置：启用服务器关闭时的只读事件通知、默认启用心跳检测、指定Dubbo编解码器
        url = URLBuilder.from(url)
                .addParameterIfAbsent(CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString())
                .addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT))
                .addParameter(CODEC_KEY, DubboCodec.NAME)
                .build();

        // 验证服务端Transporter类型的合法性，防止使用未注册的传输层实现
        String transporter = url.getParameter(SERVER_KEY, DEFAULT_REMOTING_SERVER);
        if (StringUtils.isNotEmpty(transporter)
                && !url.getOrDefaultFrameworkModel()
                        .getExtensionLoader(Transporter.class)
                        .hasExtension(transporter)) {
            throw new RpcException("Unsupported server type: " + transporter + ", url: " + url);
        }

        ExchangeServer server;
        try {
            // 绑定网络端口并创建ExchangeServer，注册统一的请求处理器requestHandler处理RPC调用
            server = Exchangers.bind(url, requestHandler);
        } catch (RemotingException e) {
            throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
        }

        // 验证客户端Transporter类型的合法性（用于该服务器作为客户端连接其他服务器时的配置校验）
        transporter = url.getParameter(CLIENT_KEY);
        if (StringUtils.isNotEmpty(transporter)
                && !url.getOrDefaultFrameworkModel()
                        .getExtensionLoader(Transporter.class)
                        .hasExtension(transporter)) {
            throw new RpcException("Unsupported client type: " + transporter);
        }

        // 封装为DefaultProtocolServer并加载服务器属性配置（如权重、预热时间等）
        DefaultProtocolServer protocolServer = new DefaultProtocolServer(server);
        loadServerProperties(protocolServer);
        return protocolServer;
    }


    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        checkDestroyed();
        return protocolBindingRefer(type, url);
    }

        /**
     * 创建基于Dubbo协议的RPC调用Invoker，负责建立网络连接并管理序列化优化。
     * <p>
     * 该方法是Dubbo协议消费者端的核心入口，主要执行以下操作：
     * <ol>
     *   <li>检查协议层是否已销毁，确保在关闭状态下不再创建新的Invoker</li>
     *   <li>调用optimizeSerialization()加载并注册URL中配置的序列化优化器（如Kryo、FST等），提升后续调用的序列化性能</li>
     *   <li>调用getClients(url)获取或创建与提供者通信的ExchangeClient数组（支持单连接或多连接配置）</li>
     *   <li>创建DubboInvoker实例，封装服务类型、URL、客户端连接集合以及全局Invoker列表引用</li>
     *   <li>将新创建的Invoker添加到invokers集合中进行统一管理，便于后续的资源清理和生命周期控制</li>
     * </ol>
     * </p>
     *
     * @param serviceType 服务接口类型，表示要引用的远程服务契约
     * @param url 服务URL，包含提供者地址、端口、协议版本、序列化方式等配置信息
     * @return 创建的DubboInvoker对象，用于发起基于Dubbo协议的远程RPC调用
     * @throws RpcException 当协议已销毁或创建连接失败时抛出
     */
    @Override
    public <T> Invoker<T> protocolBindingRefer(Class<T> serviceType, URL url) throws RpcException {
        checkDestroyed();
        optimizeSerialization(url);

        // create rpc invoker.
        DubboInvoker<T> invoker = new DubboInvoker<>(serviceType, url, getClients(url), invokers);
        invokers.add(invoker);

        return invoker;
    }


    /**
     * 根据URL配置获取客户端连接提供者，支持共享连接和独占连接两种模式。
     * <p>
     * 该方法负责决定消费者与提供者之间建立的网络连接策略：
     * <ul>
     *   <li><b>共享连接模式</b>（connections=0或未配置）：多个服务接口复用同一组物理连接，减少资源占用。
     *       连接数量由share.connections参数控制，优先从URL参数读取，其次从系统属性或配置文件获取，默认值为1。</li>
     *   <li><b>独占连接模式</b>（connections>0）：为当前服务单独创建指定数量的物理连接，适用于高吞吐场景，
     *       避免与其他服务竞争网络资源。</li>
     * </ul>
     * </p>
     *
     * @param url 服务URL，包含连接数配置、共享连接数配置等网络参数
     * @return ClientsProvider对象，提供用于RPC调用的ExchangeClient集合
     */
    private ClientsProvider getClients(URL url) {
        int connections = url.getParameter(CONNECTIONS_KEY, 0);
        // whether to share connection
        // if not configured, connection is shared, otherwise, one connection for one service
        if (connections == 0) {
            /*
             * The xml configuration should have a higher priority than properties.
             */
            /*
             * 共享连接模式：解析共享连接数配置，优先级为XML配置 > 系统属性/配置文件 > 默认值
             */
            String shareConnectionsStr = StringUtils.isBlank(url.getParameter(SHARE_CONNECTIONS_KEY, (String) null))
                    ? ConfigurationUtils.getProperty(
                            url.getOrDefaultApplicationModel(), SHARE_CONNECTIONS_KEY, DEFAULT_SHARE_CONNECTIONS)
                    : url.getParameter(SHARE_CONNECTIONS_KEY, (String) null);
            connections = Integer.parseInt(shareConnectionsStr);

            return getSharedClient(url, connections);
        }

        /*
         * 独占连接模式：为当前服务创建指定数量的独立物理连接
         */
        List<ExchangeClient> clients =
                IntStream.range(0, connections).mapToObj((i) -> initClient(url)).collect(Collectors.toList());
        return new ExclusiveClientsProvider(clients);
    }

    /**
     * Get shared connection
     *
     * @param url
     * @param connectNum connectNum must be greater than or equal to 1
     */
    @SuppressWarnings("unchecked")
    private SharedClientsProvider getSharedClient(URL url, int connectNum) {
        String key = url.getAddress();

        // connectNum must be greater than or equal to 1
        int expectedConnectNum = Math.max(connectNum, 1);
        return referenceClientMap.compute(key, (originKey, originValue) -> {
            if (originValue != null && originValue.increaseCount()) {
                return originValue;
            } else {
                return new SharedClientsProvider(
                        this, originKey, buildReferenceCountExchangeClientList(url, expectedConnectNum));
            }
        });
    }

    protected void scheduleRemoveSharedClient(String key, SharedClientsProvider sharedClient) {
        this.frameworkModel
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getSharedExecutor()
                .submit(() -> referenceClientMap.remove(key, sharedClient));
    }

    /**
     * Bulk build client
     *
     * @param url
     * @param connectNum
     * @return
     */
    private List<ReferenceCountExchangeClient> buildReferenceCountExchangeClientList(URL url, int connectNum) {
        List<ReferenceCountExchangeClient> clients = new ArrayList<>();

        for (int i = 0; i < connectNum; i++) {
            clients.add(buildReferenceCountExchangeClient(url));
        }

        return clients;
    }

    /**
     * Build a single client
     *
     * @param url
     * @return
     */
    private ReferenceCountExchangeClient buildReferenceCountExchangeClient(URL url) {
        ExchangeClient exchangeClient = initClient(url);
        ReferenceCountExchangeClient client = new ReferenceCountExchangeClient(exchangeClient, DubboCodec.NAME);
        // read configs
        int shutdownTimeout = ConfigurationUtils.getServerShutdownTimeout(url.getScopeModel());
        client.setShutdownWaitTime(shutdownTimeout);
        return client;
    }

    /**
     * Create new connection
     *
     * @param url
     */
    private ExchangeClient initClient(URL url) {
        /*
         * Instance of url is InstanceAddressURL, so addParameter actually adds parameters into ServiceInstance,
         * which means params are shared among different services. Since client is shared among services this is currently not a problem.
         */
        String str = url.getParameter(CLIENT_KEY, url.getParameter(SERVER_KEY, DEFAULT_REMOTING_CLIENT));

        // BIO is not allowed since it has severe performance issue.
        if (StringUtils.isNotEmpty(str)
                && !url.getOrDefaultFrameworkModel()
                        .getExtensionLoader(Transporter.class)
                        .hasExtension(str)) {
            throw new RpcException("Unsupported client type: " + str + "," + " supported client type is "
                    + StringUtils.join(
                            url.getOrDefaultFrameworkModel()
                                    .getExtensionLoader(Transporter.class)
                                    .getSupportedExtensions(),
                            " "));
        }

        try {
            ScopeModel scopeModel = url.getScopeModel();
            int heartbeat = UrlUtils.getHeartbeat(url);
            // Replace InstanceAddressURL with ServiceConfigURL.
            url = new ServiceConfigURL(
                    DubboCodec.NAME,
                    url.getUsername(),
                    url.getPassword(),
                    url.getHost(),
                    url.getPort(),
                    url.getPath(),
                    url.getAllParameters());
            url = url.addParameter(CODEC_KEY, DubboCodec.NAME);
            // enable heartbeat by default
            url = url.addParameterIfAbsent(HEARTBEAT_KEY, Integer.toString(heartbeat));
            url = url.setScopeModel(scopeModel);

            // connection should be lazy
            return url.getParameter(LAZY_CONNECT_KEY, false)
                    ? new LazyConnectExchangeClient(url, requestHandler)
                    : Exchangers.connect(url, requestHandler);
        } catch (RemotingException e) {
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Destroying protocol [" + this.getClass().getSimpleName() + "] ...");
        }
        for (String key : new ArrayList<>(serverMap.keySet())) {
            ProtocolServer protocolServer = serverMap.remove(key);

            if (protocolServer == null) {
                continue;
            }

            RemotingServer server = protocolServer.getRemotingServer();

            try {
                if (logger.isInfoEnabled()) {
                    logger.info("Closing dubbo server: " + server.getLocalAddress());
                }

                server.close(ConfigurationUtils.reCalShutdownTime(getServerShutdownTimeout(protocolServer)));

            } catch (Throwable t) {
                logger.warn(
                        PROTOCOL_ERROR_CLOSE_SERVER,
                        "",
                        "",
                        "Close dubbo server [" + server.getLocalAddress() + "] failed: " + t.getMessage(),
                        t);
            }
        }
        serverMap.clear();

        for (String key : new ArrayList<>(referenceClientMap.keySet())) {
            SharedClientsProvider clients = referenceClientMap.remove(key);
            clients.forceClose();
        }

        PortUnificationExchanger.close();
        referenceClientMap.clear();

        super.destroy();
    }

    /**
     * only log body in debugger mode for size & security consideration.
     *
     * @param invocation
     * @return
     */
    private Invocation getInvocationWithoutData(Invocation invocation) {
        if (logger.isDebugEnabled()) {
            return invocation;
        }
        if (invocation instanceof RpcInvocation) {
            RpcInvocation rpcInvocation = (RpcInvocation) invocation;
            rpcInvocation.setArguments(null);
            return rpcInvocation;
        }
        return invocation;
    }
}
