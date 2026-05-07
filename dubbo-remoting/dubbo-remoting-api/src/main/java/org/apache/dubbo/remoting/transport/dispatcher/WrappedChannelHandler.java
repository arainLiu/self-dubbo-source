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
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.GlobalResourcesRepository;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;
import org.apache.dubbo.rpc.executor.ExecutorSupport;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class WrappedChannelHandler implements ChannelHandlerDelegate {

    protected static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(WrappedChannelHandler.class);

    protected final ChannelHandler handler;

    protected final URL url;

    protected final ExecutorSupport executorSupport;

    public WrappedChannelHandler(ChannelHandler handler, URL url) {
        this.handler = handler;
        this.url = url;
        this.executorSupport = ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
                .getExecutorSupport(url);
    }

    public void close() {}

    @Override
    public void connected(Channel channel) throws RemotingException {
        handler.connected(channel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        handler.disconnected(channel);
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        handler.sent(channel, message);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        handler.received(channel, message);
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        handler.caught(channel, exception);
    }

    protected void sendFeedback(Channel channel, Request request, Throwable t) throws RemotingException {

        if (!request.isTwoWay()) {
            return;
        }

        String msg = "Server side(" + url.getIp() + "," + url.getPort() + ") thread pool is exhausted, detail msg:"
                + t.getMessage();

        Response response = new Response(request.getId(), request.getVersion());
        response.setStatus(Response.SERVER_THREADPOOL_EXHAUSTED_ERROR);
        response.setErrorMessage(msg);

        channel.send(response);
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }

    public URL getUrl() {
        return url;
    }

    /**
     * Currently, this method is mainly customized to facilitate the thread model on consumer side.
     * 1. Use ThreadlessExecutor, aka., delegate callback directly to the thread initiating the call.
     * 2. Use shared executor to execute the callback.
     *
     * @param msg
     * @return
     */
    /**
     * 获取处理消息的首选线程池执行器
     * 该方法主要针对消费端的线程模型进行优化，根据消息类型选择合适的执行策略：
     * 1. 对于响应消息，优先使用发起调用的线程（ThreadlessExecutor）直接执行回调
     * 2. 其他情况使用共享的线程池执行器处理
     *
     * @param msg 待处理的消息对象，通常为Request或Response类型
     * @return 用于执行消息处理逻辑的线程池执行器
     */
    public ExecutorService getPreferredExecutorService(Object msg) {
        // 针对响应消息进行特殊处理，以支持调用线程直接执行回调
        if (msg instanceof Response) {
            Response response = (Response) msg;

            // 根据响应ID获取关联的DefaultFuture对象
            DefaultFuture responseFuture = DefaultFuture.getFuture(response.getId());

            // 如果Future不存在（如超时后返回的响应），则使用共享线程池处理
            if (responseFuture == null) {
                return getSharedExecutorService();
            } else {
                // 尝试获取Future绑定的专用执行器
                ExecutorService executor = responseFuture.getExecutor();

                // 如果专用执行器不可用或已关闭，则降级使用共享执行器
                if (executor == null || executor.isShutdown()) {
                    executor = getSharedExecutorService(msg);
                }
                return executor;
            }
        } else {
            // 非响应消息（如请求消息）直接使用共享线程池处理
            return getSharedExecutorService(msg);
        }
    }

    /**
     * @param msg  msg is the network message body, executorSupport.getExecutor needs it, and gets important information from it to get executor
     * @return
     */
    public ExecutorService getSharedExecutorService(Object msg) {
        Executor executor = executorSupport.getExecutor(msg);
        return executor != null ? (ExecutorService) executor : getSharedExecutorService();
    }

    /**
     * get the shared executor for current Server or Client
     *
     * @return
     */
    public ExecutorService getSharedExecutorService() {
        // Application may be destroyed before channel disconnected, avoid create new application model
        // see https://github.com/apache/dubbo/issues/9127
        if (url.getApplicationModel() == null || url.getApplicationModel().isDestroyed()) {
            return GlobalResourcesRepository.getGlobalExecutorService();
        }

        // note: url.getOrDefaultApplicationModel() may create new application model
        ApplicationModel applicationModel = url.getOrDefaultApplicationModel();

        ExecutorRepository executorRepository = ExecutorRepository.getInstance(applicationModel);

        ExecutorService executor = executorRepository.getExecutor(url);

        if (executor == null) {
            executor = executorRepository.createExecutorIfAbsent(url);
        }

        return executor;
    }

    @Deprecated
    public ExecutorService getExecutorService() {
        return getSharedExecutorService();
    }
}
