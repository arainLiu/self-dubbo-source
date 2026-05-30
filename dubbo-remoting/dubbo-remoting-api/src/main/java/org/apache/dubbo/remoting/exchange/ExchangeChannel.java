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
package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * ExchangeChannel. (API/SPI, Prototype, ThreadSafe)
 * 交换层通道接口，扩展自底层 Channel，提供了基于请求-响应模式的通信能力。
 * 该接口定义了发送请求并获取异步响应 Future 的方法，是 Dubbo RPC 调用的核心通信抽象。
 */
public interface ExchangeChannel extends Channel {

    /**
     * send request.
     *
     * @param request
     * @return response future
     * @throws RemotingException
     * @deprecated 使用带有 ExecutorService 参数的方法以支持更灵活的线程调度
     */
    @Deprecated
    CompletableFuture<Object> request(Object request) throws RemotingException;

    /**
     * send request.
     *
     * @param request
     * @param timeout
     * @return response future
     * @throws RemotingException
     * @deprecated 使用带有 ExecutorService 参数的方法以支持更灵活的线程调度
     */
    @Deprecated
    CompletableFuture<Object> request(Object request, int timeout) throws RemotingException;

    /**
     * 发送请求并返回响应的异步 Future。
     * 该方法使用默认的超时时间和指定的线程执行器来处理响应回调。
     *
     * @param request 要发送的请求对象
     * @param executor 用于处理响应回调的线程执行器
     * @return 代表响应结果的 CompletableFuture 对象
     * @throws RemotingException 当发送请求失败时抛出异常
     */
    CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException;

    /**
     * 发送请求并返回响应的异步 Future。
     * 该方法允许指定超时时间和用于处理响应回调的线程执行器。
     *
     * @param request 要发送的请求对象
     * @param timeout 等待响应的超时时间（毫秒）
     * @param executor 用于处理响应回调的线程执行器
     * @return 代表响应结果的 CompletableFuture 对象
     * @throws RemotingException 当发送请求失败或超时时抛出异常
     */
    CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException;

    /**
     * get message handler.
     *
     * @return message handler
     * 获取当前通道关联的消息处理器。
     *
     * @return 交换层消息处理器实例
     */
    ExchangeHandler getExchangeHandler();

    /**
     * graceful close.
     *
     * @param timeout
     * 优雅关闭通道，在指定的超时时间内等待所有待处理的请求完成。
     *
     * @param timeout 关闭操作的超时时间（毫秒）
     */
    @Override
    void close(int timeout);
}
