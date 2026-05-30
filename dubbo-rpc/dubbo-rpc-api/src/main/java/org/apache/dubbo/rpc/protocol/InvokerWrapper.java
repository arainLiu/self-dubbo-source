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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/**
 * InvokerWrapper
 * Invoker 的包装类实现，用于封装底层的 Invoker 实例并持有独立的 URL。
 * 该类通常用于在 Invoker 生命周期管理（如销毁、状态检查）时提供一个稳定的代理层，
 * 即使底层 Invoker 发生变化或被销毁，外层依然可以保持对原始 URL 和接口类型的引用。
 *
 * @param <T> 服务接口类型
 */
public class InvokerWrapper<T> implements Invoker<T> {

    protected final Invoker<T> invoker;

    private final URL url;

    /**
     * 构造 InvokerWrapper 实例。
     *
     * @param invoker 被包装的底层 Invoker 实例
     * @param url 该 Invoker 对应的服务 URL
     */
    public InvokerWrapper(Invoker<T> invoker, URL url) {
        this.invoker = invoker;
        this.url = url;
    }

    @Override
    public Class<T> getInterface() {
        return invoker.getInterface();
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return invoker.isAvailable();
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    @Override
    public void destroy() {
        invoker.destroy();
    }
}
