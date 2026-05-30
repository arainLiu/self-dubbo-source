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
package org.apache.dubbo.config.invoker;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/**
 * An invoker wrapper that wrap the invoker and all the metadata (ServiceConfig)
 * 提供者元数据委托 Invoker。
 * 该类包装了底层的 Invoker 实例以及对应的 ServiceConfig 配置对象。
 *
 * 主要作用：
 * 1. 在导出服务时，将服务配置元数据（ServiceConfig）与 Invoker 绑定在一起
 * 2. 方便后续在注册中心发布服务或处理治理规则时，能够直接从 Invoker 中获取完整的配置信息
 * 3. 作为配置层与协议层之间的桥梁，确保元数据能够随 Invoker 链路传递
 *
 * @param <T> 服务接口类型
 */
public class DelegateProviderMetaDataInvoker<T> implements Invoker {
    protected final Invoker<T> invoker;
    private final ServiceConfig<?> metadata;

    /**
     * 构造 DelegateProviderMetaDataInvoker 实例。
     *
     * @param invoker 被包装的底层 Invoker 实例
     * @param metadata 服务配置元数据，包含服务的各种配置参数
     */
    public DelegateProviderMetaDataInvoker(Invoker<T> invoker, ServiceConfig<?> metadata) {
        this.invoker = invoker;
        this.metadata = metadata;
    }

    @Override
    public Class<T> getInterface() {
        return invoker.getInterface();
    }

    @Override
    public URL getUrl() {
        return invoker.getUrl();
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

    /**
     * 获取服务配置元数据。
     *
     * @return ServiceConfig 配置对象，包含服务的分组、版本、超时时间等配置信息
     */
    public ServiceConfig<?> getMetadata() {
        return metadata;
    }
}
