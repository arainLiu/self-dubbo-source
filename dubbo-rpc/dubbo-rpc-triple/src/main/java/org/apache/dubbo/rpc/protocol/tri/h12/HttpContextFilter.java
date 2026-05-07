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
package org.apache.dubbo.rpc.protocol.tri.h12;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.http12.HttpRequest;
import org.apache.dubbo.remoting.http12.HttpResponse;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcServiceContext;
import org.apache.dubbo.rpc.protocol.tri.TripleConstants;

@Activate(group = CommonConstants.PROVIDER, order = -29000)
public class HttpContextFilter implements Filter {

    /**
     * 处理HTTP上下文信息，将请求和响应对象注入到RpcContext中
     * 仅当调用链路中存在处理器类型标识时才执行上下文设置逻辑
     *
     * @param invoker 调用器对象，用于执行后续的RPC调用链
     * @param invocation 调用上下文对象，包含HTTP请求、响应及处理器类型等信息
     * @return RPC调用结果
     * @throws RpcException 当RPC调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 检查是否存在处理器类型标识，不存在则跳过上下文处理直接调用
        if (invocation.get(TripleConstants.HANDLER_TYPE_KEY) == null) {
            return invoker.invoke(invocation);
        }

        // 从Invocation中提取HTTP请求和响应对象
        HttpRequest request = (HttpRequest) invocation.get(TripleConstants.HTTP_REQUEST_KEY);
        HttpResponse response = (HttpResponse) invocation.get(TripleConstants.HTTP_RESPONSE_KEY);

        // 获取当前服务的RPC上下文，并设置远程和本地地址信息
        RpcServiceContext context = RpcContext.getServiceContext();
        context.setRemoteAddress(request.remoteHost(), request.remotePort());
        if (context.getLocalAddress() == null) {
            context.setLocalAddress(request.localHost(), request.localPort());
        }

        // 将HTTP请求和响应对象注入到上下文中，供后续业务逻辑使用
        context.setRequest(request);
        context.setResponse(response);
        return invoker.invoke(invocation);
    }
}
