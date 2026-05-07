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
import org.apache.dubbo.remoting.http12.HttpResponse;
import org.apache.dubbo.remoting.http12.HttpResult;
import org.apache.dubbo.remoting.http12.exception.HttpResultPayloadException;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.tri.TripleConstants;

@Activate(group = CommonConstants.PROVIDER, order = 10)
public class HttpContextCallbackFilter implements Filter, BaseFilter.Listener {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    /**
     * 处理HTTP上下文回调的响应结果，将HttpResponse对象转换为HttpResult并注入到Result中
     * 针对gRPC处理器类型和非gRPC类型进行差异化处理，同时处理异常情况的转换
     *
     * @param appResponse RPC调用的应用层响应结果，用于设置最终的返回值或异常
     * @param invoker 调用器对象，用于获取服务接口等信息
     * @param invocation 调用上下文对象，包含处理器类型标识和HTTP响应对象
     */
    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 检查是否存在处理器类型标识，不存在则直接返回
        Object handlerType = invocation.get(TripleConstants.HANDLER_TYPE_KEY);
        if (handlerType == null) {
            return;
        }

        // 处理HTTP结果负载异常，将其转换为标准的HttpResult对象
        Throwable exception = appResponse.getException();
        if (exception instanceof HttpResultPayloadException) {
            // 根据处理器类型选择不同的异常处理策略：gRPC类型包装为HttpResult，其他类型提取原始结果
            Object value = TripleConstants.TRIPLE_HANDLER_TYPE_GRPC.equals(handlerType)
                    ? HttpResult.of(exception)
                    : ((HttpResultPayloadException) exception).getResult();
            appResponse.setValue(value);
            appResponse.setException(null);
            return;
        }

        // 获取HTTP响应对象，如果为空则直接返回
        HttpResponse response = (HttpResponse) invocation.get(TripleConstants.HTTP_RESPONSE_KEY);
        if (response.isEmpty()) {
            return;
        }

        // 如果响应尚未提交，则设置响应体并提交响应
        if (!response.isCommitted()) {
            if (response.isContentEmpty()) {
                // 根据调用结果设置响应体：有异常时设置异常对象，否则设置返回值
                response.setBody(appResponse.hasException() ? appResponse.getException() : appResponse.getValue());
            }
            response.commit();
        }

        // 将HttpResponse转换为HttpResult对象，并根据内容类型设置到appResponse中
        HttpResult<Object> result = response.toHttpResult();
        if (result.getBody() instanceof Throwable) {
            // 如果响应体是异常对象，则设置为异常
            appResponse.setException((Throwable) result.getBody());
        } else {
            // 否则将HttpResult设置为返回值，并清除异常
            appResponse.setValue(result);
            appResponse.setException(null);
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {}
}
