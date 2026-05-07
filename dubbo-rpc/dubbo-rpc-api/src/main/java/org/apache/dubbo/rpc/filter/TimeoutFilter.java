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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.support.RpcUtils;

import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_TIMEOUT_REQUEST;

/**
 * Log any invocation timeout, but don't stop server from running
 */
@Activate(group = CommonConstants.PROVIDER)
public class TimeoutFilter implements Filter, Filter.Listener {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(TimeoutFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    /**
     * 处理响应时的超时检查逻辑
     * 从服务端上下文中获取超时倒计时器，如果调用已超时则记录警告日志
     *
     * @param appResponse RPC调用的应用层响应结果
     * @param invoker 调用器对象，用于获取URL配置信息
     * @param invocation 调用上下文对象，包含方法名等元数据
     */
    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 从服务端附件中获取超时倒计时器对象
        Object obj = RpcContext.getServerAttachment().getObjectAttachment(TIME_COUNTDOWN_KEY);
        if (obj != null) {
            TimeoutCountDown countDown = (TimeoutCountDown) obj;

            // 检查调用是否已经超时，如果超时则记录警告日志
            if (countDown.isExpired()) {
                if (logger.isWarnEnabled()) {
                    logger.warn(
                            PROXY_TIMEOUT_REQUEST,
                            "",
                            "",
                            "invoke timed out. method: " + RpcUtils.getMethodName(invocation) + " url is "
                                    + invoker.getUrl() + ", invoke elapsed " + countDown.elapsedMillis() + " ms.");
                }
            }
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {}
}
