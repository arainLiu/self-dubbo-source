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
package org.apache.dubbo.spring.security.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.spring.security.utils.SecurityNames;

import java.util.Map;
import java.util.Objects;

import static org.apache.dubbo.spring.security.utils.SecurityNames.SECURITY_AUTHENTICATION_CONTEXT_KEY;

@Activate(group = CommonConstants.CONSUMER, order = -1)
public class ContextHolderParametersSelectedTransferFilter implements ClusterFilter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) {
        this.setSecurityContextIfExists(invocation);

        return invoker.invoke(invocation);
    }

    /**
     * 将服务端上下文中的安全认证信息转移到调用对象中，实现跨层级的身份传递。
     * <p>
     * 该方法检查当前 RpcContext 的服务端附件中是否包含安全认证对象（Authentication）。
     * 如果存在，则将其提取并设置到 Invocation 的对象附件中，确保在后续的远程调用或内部处理逻辑中，
     * 安全上下文能够被正确感知和传播。如果未找到认证信息，则直接返回，不做任何处理。
     * </p>
     *
     * @param invocation 当前的 RPC 调用对象，用于承载安全认证信息
     */
    private void setSecurityContextIfExists(Invocation invocation) {
        /*
         * 获取当前线程 RpcContext 中的所有服务端附件信息
         */
        Map<String, Object> resultMap = RpcContext.getServerAttachment().getObjectAttachments();

        /*
         * 尝试从附件中提取 Spring Security 的认证对象
         */
        Object authentication = resultMap.get(SECURITY_AUTHENTICATION_CONTEXT_KEY);

        if (Objects.isNull(authentication)) {
            return;
        }

        /*
         * 将认证对象重新设置到 Invocation 的附件中，以便向下游传递
         */
        invocation.setObjectAttachment(SecurityNames.SECURITY_AUTHENTICATION_CONTEXT_KEY, authentication);
    }
}
