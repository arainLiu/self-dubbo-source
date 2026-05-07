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
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

import static org.apache.dubbo.common.constants.CommonConstants.STAGED_CLASSLOADER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.WORKING_CLASSLOADER_KEY;

/**
 * Set the current execution thread class loader to service interface's class loader.
 */
@Activate(group = CommonConstants.PROVIDER, order = -30000)
public class ClassLoaderFilter implements Filter, BaseFilter.Listener {

        /**
     * 切换线程上下文类加载器，确保RPC调用在正确的类加载器环境下执行
     * 根据服务模型或Invoker类型确定目标类加载器，并在调用结束后恢复原始类加载器
     *
     * @param invoker 服务调用器，用于获取Invoker自身的类加载器作为备选
     * @param invocation 调用上下文对象，包含服务模型信息及用于存储类加载器引用
     * @return RPC调用结果
     * @throws RpcException 当RPC调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 保存当前线程的原始上下文类加载器，以便后续恢复
        ClassLoader stagedClassLoader = Thread.currentThread().getContextClassLoader();

        // 确定本次调用应使用的有效类加载器：优先使用服务模型中的类加载器，否则使用Invoker的类加载器
        ClassLoader effectiveClassLoader;
        if (invocation.getServiceModel() != null) {
            effectiveClassLoader = invocation.getServiceModel().getClassLoader();
        } else {
            effectiveClassLoader = invoker.getClass().getClassLoader();
        }

        // 如果确定了有效的目标类加载器，则进行切换并记录到invocation上下文中
        if (effectiveClassLoader != null) {
            invocation.put(STAGED_CLASSLOADER_KEY, stagedClassLoader);
            invocation.put(WORKING_CLASSLOADER_KEY, effectiveClassLoader);

            Thread.currentThread().setContextClassLoader(effectiveClassLoader);
        }
        try {
            // 执行后续的调用链，此时线程上下文已切换到目标类加载器
            return invoker.invoke(invocation);
        } finally {
            // 无论调用成功与否，都必须将线程上下文类加载器恢复为原始值，防止内存泄漏或类加载冲突
            Thread.currentThread().setContextClassLoader(stagedClassLoader);
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        resetClassLoader(invoker, invocation);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        resetClassLoader(invoker, invocation);
    }

    private void resetClassLoader(Invoker<?> invoker, Invocation invocation) {
        ClassLoader stagedClassLoader = (ClassLoader) invocation.get(STAGED_CLASSLOADER_KEY);
        if (stagedClassLoader != null) {
            Thread.currentThread().setContextClassLoader(stagedClassLoader);
        }
    }
}
