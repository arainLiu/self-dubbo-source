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
package org.apache.dubbo.rpc.cluster.filter.support;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.ServiceModel;

import java.util.Optional;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;

@Activate(group = CONSUMER, order = Integer.MIN_VALUE + 100)
public class ConsumerClassLoaderFilter implements ClusterFilter {
    /**
     * 在RPC调用期间切换线程上下文类加载器，确保服务模型隔离环境下的类加载正确性。
     * <p>
     * 该过滤器用于解决多模块或多租户场景下的类加载冲突问题。在执行远程调用前，它会尝试从Invocation关联的ServiceModel中获取专属的ClassLoader并设置为当前线程的上下文类加载器。
     * 这样可以确保在序列化、反序列化或执行回调逻辑时，能够正确加载到用户应用定义的类，而不是错误地加载到框架层或其他模块的同名类。
     * 调用结束后，无论成功与否，都会在finally块中恢复原始的类加载器，避免对线程池中的其他任务产生副作用。
     * </p>
     *
     * @param invoker 当前调用的执行器，代表远程服务的代理
     * @param invocation 封装了服务模型及调用信息的对象，用于提取目标ClassLoader
     * @return 远程调用的执行结果
     * @throws RpcException 当调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            /*
             * 如果服务模型中配置了专属类加载器，则临时切换当前线程的上下文类加载器
             */
            Optional.ofNullable(invocation.getServiceModel())
                    .map(ServiceModel::getClassLoader)
                    .ifPresent(Thread.currentThread()::setContextClassLoader);
            return invoker.invoke(invocation);
        } finally {
            /*
             * 确保调用结束后恢复原始类加载器，防止线程复用时的环境污染
             */
            Thread.currentThread().setContextClassLoader(originClassLoader);
        }
    }
}
