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
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ExporterListener;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.InvokerListener;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.listener.InjvmExporterListener;
import org.apache.dubbo.rpc.listener.ListenerExporterWrapper;
import org.apache.dubbo.rpc.listener.ListenerInvokerWrapper;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.Collections;
import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.EXPORTER_LISTENER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INVOKER_LISTENER_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_TYPE_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;

/**
 * ListenerProtocol
 */
@Activate(order = 200)
public class ProtocolListenerWrapper implements Protocol {

    private final Protocol protocol;

    public ProtocolListenerWrapper(Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol == null");
        }
        this.protocol = protocol;
    }

    @Override
    public int getDefaultPort() {
        return protocol.getDefaultPort();
    }

    /**
     * 导出服务并包装Exporter监听器
     * <p>
     * 该方法是Protocol接口的装饰器实现，负责为导出的Exporter添加监听器功能：
     * 1. 检查URL是否为注册中心类型，如果是则直接导出（不添加监听器）
     * 2. 从ScopeModel中获取激活的ExporterListener扩展列表
     * 3. 对于injvm协议（本地协议），额外添加InjvmExporterListener监听器
     * 4. 将实际导出的Exporter和监听器列表包装成ListenerExporterWrapper
     * <p>
     * 该设计使得Exporter的生命周期事件（如unexport）可以被监听和处理，
     * 支持SPI机制动态激活监听器，提供了灵活的扩展能力。
     *
     * @param invoker 服务Invoker对象，包含服务调用的所有信息
     * @param <T> 服务接口的泛型类型
     * @return 包装了监听器的ListenerExporterWrapper对象
     * @throws RpcException 当RPC调用或协议处理发生错误时抛出
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        // 如果是注册中心URL，直接导出，不添加Exporter监听器
        if (UrlUtils.isRegistry(invoker.getUrl())) {
            return protocol.export(invoker);
        }

        // 获取激活的ExporterListener扩展列表
        List<ExporterListener> exporterListeners = ScopeModelUtil.getExtensionLoader(
                        ExporterListener.class, invoker.getUrl().getScopeModel())
                .getActivateExtension(invoker.getUrl(), EXPORTER_LISTENER_KEY);

        // 对于injvm本地协议，额外添加InjvmExporterListener监听器
        if (LOCAL_PROTOCOL.equals(invoker.getUrl().getProtocol())) {
            exporterListeners.add(invoker.getUrl()
                    .getOrDefaultFrameworkModel()
                    .getBeanFactory()
                    .getBean(InjvmExporterListener.class));
        }

        // 将Exporter和监听器列表包装成ListenerExporterWrapper，实现监听功能
        return new ListenerExporterWrapper<>(protocol.export(invoker), Collections.unmodifiableList(exporterListeners));
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        if (UrlUtils.isRegistry(url)) {
            return protocol.refer(type, url);
        }

        Invoker<T> invoker = protocol.refer(type, url);
        if (StringUtils.isEmpty(url.getParameter(REGISTRY_CLUSTER_TYPE_KEY))) {
            invoker = new ListenerInvokerWrapper<>(
                    invoker,
                    Collections.unmodifiableList(ScopeModelUtil.getExtensionLoader(
                                    InvokerListener.class, invoker.getUrl().getScopeModel())
                            .getActivateExtension(url, INVOKER_LISTENER_KEY)));
        }
        return invoker;
    }

    @Override
    public void destroy() {
        protocol.destroy();
    }

    @Override
    public List<ProtocolServer> getServers() {
        return protocol.getServers();
    }
}
