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
package org.apache.dubbo.rpc.cluster.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_FILTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SERVICE_FILTER_KEY;

@Activate(order = 100)
public class ProtocolFilterWrapper implements Protocol {

    private final Protocol protocol;

    public ProtocolFilterWrapper(Protocol protocol) {
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
     * 导出服务提供者，为Invoker构建过滤器链以增强RPC调用功能。
     * <p>
     * 该方法是Dubbo服务导出流程中的关键装饰器，负责在真正的协议层导出之前，
     * 根据配置动态组装过滤器链（如日志、监控、限流、认证等），将原始Invoker包装为具备横切关注点能力的增强Invoker。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>注册中心URL判断</b>：如果invoker的URL是registry://协议，说明是RegistryProtocol层面的导出，直接委托给底层protocol，不构建过滤器链</li>
     *   <li><b>获取过滤器链构建器</b>：调用getFilterChainBuilder从SPI扩展中获取激活的FilterChainBuilder实现（默认为DefaultFilterChainBuilder）</li>
     *   <li><b>构建过滤器链</b>：调用builder.buildInvokerChain传入SERVICE_FILTER_KEY和PROVIDER标识，加载所有激活的Provider端Filter，按order排序后依次包装Invoker</li>
     *   <li><b>协议层导出</b>：将增强后的Invoker委托给底层protocol（如DubboProtocol）进行真正的网络暴露和端口绑定</li>
     * </ol>
     * </p>
     *
     * @param invoker 服务提供者的调用器对象，包含服务接口、实现类引用、URL配置等信息
     * @return 经过过滤器链增强的Exporter对象，负责接收远程调用并依次执行过滤器逻辑
     * @throws RpcException 当过滤器链构建失败、端口绑定异常或协议层导出出错时抛出RPC异常
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        if (UrlUtils.isRegistry(invoker.getUrl())) {
            return protocol.export(invoker);
        }
        FilterChainBuilder builder = getFilterChainBuilder(invoker.getUrl());
        return protocol.export(builder.buildInvokerChain(invoker, SERVICE_FILTER_KEY, CommonConstants.PROVIDER));
    }

    private <T> FilterChainBuilder getFilterChainBuilder(URL url) {
        return ScopeModelUtil.getExtensionLoader(FilterChainBuilder.class, url.getScopeModel())
                .getDefaultExtension();
    }

    /**
     * 创建服务引用的Invoker，并为非注册中心场景构建过滤器链。
     * <p>
     * 该方法在消费者引用服务时被调用，负责根据URL类型决定是否应用过滤器链：
     * <ul>
     *   <li>如果是注册中心URL（registry://协议），直接委托给底层protocol创建Invoker，不应用过滤器</li>
     *   <li>如果是普通服务URL，先调用底层protocol创建基础Invoker，再通过FilterChainBuilder构建包含所有激活过滤器的责任链</li>
     * </ul>
     * </p>
     * <p>
     * 这种设计确保了过滤器只在真正的服务调用层面生效，而在注册中心交互层面（如订阅、发现等）不执行额外的过滤逻辑。
     * </p>
     *
     * @param type 服务接口类型，表示要引用的远程服务契约
     * @param url 服务URL，包含注册地址、协议、参数等配置信息
     * @return 创建的Invoker对象，可能包含过滤器链增强的逻辑
     * @throws RpcException 当创建Invoker失败时抛出
     */
    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        if (UrlUtils.isRegistry(url)) {
            return protocol.refer(type, url);
        }
        FilterChainBuilder builder = getFilterChainBuilder(url);
        return builder.buildInvokerChain(protocol.refer(type, url), REFERENCE_FILTER_KEY, CommonConstants.CONSUMER);
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
