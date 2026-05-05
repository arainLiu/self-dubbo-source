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
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.SerializeSecurityConfigurator;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceMetadata;
import org.apache.dubbo.rpc.model.ServiceModel;

import java.util.List;
import java.util.Optional;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;

@Activate(order = 200)
public class ProtocolSecurityWrapper implements Protocol {
    private final Protocol protocol;

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(ProtocolSecurityWrapper.class);

    public ProtocolSecurityWrapper(Protocol protocol) {
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
     * 导出服务并执行序列化安全检查
     * <p>
     * 该方法是Protocol接口的装饰器实现，在导出服务之前执行序列化安全相关的初始化和检查：
     * 1. 获取ServiceModel和ScopeModel，用于定位模块级别的配置
     * 2. 获取SerializeSecurityConfigurator Bean，负责序列化安全管理
     * 3. 刷新序列化安全检查的状态和配置
     * 4. 注册需要安全检查的接口类，包括：
     *    - Invoker直接关联的接口
     *    - ServiceModel中ServiceDescriptor描述的服务接口
     *    - ServiceMetadata中定义的服务类型
     * 5. 如果安全检查初始化失败，记录错误日志但不影响服务导出
     * 6. 调用底层protocol.export()完成实际的服务导出
     * <p>
     * 该设计确保所有导出的Dubbo服务都经过序列化安全检查，
     * 防止反序列化漏洞带来的安全风险。
     *
     * @param invoker 服务Invoker对象，包含服务调用的所有信息
     * @param <T> 服务接口的泛型类型
     * @return 导出后的Exporter对象，用于管理服务的生命周期
     * @throws RpcException 当RPC调用或协议处理发生错误时抛出
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        try {
            // 获取服务模型和作用域模型
            ServiceModel serviceModel = invoker.getUrl().getServiceModel();
            ScopeModel scopeModel = invoker.getUrl().getScopeModel();

            // 获取模块级别的SerializeSecurityConfigurator，用于序列化安全管理
            SerializeSecurityConfigurator serializeSecurityConfigurator = ScopeModelUtil.getModuleModel(scopeModel)
                    .getBeanFactory()
                    .getBean(SerializeSecurityConfigurator.class);

            // 刷新序列化安全检查的状态和配置
            serializeSecurityConfigurator.refreshStatus();
            serializeSecurityConfigurator.refreshCheck();

            // 注册Invoker直接关联的接口到安全检查器
            Optional.ofNullable(invoker.getInterface()).ifPresent(serializeSecurityConfigurator::registerInterface);

            // 注册ServiceDescriptor中描述的服务接口到安全检查器
            Optional.ofNullable(serviceModel)
                    .map(ServiceModel::getServiceModel)
                    .map(ServiceDescriptor::getServiceInterfaceClass)
                    .ifPresent(serializeSecurityConfigurator::registerInterface);

            // 注册ServiceMetadata中定义的服务类型到安全检查器
            Optional.ofNullable(serviceModel)
                    .map(ServiceModel::getServiceMetadata)
                    .map(ServiceMetadata::getServiceType)
                    .ifPresent(serializeSecurityConfigurator::registerInterface);
        } catch (Throwable t) {
            // 安全检查初始化失败不影响服务导出，仅记录错误日志
            logger.error(INTERNAL_ERROR, "", "", "Failed to register interface for security check", t);
        }

        // 调用底层protocol完成实际的服务导出
        return protocol.export(invoker);
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        try {
            ServiceModel serviceModel = url.getServiceModel();
            ScopeModel scopeModel = url.getScopeModel();
            SerializeSecurityConfigurator serializeSecurityConfigurator = ScopeModelUtil.getModuleModel(scopeModel)
                    .getBeanFactory()
                    .getBean(SerializeSecurityConfigurator.class);
            serializeSecurityConfigurator.refreshStatus();
            serializeSecurityConfigurator.refreshCheck();

            Optional.ofNullable(serviceModel)
                    .map(ServiceModel::getServiceModel)
                    .map(ServiceDescriptor::getServiceInterfaceClass)
                    .ifPresent(serializeSecurityConfigurator::registerInterface);

            Optional.ofNullable(serviceModel)
                    .map(ServiceModel::getServiceMetadata)
                    .map(ServiceMetadata::getServiceType)
                    .ifPresent(serializeSecurityConfigurator::registerInterface);
            serializeSecurityConfigurator.registerInterface(type);
        } catch (Throwable t) {
            logger.error(INTERNAL_ERROR, "", "", "Failed to register interface for security check", t);
        }

        return protocol.refer(type, url);
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
