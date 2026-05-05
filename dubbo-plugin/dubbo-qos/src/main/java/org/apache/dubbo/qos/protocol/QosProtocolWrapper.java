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
package org.apache.dubbo.qos.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.qos.api.PermissionLevel;
import org.apache.dubbo.qos.common.QosConstants;
import org.apache.dubbo.qos.server.Server;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.QOS_FAILED_START_SERVER;
import static org.apache.dubbo.common.constants.QosConstants.ACCEPT_FOREIGN_IP;
import static org.apache.dubbo.common.constants.QosConstants.ACCEPT_FOREIGN_IP_WHITELIST;
import static org.apache.dubbo.common.constants.QosConstants.ANONYMOUS_ACCESS_ALLOW_COMMANDS;
import static org.apache.dubbo.common.constants.QosConstants.ANONYMOUS_ACCESS_PERMISSION_LEVEL;
import static org.apache.dubbo.common.constants.QosConstants.QOS_CHECK;
import static org.apache.dubbo.common.constants.QosConstants.QOS_ENABLE;
import static org.apache.dubbo.common.constants.QosConstants.QOS_HOST;
import static org.apache.dubbo.common.constants.QosConstants.QOS_PORT;

@Activate(order = 200)
public class QosProtocolWrapper implements Protocol, ScopeModelAware {

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(QosProtocolWrapper.class);

    private final AtomicBoolean hasStarted = new AtomicBoolean(false);

    private final Protocol protocol;

    private FrameworkModel frameworkModel;

    public QosProtocolWrapper(Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol == null");
        }
        this.protocol = protocol;
    }

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
    }

    @Override
    public int getDefaultPort() {
        return protocol.getDefaultPort();
    }

        /**
     * 导出服务提供者Invoker，并在首次调用时启动QoS服务器。
     * <p>
     * 该方法是服务端服务暴露的入口点，在委托给底层协议执行实际的服务导出之前，
     * 会根据URL配置检查并启动QoS（Quality of Service）服务器，用于提供服务治理和监控能力。
     * 通过AtomicBoolean保证QoS服务器只启动一次，避免重复初始化。
     * </p>
     *
     * @param invoker 服务提供者的Invoker对象，包含服务接口、实现类引用、URL配置等信息
     * @return 已导出的Exporter对象，代表已注册的服务实例
     * @throws RpcException 当QoS检查启用且QoS服务器启动失败时抛出异常；若QoS检查未启用，则仅记录警告日志，不影响服务导出
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        /*
         * 根据服务URL配置启动QoS服务器（仅在首次调用时执行）
         * isServer参数为true表示这是服务端场景，启动失败时会抛出异常
         */
        startQosServer(invoker.getUrl(), true);
        /*
         * 委托给底层协议执行实际的服务导出逻辑
         */
        return protocol.export(invoker);
    }


    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        startQosServer(url, false);
        return protocol.refer(type, url);
    }

    @Override
    public void destroy() {
        protocol.destroy();
        stopServer();
    }

    @Override
    public List<ProtocolServer> getServers() {
        return protocol.getServers();
    }

        /**
     * 启动QoS服务器，根据URL配置初始化服务器的各项参数。
     * <p>
     * 该方法负责检查并启动QoS（Quality of Service）服务器，提供服务治理和监控能力。
     * 通过AtomicBoolean保证幂等性，确保服务器只启动一次。支持从URL中读取配置参数，
     * 包括主机地址、端口号、外部IP访问控制、匿名访问权限等。
     * </p>
     * <p>
     * 异常处理策略：
     * <ul>
     *   <li>如果启用了QoS检查（qos.check=true），启动失败时会尝试停止服务器以支持重新启动，并在服务端场景下抛出RpcException</li>
     *   <li>如果未启用QoS检查，仅记录警告日志，不影响正常业务流程</li>
     * </ul>
     * </p>
     *
     * @param url 包含QoS服务器配置的URL对象，可提取主机、端口、访问控制等参数
     * @param isServer 标识是否为服务端场景，用于决定启动失败时是否抛出异常（服务端导出服务时为true，客户端引用服务时为false）
     * @throws RpcException 当QoS检查启用且为服务端场景时，如果启动失败则抛出此异常
     */
    private void startQosServer(URL url, boolean isServer) throws RpcException {
        /*
         * 获取QoS检查开关，决定是否在启动失败时抛出异常
         */
        boolean qosCheck = url.getParameter(QOS_CHECK, false);

        try {
            /*
             * 使用CAS操作保证QoS服务器只启动一次，避免重复初始化
             */
            if (!hasStarted.compareAndSet(false, true)) {
                return;
            }

            /*
             * 检查QoS功能是否启用，未启用则直接返回
             */
            boolean qosEnable = url.getParameter(QOS_ENABLE, true);
            if (!qosEnable) {
                logger.info("qos won't be started because it is disabled. "
                        + "Please check dubbo.application.qos.enable is configured either in system property, "
                        + "dubbo.properties or XML/spring-boot configuration.");
                return;
            }

            /*
             * 从URL中提取QoS服务器的配置参数
             */
            String host = url.getParameter(QOS_HOST);
            int port = url.getParameter(QOS_PORT, QosConstants.DEFAULT_PORT);
            boolean acceptForeignIp = Boolean.parseBoolean(url.getParameter(ACCEPT_FOREIGN_IP, "false"));
            String acceptForeignIpWhitelist = url.getParameter(ACCEPT_FOREIGN_IP_WHITELIST, StringUtils.EMPTY_STRING);
            String anonymousAccessPermissionLevel =
                    url.getParameter(ANONYMOUS_ACCESS_PERMISSION_LEVEL, PermissionLevel.PUBLIC.name());
            String anonymousAllowCommands = url.getParameter(ANONYMOUS_ACCESS_ALLOW_COMMANDS, StringUtils.EMPTY_STRING);
            Server server = frameworkModel.getBeanFactory().getBean(Server.class);

            /*
             * 双重检查：如果服务器已经启动，则直接返回
             */
            if (server.isStarted()) {
                return;
            }

            /*
             * 配置QoS服务器参数并启动
             */
            server.setHost(host);
            server.setPort(port);
            server.setAcceptForeignIp(acceptForeignIp);
            server.setAcceptForeignIpWhitelist(acceptForeignIpWhitelist);
            server.setAnonymousAccessPermissionLevel(anonymousAccessPermissionLevel);
            server.setAnonymousAllowCommands(anonymousAllowCommands);
            server.start();

        } catch (Throwable throwable) {
            logger.warn(QOS_FAILED_START_SERVER, "", "", "Fail to start qos server: ", throwable);

            /*
             * 如果启用了QoS检查，则在启动失败时尝试停止服务器以支持重新启动
             */
            if (qosCheck) {
                try {
                    // Stop QoS Server to support re-start if Qos-Check is enabled
                    stopServer();
                } catch (Throwable stop) {
                    logger.warn(QOS_FAILED_START_SERVER, "", "", "Fail to stop qos server: ", stop);
                }
                /*
                 * 仅在服务端场景（服务导出）时抛出异常，客户端场景不抛出异常
                 */
                if (isServer) {
                    // Only throws exception when export services
                    throw new RpcException(throwable);
                }
            }
        }
    }


    /**
     * 停止QoS服务器，释放相关资源。
     * <p>
     * 该方法通过CAS操作确保只有成功将启动状态从true改为false时才会执行停止逻辑，
     * 避免重复停止或停止未启动的服务器。主要用于协议销毁或服务关闭场景。
     * </p>
     */
    void stopServer() {
        /*
         * 使用CAS操作确保只在已启动状态下执行停止逻辑，保证线程安全和幂等性
         */
        if (hasStarted.compareAndSet(true, false)) {
            Server server = frameworkModel.getBeanFactory().getBean(Server.class);
            /*
             * 双重检查服务器是否正在运行，避免停止未启动的服务器
             */
            if (server.isStarted()) {
                server.stop();
            }
        }
    }

}
