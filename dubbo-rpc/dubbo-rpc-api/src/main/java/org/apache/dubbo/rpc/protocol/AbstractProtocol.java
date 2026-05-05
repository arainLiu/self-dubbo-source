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
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.support.SerializableClassRegistry;
import org.apache.dubbo.common.serialize.support.SerializationOptimizer;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_SERVER_SHUTDOWN_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.OPTIMIZER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_DESTROY_INVOKER;

/**
 * abstract ProtocolSupport.
 */
/**
 * Dubbo RPC协议的抽象基类，提供协议实现的通用功能和基础设施。
 * <p>
 * 该类实现了{@link Protocol}接口的核心功能，为具体的协议实现（如DubboProtocol、RestProtocol等）提供以下支持：
 * <ul>
 *   <li><b>Exporter管理</b>：通过exporterMap维护服务导出器的映射关系，key为serviceKey（host:port/path/version/group），value为Exporter对象</li>
 *   <li><b>服务器管理</b>：通过serverMap维护协议服务器的映射关系，key为host:port，value为ProtocolServer对象，支持多端口多协议场景</li>
 *   <li><b>Invoker管理</b>：通过invokers集合跟踪所有创建的消费者引用，在协议销毁时统一清理资源</li>
 *   <li><b>优雅关闭</b>：在destroy()方法中依次销毁所有Invoker和Exporter，确保资源正确释放，异常不会中断整体流程</li>
 *   <li><b>序列化优化</b>：提供optimizeSerialization()方法，支持动态加载和注册序列化优化器（如Kryo、FST等）</li>
 * </ul>
 * </p>
 * <p>
 * 典型使用场景：
 * <ol>
 *   <li>具体协议继承AbstractProtocol，实现protocolBindingRefer()和export()方法</li>
 *   <li>服务导出时调用export()方法，将Exporter注册到exporterMap中</li>
 *   <li>服务引用时调用refer()方法，将Invoker添加到invokers集合中</li>
 *   <li>应用关闭时调用destroy()方法，清理所有已导出的服务和引用的服务</li>
 * </ol>
 * </p>
 */
public abstract class AbstractProtocol implements Protocol, ScopeModelAware {

    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    /**
     * 服务导出器映射表，存储所有已导出的服务。
     * <p>
     * Key格式：serviceKey = host:port/path/serviceName:version:group
     * Value：对应的Exporter对象，包含Invoker和导出状态信息
     * </p>
     */
    protected final Map<String, Exporter<?>> exporterMap = new ConcurrentHashMap<>();

    /**
     * 协议服务器映射表，按地址索引管理的服务器实例。
     * <p>
     * Key格式：host:port，表示服务器绑定的网络地址
     * Value：ProtocolServer对象，代表一个正在运行的协议服务器
     * </p>
     */
    protected final ConcurrentMap<String, ProtocolServer> serverMap = new ConcurrentHashMap<>();

    // TODO SoftReference
    protected final Set<Invoker<?>> invokers = new ConcurrentHashSet<>();

    protected FrameworkModel frameworkModel;

    private final Set<String> optimizers = new ConcurrentHashSet<>();

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
    }

    /**
     * 根据URL生成服务的唯一标识键。
     * <p>
     * 该方法从URL中提取绑定端口、服务路径、版本号和分组信息，组合成全局唯一的服务键，
     * 用于在exporterMap中索引和管理服务导出器。
     * </p>
     *
     * @param url 服务URL，包含端口、路径、版本、分组等信息
     * @return 服务唯一标识键，格式为：port/path/serviceName:version:group
     */
    protected static String serviceKey(URL url) {
        int port = url.getParameter(Constants.BIND_PORT_KEY, url.getPort());
        return serviceKey(port, url.getPath(), url.getVersion(), url.getGroup());
    }

    /**
     * 根据服务参数生成服务的唯一标识键。
     *
     * @param port 服务端口号
     * @param serviceName 服务路径或接口名称
     * @param serviceVersion 服务版本号
     * @param serviceGroup 服务分组名
     * @return 服务唯一标识键，格式为：port/path/serviceName:version:group
     */
    protected static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
        return ProtocolUtils.serviceKey(port, serviceName, serviceVersion, serviceGroup);
    }

    @Override
    public List<ProtocolServer> getServers() {
        return Collections.unmodifiableList(new ArrayList<>(serverMap.values()));
    }

    protected void loadServerProperties(ProtocolServer server) {
        // read and hold config before destroy
        int serverShutdownTimeout =
                ConfigurationUtils.getServerShutdownTimeout(server.getUrl().getScopeModel());
        server.getAttributes().put(SHUTDOWN_WAIT_KEY, serverShutdownTimeout);
    }

    /**
     * 获取服务器的关闭超时时间。
     *
     * @param server 协议服务器对象
     * @return 关闭超时时间（毫秒），如果未配置则返回默认值DEFAULT_SERVER_SHUTDOWN_TIMEOUT
     */
    protected int getServerShutdownTimeout(ProtocolServer server) {
        return (int) server.getAttributes().getOrDefault(SHUTDOWN_WAIT_KEY, DEFAULT_SERVER_SHUTDOWN_TIMEOUT);
    }

    /**
     * 销毁协议占用的所有资源，包括所有引用的服务和导出的服务。
     * <p>
     * 该方法按照以下顺序执行清理操作：
     * <ol>
     *   <li>遍历并销毁所有Invoker（服务引用），记录日志并捕获异常避免中断流程</li>
     *   <li>清空invokers集合，释放引用</li>
     *   <li>遍历并取消导出所有Exporter（服务导出），记录日志并捕获异常</li>
     *   <li>清空exporterMap集合，完成资源释放</li>
     * </ol>
     * </p>
     * <p>
     * 异常处理策略：任何单个Invoker或Exporter的销毁失败都不会影响其他资源的清理，
     * 确保最大程度的资源回收。
     * </p>
     */
    @Override
    public void destroy() {
        for (Invoker<?> invoker : invokers) {
            if (invoker != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Destroy reference: " + invoker.getUrl());
                    }
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn(PROTOCOL_FAILED_DESTROY_INVOKER, "", "", t.getMessage(), t);
                }
            }
        }
        invokers.clear();

        exporterMap.forEach((key, exporter) -> {
            if (exporter != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Unexport service: " + exporter.getInvoker().getUrl());
                    }
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn(PROTOCOL_FAILED_DESTROY_INVOKER, "", "", t.getMessage(), t);
                }
            }
        });
        exporterMap.clear();
    }

    /**
     * 创建服务引用的Invoker对象，委托给子类实现具体的协议绑定逻辑。
     * <p>
     * 该方法是模板方法模式的体现，定义了refer的标准流程，具体的协议绑定细节
     * 由子类的protocolBindingRefer()方法实现。
     * </p>
     *
     * @param type 服务接口类型
     * @param url 服务URL，包含注册地址、协议、参数等信息
     * @return 创建的Invoker对象，用于发起远程调用
     * @throws RpcException 当创建Invoker失败时抛出
     */
    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        return protocolBindingRefer(type, url);
    }

    /**
     * 子类实现的协议绑定引用方法，定义具体的服务引用创建逻辑。
     *
     * @param type 服务接口类型
     * @param url 服务URL
     * @return 创建的Invoker对象
     * @throws RpcException 当创建Invoker失败时抛出
     * @deprecated 建议使用refer()方法，该方法为内部实现方法
     */
    @Deprecated
    protected abstract <T> Invoker<T> protocolBindingRefer(Class<T> type, URL url) throws RpcException;

    public Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }

    protected void optimizeSerialization(URL url) throws RpcException {
        String className = url.getParameter(OPTIMIZER_KEY, "");
        if (StringUtils.isEmpty(className) || optimizers.contains(className)) {
            return;
        }

        logger.info("Optimizing the serialization process for Kryo, FST, etc...");

        try {
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            if (!SerializationOptimizer.class.isAssignableFrom(clazz)) {
                throw new RpcException("The serialization optimizer " + className + " isn't an instance of "
                        + SerializationOptimizer.class.getName());
            }

            SerializationOptimizer optimizer = (SerializationOptimizer) clazz.newInstance();

            if (optimizer.getSerializableClasses() == null) {
                return;
            }

            for (Class c : optimizer.getSerializableClasses()) {
                SerializableClassRegistry.registerClass(c);
            }

            optimizers.add(className);

        } catch (ClassNotFoundException e) {
            throw new RpcException("Cannot find the serialization optimizer class: " + className, e);

        } catch (InstantiationException | IllegalAccessException e) {
            throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);
        }
    }

    /**
     * 根据URL获取服务器绑定地址字符串。
     * <p>
     * 该方法解析URL中的绑定IP和端口信息，如果配置了anyhost（绑定到0.0.0.0），
     * 则将其转换为任意主机标识。最终返回格式为"ip:port"的地址字符串。
     * </p>
     *
     * @param url 包含绑定配置的URL对象
     * @return 格式为"ip:port"的地址字符串，例如"192.168.1.100:20880"
     */
    protected String getAddr(URL url) {
        String bindIp = url.getParameter(org.apache.dubbo.remoting.Constants.BIND_IP_KEY, url.getHost());
        if (url.getParameter(ANYHOST_KEY, false)) {
            bindIp = ANYHOST_VALUE;
        }
        return NetUtils.getIpByHost(bindIp) + ":"
                + url.getParameter(org.apache.dubbo.remoting.Constants.BIND_PORT_KEY, url.getPort());
    }
}
