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
package org.apache.dubbo.rpc.cluster.support.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.url.component.DubboServiceAddressURL;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.listener.ExporterChangeListener;
import org.apache.dubbo.rpc.listener.InjvmExporterListener;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.BROADCAST_CLUSTER;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.Constants.SCOPE_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_LOCAL;
import static org.apache.dubbo.rpc.Constants.SCOPE_REMOTE;
import static org.apache.dubbo.rpc.cluster.Constants.PEER_KEY;

/**
 * 基于调用范围（Scope）的集群调用器包装类
 * 负责在本地调用（Injvm）和远程调用之间进行动态路由，并监听服务的导出状态以实现自动切换
 * 支持点对点直连、广播调用以及强制本地/远程调用等多种场景
 *
 * @param <T> 服务接口类型
 */
public class ScopeClusterInvoker<T> implements ClusterInvoker<T>, ExporterChangeListener {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ScopeClusterInvoker.class);

    /** 用于创建InjvmInvoker时的同步锁，保证线程安全 */
    private final Object createLock = new Object();

    /** Protocol SPI扩展，用于执行服务引用操作 */
    private Protocol protocolSPI;

    /** 服务目录，包含所有可用的服务提供者信息 */
    private final Directory<T> directory;

    /** 原始的远程集群调用器 */
    private final Invoker<T> invoker;

    /** 标识服务是否已在本地JVM中导出 */
    private final AtomicBoolean isExported;

    /** 本地JVM调用器，用于同一进程内的高效通信 */
    private volatile Invoker<T> injvmInvoker;

    /** InjvmExporter监听器，用于感知服务导出状态的变更 */
    private volatile InjvmExporterListener injvmExporterListener;

    /** 点对点直连标志 */
    private boolean peerFlag;

    /** 允许本地调用标志 */
    private boolean injvmFlag;

    public ScopeClusterInvoker(Directory<T> directory, Invoker<T> invoker) {
        this.directory = directory;
        this.invoker = invoker;
        this.isExported = new AtomicBoolean(false);
        init();
    }

    @Override
    public URL getUrl() {
        return directory.getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return directory.getUrl();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public boolean isDestroyed() {
        return directory.isDestroyed();
    }

    /**
     * 检查当前调用器是否处于可用状态
     * 根据调用范围（点对点、广播、本地或远程）选择对应的底层调用器进行可用性校验
     *
     * @return 如果服务可用返回true，否则返回false
     */
    @Override
    public boolean isAvailable() {
        // 对于点对点直连或广播调用，直接检查远程调用器的连通性
        if (peerFlag || isBroadcast()) {
            // If it's a point-to-point direct connection or broadcasting, it should be called remotely.
            return invoker.isAvailable();
        }

        // 如果配置了强制本地调用，则仅检查服务是否已在本地JVM导出
        if (injvmFlag && isForceLocal()) {
            // If it's a local call, it should be called locally.
            return isExported.get();
        }

        // 如果允许本地调用且服务已导出，则认为当前调用器可用（优先走本地优化路径）
        if (injvmFlag && isExported.get()) {
            // If allow local call, check if local exported first
            return true;
        }

        // 默认情况下，委托给远程调用器检查其与服务提供者的连接状态
        return invoker.isAvailable();
    }


    @Override
    public void destroy() {
        if (injvmExporterListener != null) {
            injvmExporterListener.removeExporterChangeListener(this, getUrl().getServiceKey());
        }
        destroyInjvmInvoker();
        this.invoker.destroy();
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    /**
     * 根据调用范围（Scope）选择合适的Invoker执行RPC调用
     * 优先判断是否为广播或点对点调用，其次检查是否存在本地JVM导出服务，最后回退到远程调用
     *
     * @param invocation 包含方法名、参数及附件信息的调用上下文对象
     * @return RPC调用的执行结果
     * @throws RpcException 当调用过程中发生网络异常或服务不可用时抛出
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        // 处理广播调用场景：广播必须走远程调用链路，不能使用本地injvm优化
        if (isBroadcast()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Performing broadcast call for method: " + RpcUtils.getMethodName(invocation)
                        + " of service: " + getUrl().getServiceKey());
            }
            return invoker.invoke(invocation);
        }

        // 处理点对点直连场景：直接调用原始的远程Invoker
        if (peerFlag) {
            if (logger.isDebugEnabled()) {
                logger.debug("Performing point-to-point call for method: " + RpcUtils.getMethodName(invocation)
                        + " of service: " + getUrl().getServiceKey());
            }
            // If it's a point-to-point direct connection, invoke the original Invoker
            return invoker.invoke(invocation);
        }

        // 处理本地JVM调用场景：如果服务已在本地导出，则优先使用injvmInvoker以提升性能并减少网络开销
        if (isInjvmExported()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Performing local JVM call for method: " + RpcUtils.getMethodName(invocation)
                        + " of service: " + getUrl().getServiceKey());
            }
            // If it's exported to the local JVM, invoke the corresponding Invoker
            return injvmInvoker.invoke(invocation);
        }

        // 默认远程调用场景：当以上条件均不满足时，委托给原始的远程Invoker执行调用
        if (logger.isDebugEnabled()) {
            logger.debug("Performing remote call for method: " + RpcUtils.getMethodName(invocation) + " of service: "
                    + getUrl().getServiceKey());
        }
        // Otherwise, delegate the invocation to the original Invoker
        return invoker.invoke(invocation);
    }


    private boolean isBroadcast() {
        return BROADCAST_CLUSTER.equalsIgnoreCase(getUrl().getParameter(CLUSTER_KEY));
    }

    /**
     * 当监听到服务导出事件时触发，用于动态创建本地InjvmInvoker
     *
     * @param exporter 导出的服务Exporter对象
     */
    @Override
    public void onExporterChangeExport(Exporter<?> exporter) {
        if (isExported.get()) {
            return;
        }
        // 校验服务键和协议，确保是当前关注的本地服务导出
        if (getUrl().getServiceKey().equals(exporter.getInvoker().getUrl().getServiceKey())
                && exporter.getInvoker().getUrl().getProtocol().equalsIgnoreCase(LOCAL_PROTOCOL)) {
            createInjvmInvoker(exporter);
            isExported.compareAndSet(false, true);
        }
    }

    /**
     * 当监听到服务取消导出事件时触发，用于销毁本地InjvmInvoker
     *
     * @param exporter 取消导出的服务Exporter对象
     */
    @Override
    public void onExporterChangeUnExport(Exporter<?> exporter) {
        if (getUrl().getServiceKey().equals(exporter.getInvoker().getUrl().getServiceKey())
                && exporter.getInvoker().getUrl().getProtocol().equalsIgnoreCase(LOCAL_PROTOCOL)) {
            destroyInjvmInvoker();
            isExported.compareAndSet(true, false);
        }
    }

    public Invoker<?> getInvoker() {
        return invoker;
    }

    /**
     * 初始化ScopeClusterInvoker实例，确定调用范围和监听服务导出状态
     * 根据URL配置判断是否为点对点调用、本地 injvm 调用或远程调用，并注册Exporter变更监听器
     */
    private void init() {
        // 从URL属性中获取peer标识，用于判断是否为点对点直连模式
        Boolean peer = (Boolean) getUrl().getAttribute(PEER_KEY);
        String isInjvm = getUrl().getParameter(LOCAL_PROTOCOL);

        // 当配置为点对点直连时，直接结束初始化流程
        if (peer != null && peer) {
            peerFlag = true;
            return;
        }

        // 检查服务是否已通过Injvm协议导出，如果是则直接使用当前invoker作为injvmInvoker
        if (injvmInvoker == null
                && LOCAL_PROTOCOL.equalsIgnoreCase(getRegistryUrl().getProtocol())) {
            injvmInvoker = invoker;
            isExported.compareAndSet(false, true);
            injvmFlag = true;
            return;
        }

        // 根据配置参数或服务发现范围判断是否启用本地injvm调用
        if (Boolean.TRUE.toString().equalsIgnoreCase(isInjvm)
                || SCOPE_LOCAL.equalsIgnoreCase(getUrl().getParameter(SCOPE_KEY))) {
            // 显式配置了injvm=true或scope=local时，启用本地调用标志
            injvmFlag = true;
        } else if (isInjvm == null) {
            // 未显式配置时，根据是否为远程调用或泛化调用来推断是否使用本地调用
            injvmFlag = isNotRemoteOrGeneric();
        }

        // 获取Protocol的SPI自适应扩展实例，用于后续的服务引用和导出操作
        protocolSPI = getUrl().getApplicationModel()
                .getExtensionLoader(Protocol.class)
                .getAdaptiveExtension();

        // 获取InjvmExporter监听器并注册当前实例，以便在服务导出状态变更时收到通知
        injvmExporterListener =
                getUrl().getOrDefaultFrameworkModel().getBeanFactory().getBean(InjvmExporterListener.class);
        injvmExporterListener.addExporterChangeListener(this, getUrl().getServiceKey());
    }


    /**
     * Check if the service is a generalized call or the SCOPE_REMOTE parameter is set
     *
     * @return boolean
     */
    private boolean isNotRemoteOrGeneric() {
        return !SCOPE_REMOTE.equalsIgnoreCase(getUrl().getParameter(SCOPE_KEY))
                && !getUrl().getParameter(GENERIC_KEY, false);
    }

    /**
     * Checks whether the current ScopeClusterInvoker is exported to the local JVM and returns a boolean value.
     *
     * @return true if the ScopeClusterInvoker is exported to the local JVM, false otherwise
     * @throws RpcException if there was an error during the invocation
     */
    private boolean isInjvmExported() {
        Boolean localInvoke = RpcContext.getServiceContext().getLocalInvoke();
        boolean isExportedValue = isExported.get();
        boolean localOnce = (localInvoke != null && localInvoke);

        // Determine whether this call is local
        // 如果服务已导出且当前上下文要求本地调用，则确认可用
        if (isExportedValue && localOnce) {
            return true;
        }

        // Determine whether this call is remote
        // 如果当前上下文明确要求远程调用，则忽略本地导出状态
        if (localInvoke != null && !localInvoke) {
            return false;
        }

        // When calling locally, determine whether it does not meet the requirements
        // 如果期望本地调用但服务尚未导出，则抛出异常提示
        if (!isExportedValue && (isForceLocal() || localOnce)) {
            // If it's supposed to be exported to the local JVM ,but it's not, throw an exception
            throw new RpcException(
                    "Local service for " + getUrl().getServiceInterface() + " has not been exposed yet!");
        }

        return isExportedValue && injvmFlag;
    }

    private boolean isForceLocal() {
        return SCOPE_LOCAL.equalsIgnoreCase(getUrl().getParameter(SCOPE_KEY))
                || Boolean.TRUE.toString().equalsIgnoreCase(getUrl().getParameter(LOCAL_PROTOCOL));
    }

    /**
     * Creates a new Invoker for the current ScopeClusterInvoker and exports it to the local JVM.
     */
    private void createInjvmInvoker(Exporter<?> exporter) {
        if (injvmInvoker == null) {
            synchronized (createLock) {
                if (injvmInvoker == null) {
                    // 构造本地调用URL，使用localhost和本地端口
                    URL url = new ServiceConfigURL(
                            LOCAL_PROTOCOL,
                            NetUtils.getLocalHost(),
                            getUrl().getPort(),
                            getInterface().getName(),
                            getUrl().getParameters());
                    url = url.setScopeModel(getUrl().getScopeModel());
                    url = url.setServiceModel(getUrl().getServiceModel());

                    // 构建消费者URL，关联提供者的地址信息
                    DubboServiceAddressURL consumerUrl = new DubboServiceAddressURL(
                            url.getUrlAddress(),
                            url.getUrlParam(),
                            exporter.getInvoker().getUrl(),
                            null);

                    // 引用本地服务并包装为集群Invoker
                    Invoker<?> invoker = protocolSPI.refer(getInterface(), consumerUrl);
                    List<Invoker<?>> invokers = new ArrayList<>();
                    invokers.add(invoker);
                    injvmInvoker = Cluster.getCluster(url.getScopeModel(), Cluster.DEFAULT, false)
                            .join(new StaticDirectory(url, invokers), true);
                }
            }
        }
    }

    /**
     * Destroy the existing InjvmInvoker.
     */
    private void destroyInjvmInvoker() {
        if (injvmInvoker != null) {
            injvmInvoker.destroy();
            injvmInvoker = null;
        }
    }

    @Override
    public String toString() {
        return "ScopeClusterInvoker{" + "directory="
                + directory + ", isExported="
                + isExported + ", peerFlag="
                + peerFlag + ", injvmFlag="
                + injvmFlag + '}';
    }
}
