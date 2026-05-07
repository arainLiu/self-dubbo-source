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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

/**
 * RPC Invocation.
 *
 * @serial Don't change the class name and properties.
 */
/**
 * RPC调用上下文对象，封装了一次RPC调用的所有信息
 * 包含方法名、参数类型、参数值、附件信息等
 * 实现了Invocation接口，支持序列化传输
 */
public class RpcInvocation implements Invocation, Serializable {

    private static final long serialVersionUID = -4355285085441097045L;

    /** 目标服务的唯一名称 */
    private String targetServiceUniqueName;

    /** 协议服务键，用于标识服务 */
    private String protocolServiceKey;

    /** 服务模型，包含服务的元数据信息 */
    private ServiceModel serviceModel;

    /** 调用的方法名 */
    private String methodName;

    /** 接口名称 */
    private String interfaceName;

    /** 参数类型数组，使用transient避免序列化 */
    private transient Class<?>[] parameterTypes;

    /** 参数类型描述字符串 */
    private String parameterTypesDesc;

    /** 兼容的参数签名数组 */
    private String[] compatibleParamSignatures;

    /** 方法调用的参数值数组 */
    private Object[] arguments;

    /**
     * 附件信息，会在RPC调用时传递到远程服务器
     * 用于传递额外的配置和元数据
     */
    private Map<String, Object> attachments;

    /** 附件操作的锁，保证线程安全 */
    private final transient Lock attachmentLock = new ReentrantLock();

    /**
     * 自定义属性集合，仅在调用方使用，不会在网络上传输
     * 用于在调用链中传递本地上下文信息
     */
    private transient Map<Object, Object> attributes = Collections.synchronizedMap(new HashMap<>());

    /** 调用器引用，用于执行实际的RPC调用 */
    private transient Invoker<?> invoker;

    /** 返回值类型 */
    private transient Class<?> returnType;

    /** 返回值的泛型类型数组 */
    private transient Type[] returnTypes;

    /** 调用模式，如同步、异步等 */
    private transient InvokeMode invokeMode;

    /** 已调用的Invoker列表，用于记录调用链 */
    private transient List<Invoker<?>> invokedInvokers = new LinkedList<>();

    /**
     * 默认构造函数
     * @deprecated 仅用于测试场景
     */
    @Deprecated
    public RpcInvocation() {}

    /**
     * 基于现有Invocation创建深拷贝
     *
     * @param invocation 原始的Invocation对象
     */
    public RpcInvocation(Invocation invocation) {
        this(invocation, null);
    }

    /**
     * 基于现有Invocation创建深拷贝，并从Invoker中添加服务参数到attachment
     * 注意：不会改变原始Invocation中的invoker引用
     *
     * @param invocation 原始的Invocation对象
     * @param invoker    目标Invoker，从中提取URL参数并设置到attachment
     */
    public RpcInvocation(Invocation invocation, Invoker<?> invoker) {
        // 调用主构造函数，复制Invocation的基本信息
        this(
                invocation.getTargetServiceUniqueName(),
                invocation.getServiceModel(),
                invocation.getMethodName(),
                invocation.getServiceName(),
                invocation.getProtocolServiceKey(),
                invocation.getParameterTypes(),
                invocation.getArguments(),
                invocation.copyObjectAttachments(),
                invocation.getInvoker(),
                invocation.getAttributes(),
                invocation instanceof RpcInvocation ? ((RpcInvocation) invocation).getInvokeMode() : null);

        // 如果提供了invoker，从URL中提取关键参数并设置到attachment中
        if (invoker != null) {
            URL url = invoker.getUrl();
            // 设置服务路径
            setAttachment(PATH_KEY, url.getPath());
            // 设置接口名称
            if (url.hasParameter(INTERFACE_KEY)) {
                setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
            }
            // 设置服务分组
            if (url.hasParameter(GROUP_KEY)) {
                setAttachment(GROUP_KEY, url.getGroup());
            }
            // 设置服务版本
            if (url.hasParameter(VERSION_KEY)) {
                setAttachment(VERSION_KEY, url.getVersion("0.0.0"));
            }
            // 设置超时时间
            if (url.hasParameter(TIMEOUT_KEY)) {
                setAttachment(TIMEOUT_KEY, url.getParameter(TIMEOUT_KEY));
            }
            // 设置令牌
            if (url.hasParameter(TOKEN_KEY)) {
                setAttachment(TOKEN_KEY, url.getParameter(TOKEN_KEY));
            }
            // 设置应用名称
            if (url.hasParameter(APPLICATION_KEY)) {
                setAttachment(APPLICATION_KEY, url.getApplication());
            }
        }
    }

    /**
     * 创建全新的Invocation对象
     */
    public RpcInvocation(
            ServiceModel serviceModel,
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments) {
        this(
                null,
                serviceModel,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                null,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            ServiceModel serviceModel,
            Method method,
            String interfaceName,
            String protocolServiceKey,
            Object[] arguments) {
        this(
                null,
                serviceModel,
                method.getName(),
                interfaceName,
                protocolServiceKey,
                method.getParameterTypes(),
                arguments,
                null,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(Method method, String interfaceName, String protocolServiceKey, Object[] arguments) {
        this(
                null,
                null,
                method.getName(),
                interfaceName,
                protocolServiceKey,
                method.getParameterTypes(),
                arguments,
                null,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            ServiceModel serviceModel,
            Method method,
            String interfaceName,
            String protocolServiceKey,
            Object[] arguments,
            Map<String, Object> attachment,
            Map<Object, Object> attributes) {
        this(
                null,
                serviceModel,
                method.getName(),
                interfaceName,
                protocolServiceKey,
                method.getParameterTypes(),
                arguments,
                attachment,
                null,
                attributes,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            Method method,
            String interfaceName,
            String protocolServiceKey,
            Object[] arguments,
            Map<String, Object> attachment,
            Map<Object, Object> attributes) {
        this(
                null,
                null,
                method.getName(),
                interfaceName,
                protocolServiceKey,
                method.getParameterTypes(),
                arguments,
                attachment,
                null,
                attributes,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments) {
        this(
                null,
                null,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                null,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            ServiceModel serviceModel,
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments) {
        this(
                null,
                serviceModel,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                attachments,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments) {
        this(
                null,
                null,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                attachments,
                null,
                null,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments,
            Invoker<?> invoker,
            Map<Object, Object> attributes) {
        this(
                null,
                null,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                attachments,
                invoker,
                attributes,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            ServiceModel serviceModel,
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments,
            Invoker<?> invoker,
            Map<Object, Object> attributes) {
        this(
                null,
                serviceModel,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                attachments,
                invoker,
                attributes,
                null);
    }

    /**
     * @deprecated 已废弃，将在3.1.x版本中移除
     */
    @Deprecated
    public RpcInvocation(
            ServiceModel serviceModel,
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments,
            Invoker<?> invoker,
            Map<Object, Object> attributes,
            InvokeMode invokeMode) {
        this(
                null,
                serviceModel,
                methodName,
                interfaceName,
                protocolServiceKey,
                parameterTypes,
                arguments,
                attachments,
                invoker,
                attributes,
                invokeMode);
    }

    /**
     * 主构造函数，创建全新的Invocation对象
     *
     * @param targetServiceUniqueName 目标服务的唯一名称
     * @param serviceModel 服务模型，包含服务元数据
     * @param methodName 调用的方法名
     * @param interfaceName 接口名称
     * @param protocolServiceKey 协议服务键
     * @param parameterTypes 参数类型数组
     * @param arguments 参数值数组
     * @param attachments 附件信息，会传递到远程服务器
     * @param invoker 调用器引用
     * @param attributes 本地属性集合，不通过网络传输
     * @param invokeMode 调用模式（同步/异步等）
     */
    public RpcInvocation(
            String targetServiceUniqueName,
            ServiceModel serviceModel,
            String methodName,
            String interfaceName,
            String protocolServiceKey,
            Class<?>[] parameterTypes,
            Object[] arguments,
            Map<String, Object> attachments,
            Invoker<?> invoker,
            Map<Object, Object> attributes,
            InvokeMode invokeMode) {
        // 初始化基本属性
        this.targetServiceUniqueName = targetServiceUniqueName;
        this.serviceModel = serviceModel;
        this.methodName = methodName;
        this.interfaceName = interfaceName;
        this.protocolServiceKey = protocolServiceKey;
        // 初始化参数类型数组，避免null值
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
        // 初始化参数值数组，避免null值
        this.arguments = arguments == null ? new Object[0] : arguments;
        // 初始化附件Map，避免null值
        this.attachments = attachments == null ? new HashMap<>() : attachments;
        // 初始化属性Map，使用同步Map保证线程安全
        this.attributes = attributes == null ? Collections.synchronizedMap(new HashMap<>()) : attributes;
        // 设置调用器
        this.invoker = invoker;
        // 初始化参数描述信息
        initParameterDesc();
        // 设置调用模式
        this.invokeMode = invokeMode;
    }

    /**
     * 初始化参数描述信息
     * 尝试从服务模型或框架模型中获取方法描述符，如果获取失败则通过反射生成
     */
    private void initParameterDesc() {
        // 创建原子引用存储服务描述符
        AtomicReference<ServiceDescriptor> serviceDescriptor = new AtomicReference<>();

        // 优先从serviceModel中获取服务描述符
        if (serviceModel != null) {
            serviceDescriptor.set(serviceModel.getServiceModel());
        } else if (StringUtils.isNotEmpty(interfaceName)) {
            // 如果interfaceName不为空，从框架模型的ProviderModel中查找匹配的服务描述符
            // TODO: 多实例兼容模式处理
            FrameworkModel.defaultModel().getServiceRepository().allProviderModels().stream()
                    .map(ProviderModel::getServiceModel)
                    .filter(s -> interfaceName.equals(s.getInterfaceName()))
                    .findFirst()
                    .ifPresent(serviceDescriptor::set);
        }

        // 如果找到了服务描述符，从中提取方法描述信息
        if (serviceDescriptor.get() != null) {
            MethodDescriptor methodDescriptor = serviceDescriptor.get().getMethod(methodName, parameterTypes);
            if (methodDescriptor != null) {
                // 设置参数描述字符串
                this.parameterTypesDesc = methodDescriptor.getParamDesc();
                // 设置兼容的参数签名
                this.compatibleParamSignatures = methodDescriptor.getCompatibleParamSignatures();
                // 设置返回类型信息
                this.returnTypes = methodDescriptor.getReturnTypes();
                this.returnType = methodDescriptor.getReturnClass();
            }
        }

        // 如果仍然没有获取到参数描述，使用反射方式生成
        if (parameterTypesDesc == null) {
            // 通过ReflectUtils生成参数类型描述
            this.parameterTypesDesc = ReflectUtils.getDesc(this.getParameterTypes());
            // 生成兼容参数签名数组，使用类的全限定名
            this.compatibleParamSignatures =
                    Stream.of(this.parameterTypes).map(Class::getName).toArray(String[]::new);
            // 通过RpcUtils获取返回类型
            this.returnTypes = RpcUtils.getReturnTypes(this);
            this.returnType = RpcUtils.getReturnType(this);
        }
    }

    /**
     * 获取调用器
     * @return Invoker对象
     */
    @Override
    public Invoker<?> getInvoker() {
        return invoker;
    }

    /**
     * 设置调用器
     * @param invoker 要设置的Invoker对象
     */
    public void setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
    }

    /**
     * 从attributes中移除指定key的属性
     * @param key 属性键
     * @return 被移除的属性值
     */
    public Object remove(Object key) {
        return attributes.remove(key);
    }

    /**
     * 向attributes中添加属性
     * @param key 属性键
     * @param value 属性值
     * @return 之前的属性值，如果不存在则返回null
     */
    @Override
    public Object put(Object key, Object value) {
        return attributes.put(key, value);
    }

    /**
     * 从attributes中获取指定key的属性值
     * @param key 属性键
     * @return 属性值，如果不存在则返回null
     */
    @Override
    public Object get(Object key) {
        return attributes.get(key);
    }

    /**
     * 获取所有attributes属性
     * @return 属性Map
     */
    @Override
    public Map<Object, Object> getAttributes() {
        return attributes;
    }

    /**
     * 添加已调用的Invoker到列表中
     * 用于记录调用链，便于追踪和调试
     * @param invoker 已调用的Invoker
     */
    @Override
    public void addInvokedInvoker(Invoker<?> invoker) {
        this.invokedInvokers.add(invoker);
    }

    /**
     * 获取所有已调用的Invoker列表
     * @return Invoker列表
     */
    @Override
    public List<Invoker<?>> getInvokedInvokers() {
        return this.invokedInvokers;
    }

    /**
     * 获取目标服务的唯一名称
     * @return 服务唯一名称
     */
    @Override
    public String getTargetServiceUniqueName() {
        return targetServiceUniqueName;
    }

    /**
     * 设置目标服务的唯一名称
     * @param targetServiceUniqueName 服务唯一名称
     */
    public void setTargetServiceUniqueName(String targetServiceUniqueName) {
        this.targetServiceUniqueName = targetServiceUniqueName;
    }

    /**
     * 获取协议服务键
     * @return 协议服务键
     */
    @Override
    public String getProtocolServiceKey() {
        return protocolServiceKey;
    }

    /**
     * 获取调用的方法名
     * @return 方法名
     */
    @Override
    public String getMethodName() {
        return methodName;
    }

    /**
     * 设置调用的方法名
     * @param methodName 方法名
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * 获取服务名称（即接口名称）
     * @return 服务名称
     */
    @Override
    public String getServiceName() {
        return interfaceName;
    }

    /**
     * 设置服务名称
     * @param interfaceName 接口名称
     */
    public void setServiceName(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    /**
     * 获取参数类型数组
     * @return 参数类型数组
     */
    @Override
    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    /**
     * 设置参数类型数组
     * @param parameterTypes 参数类型数组，如果为null则初始化为空数组
     */
    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
    }

    /**
     * 获取参数类型描述字符串
     * @return 参数类型描述
     */
    public String getParameterTypesDesc() {
        return parameterTypesDesc;
    }

    /**
     * 设置参数类型描述字符串
     * @param parameterTypesDesc 参数类型描述
     */
    public void setParameterTypesDesc(String parameterTypesDesc) {
        this.parameterTypesDesc = parameterTypesDesc;
    }

    /**
     * 获取兼容的参数签名数组
     * 用于方法匹配和兼容性检查
     * @return 参数签名数组
     */
    @Override
    public String[] getCompatibleParamSignatures() {
        return compatibleParamSignatures;
    }

    /**
     * 设置兼容的参数签名数组
     * 当服务端找不到服务类型且不是泛化调用时，可以手动设置参数签名
     * @param compatibleParamSignatures 参数签名数组
     */
    public void setCompatibleParamSignatures(String[] compatibleParamSignatures) {
        this.compatibleParamSignatures = compatibleParamSignatures;
    }

    /**
     * 获取方法调用的参数值数组
     * @return 参数值数组
     */
    @Override
    public Object[] getArguments() {
        return arguments;
    }

    /**
     * 设置方法调用的参数值数组
     * @param arguments 参数值数组，如果为null则初始化为空数组
     */
    public void setArguments(Object[] arguments) {
        this.arguments = arguments == null ? new Object[0] : arguments;
    }

    /**
     * 获取附件Map的引用（非线程安全的访问）
     * 如果attachments为null，会创建新的HashMap
     * @return 附件Map
     */
    @Override
    public Map<String, Object> getObjectAttachments() {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                attachments = new HashMap<>();
            }
            return attachments;
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 复制附件Map，返回一个新的HashMap副本
     * 用于安全地获取附件信息而不影响原始数据
     * @return 附件Map的副本
     */
    @Override
    public Map<String, Object> copyObjectAttachments() {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return new HashMap<>();
            }
            // 返回副本而非原始引用
            return new HashMap<>(attachments);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 遍历所有附件条目
     * 使用Consumer函数式接口处理每个附件条目
     * @param consumer 处理附件条目的消费者函数
     */
    @Override
    public void foreachAttachment(Consumer<Map.Entry<String, Object>> consumer) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments != null) {
                // 遍历附件条目并应用consumer
                attachments.entrySet().forEach(consumer);
            }
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 设置附件Map
     * 替换现有的所有附件
     * @param attachments 新的附件Map，如果为null则初始化为空Map
     */
    public void setObjectAttachments(Map<String, Object> attachments) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            this.attachments = attachments == null ? new HashMap<>() : attachments;
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 设置附件（String类型的值）
     * @param key 附件键
     * @param value 附件值
     */
    @Override
    public void setAttachment(String key, String value) {
        setObjectAttachment(key, value);
    }

    /**
     * 获取所有附件（转换为String类型）
     * @deprecated 已废弃，建议使用getObjectAttachments
     * @return String类型的附件Map
     */
    @Deprecated
    @Override
    public Map<String, String> getAttachments() {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                attachments = new HashMap<>();
            }
            // 转换为String类型的Map适配器
            return new AttachmentsAdapter.ObjectToStringMap(attachments);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 设置附件Map（String类型）
     * @deprecated 已废弃，建议使用setObjectAttachments
     * @param attachments String类型的附件Map
     */
    @Deprecated
    public void setAttachments(Map<String, String> attachments) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            // 创建新的HashMap避免直接引用
            this.attachments = attachments == null ? new HashMap<>() : new HashMap<>(attachments);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 设置附件（Object类型的值）
     * @param key 附件键
     * @param value 附件值
     */
    @Override
    public void setAttachment(String key, Object value) {
        setObjectAttachment(key, value);
    }

    /**
     * 设置对象类型的附件
     * @param key 附件键
     * @param value 附件值（可以是任意Object类型）
     */
    @Override
    public void setObjectAttachment(String key, Object value) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                attachments = new HashMap<>();
            }
            // 放入键值对
            attachments.put(key, value);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 如果附件中不存在该key，则设置附件（String类型值）
     * @param key 附件键
     * @param value 附件值
     */
    @Override
    public void setAttachmentIfAbsent(String key, String value) {
        setObjectAttachmentIfAbsent(key, value);
    }

    /**
     * 如果附件中不存在该key，则设置附件（Object类型值）
     * @param key 附件键
     * @param value 附件值
     */
    @Override
    public void setAttachmentIfAbsent(String key, Object value) {
        setObjectAttachmentIfAbsent(key, value);
    }

    /**
     * 如果附件中不存在该key，则设置对象类型的附件
     * 用于设置默认值，避免覆盖已有的值
     * @param key 附件键
     * @param value 附件值
     */
    @Override
    public void setObjectAttachmentIfAbsent(String key, Object value) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                attachments = new HashMap<>();
            }
            // 只有当key不存在时才设置
            if (!attachments.containsKey(key)) {
                attachments.put(key, value);
            }
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 批量添加附件（String类型）
     * @deprecated 已废弃，建议使用addObjectAttachments
     * @param attachments 要添加的附件Map
     */
    @Deprecated
    public void addAttachments(Map<String, String> attachments) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return;
            }
            if (this.attachments == null) {
                this.attachments = new HashMap<>();
            }
            // 将所有附件添加到现有Map中
            this.attachments.putAll(attachments);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 批量添加对象类型的附件
     * 将传入的Map中的所有键值对添加到attachments中
     * @param attachments 要添加的附件Map
     */
    public void addObjectAttachments(Map<String, Object> attachments) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return;
            }
            if (this.attachments == null) {
                this.attachments = new HashMap<>();
            }
            // 将所有附件添加到现有Map中
            this.attachments.putAll(attachments);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 批量添加附件，仅添加当前attachments中不存在的key（String类型）
     * @deprecated 已废弃，建议使用addObjectAttachmentsIfAbsent
     * @param attachments 要添加的附件Map
     */
    @Deprecated
    public void addAttachmentsIfAbsent(Map<String, String> attachments) {
        if (attachments == null) {
            return;
        }
        // 遍历所有条目，只在key不存在时添加
        for (Map.Entry<String, String> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 批量添加对象类型的附件，仅添加当前attachments中不存在的key
     * 用于设置默认值，不会覆盖已有的附件
     * @param attachments 要添加的附件Map
     */
    public void addObjectAttachmentsIfAbsent(Map<String, Object> attachments) {
        if (attachments == null) {
            return;
        }
        // 遍历所有条目，只在key不存在时添加
        for (Map.Entry<String, Object> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 获取附件值（String类型）
     * @deprecated 已废弃，建议使用getObjectAttachment
     * @param key 附件键
     * @return 附件值，如果不是String类型或不存在则返回null
     */
    @Override
    @Deprecated
    public String getAttachment(String key) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return null;
            }
            Object value = attachments.get(key);
            // 只有String类型才返回
            if (value instanceof String) {
                return (String) value;
            }
            return null;
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 获取对象类型的附件值
     * 先尝试用原始key查找，如果找不到则用小写key查找
     * @param key 附件键
     * @return 附件值，如果不存在则返回null
     */
    @Override
    public Object getObjectAttachment(String key) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return null;
            }
            // 先用原始key查找
            final Object val = attachments.get(key);
            if (val != null) {
                return val;
            }
            // 如果找不到，尝试用小写key查找（兼容性处理）
            return attachments.get(key.toLowerCase(Locale.ROOT));
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 获取附件值（String类型），支持默认值
     * @deprecated 已废弃，建议使用getObjectAttachment
     * @param key 附件键
     * @param defaultValue 默认值，当key不存在或值为空时返回
     * @return 附件值，如果为空或不存在则返回默认值
     */
    @Override
    @Deprecated
    public String getAttachment(String key, String defaultValue) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return defaultValue;
            }
            Object value = attachments.get(key);
            if (value instanceof String) {
                String strValue = (String) value;
                // 如果值为空，返回默认值
                if (StringUtils.isEmpty(strValue)) {
                    return defaultValue;
                } else {
                    return strValue;
                }
            }
            // 如果类型不匹配，返回默认值
            return defaultValue;
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 获取对象类型的附件值，支持默认值
     * @deprecated 已废弃
     * @param key 附件键
     * @param defaultValue 默认值
     * @return 附件值，如果不存在则返回默认值
     */
    @Deprecated
    @Override
    public Object getObjectAttachment(String key, Object defaultValue) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return defaultValue;
            }
            Object value = attachments.get(key);
            // 如果值为null，返回默认值
            if (value == null) {
                return defaultValue;
            }
            return value;
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 获取附件值，不进行类型转换
     * 直接返回存储在Map中的原始对象
     * @param key 附件键
     * @return 原始附件值，如果不存在则返回null
     */
    @Override
    public Object getObjectAttachmentWithoutConvert(String key) {
        try {
            // 加锁保证线程安全
            attachmentLock.lock();
            if (attachments == null) {
                return null;
            }
            // 直接返回原始值，不做任何转换
            return attachments.get(key);
        } finally {
            // 释放锁
            attachmentLock.unlock();
        }
    }

    /**
     * 获取返回值类型
     * @return 返回值类型
     */
    public Class<?> getReturnType() {
        return returnType;
    }

    /**
     * 设置返回值类型
     * @param returnType 返回值类型
     */
    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    /**
     * 获取返回值的泛型类型数组
     * @return 返回类型数组
     */
    public Type[] getReturnTypes() {
        return returnTypes;
    }

    /**
     * 设置返回值的泛型类型数组
     * @param returnTypes 返回类型数组
     */
    public void setReturnTypes(Type[] returnTypes) {
        this.returnTypes = returnTypes;
    }

    /**
     * 获取调用模式
     * @return 调用模式（同步、异步等）
     */
    public InvokeMode getInvokeMode() {
        return invokeMode;
    }

    /**
     * 设置调用模式
     * @param invokeMode 调用模式
     */
    public void setInvokeMode(InvokeMode invokeMode) {
        this.invokeMode = invokeMode;
    }

    /**
     * 设置服务模型
     * @param serviceModel 服务模型
     */
    @Override
    public void setServiceModel(ServiceModel serviceModel) {
        this.serviceModel = serviceModel;
    }

    /**
     * 获取服务模型
     * @return 服务模型
     */
    @Override
    public ServiceModel getServiceModel() {
        return serviceModel;
    }

    /**
     * 生成Invocation的字符串表示
     * 包含方法名和参数类型信息，便于调试
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "RpcInvocation [methodName=" + methodName + ", parameterTypes=" + Arrays.toString(parameterTypes) + "]";
    }
}

