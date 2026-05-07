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

import org.apache.dubbo.common.beanutil.JavaBeanAccessor;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.compact.Dubbo2CompactUtils;
import org.apache.dubbo.common.compact.Dubbo2GenericExceptionUtils;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.io.UnsafeByteArrayOutputStream;
import org.apache.dubbo.common.json.GsonUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE;
import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE_ASYNC;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_BEAN;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_NATIVE_JAVA;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_PROTOBUF;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FILTER_VALIDATION_EXCEPTION;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;

/**
 * GenericInvokerFilter.
 */
@Activate(group = CommonConstants.PROVIDER, order = -20000)
public class GenericFilter implements Filter, Filter.Listener, ScopeModelAware {
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(GenericFilter.class);

    private ApplicationModel applicationModel;

    private final Map<ClassLoader, Map<String, Class<?>>> classCache = new ConcurrentHashMap<>();

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    /**
     * 处理泛化调用请求，将泛化参数反序列化为具体的Java对象并执行实际调用
     * 拦截$invoke/$invokeAsync方法的调用，根据指定的泛化序列化类型对参数进行反序列化
     *
     * @param invoker 调用器对象，代表目标服务的代理
     * @param inv 调用上下文对象，包含方法名、泛化参数（方法名、参数类型数组、参数值数组）及附件信息
     * @return RPC调用结果
     * @throws RpcException 当参数校验失败、反序列化异常或找不到目标方法时抛出
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        // 校验是否为合法的泛化调用请求：方法名为$invoke/$invokeAsync，包含3个参数，且目标接口非GenericService
        if ((inv.getMethodName().equals($INVOKE) || inv.getMethodName().equals($INVOKE_ASYNC))
                && inv.getArguments() != null
                && inv.getArguments().length == 3
                && !GenericService.class.isAssignableFrom(invoker.getInterface())) {

            // 提取泛化调用的目标方法名、参数类型签名和参数值
            String name = ((String) inv.getArguments()[0]).trim();
            String[] types = (String[]) inv.getArguments()[1];
            Object[] args = (Object[]) inv.getArguments()[2];
            try {
                // 根据方法签名查找对应的Method对象，并获取其真实参数类型
                Method method = findMethodByMethodSignature(invoker.getInterface(), name, types, inv.getServiceModel());
                Class<?>[] params = method.getParameterTypes();

                // 初始化参数数组，确保不为null
                if (args == null) {
                    args = new Object[params.length];
                }

                // 初始化类型数组，确保不为null
                if (types == null) {
                    types = new String[params.length];
                }

                // 校验参数值与参数类型的数量是否一致
                if (args.length != types.length) {
                    throw new RpcException(
                            "GenericFilter#invoke args.length != types.length, please check your " + "params");
                }

                // 从附件或RpcContext中获取泛化序列化类型标识
                String generic = inv.getAttachment(GENERIC_KEY);

                if (StringUtils.isBlank(generic)) {
                    generic = getGenericValueFromRpcContext();
                }

                // 根据不同的泛化序列化类型，执行相应的参数反序列化逻辑
                if (StringUtils.isEmpty(generic)
                        || ProtocolUtils.isDefaultGenericSerialization(generic)
                        || ProtocolUtils.isGenericReturnRawResult(generic)) {
                    // 使用默认的POJO泛化方式，将Map/List结构转换为真实的Java对象
                    try {
                        args = PojoUtils.realize(args, params, method.getGenericParameterTypes());
                    } catch (Exception e) {
                        logger.error(
                                LoggerCodeConstants.PROTOCOL_ERROR_DESERIALIZE,
                                "",
                                "",
                                "Deserialize generic invocation failed. ServiceKey: "
                                        + inv.getTargetServiceUniqueName(),
                                e);
                        throw new RpcException(e);
                    }
                } else if (ProtocolUtils.isGsonGenericSerialization(generic)) {
                    // 使用Gson JSON方式反序列化参数
                    args = getGsonGenericArgs(args, method.getGenericParameterTypes());
                } else if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                    // 使用Native Java序列化方式，需先检查安全配置开关
                    Configuration configuration = ApplicationModel.ofNullable(applicationModel)
                            .modelEnvironment()
                            .getConfiguration();
                    if (!configuration.getBoolean(CommonConstants.ENABLE_NATIVE_JAVA_GENERIC_SERIALIZE, false)) {
                        String notice = "Trigger the safety barrier! "
                                + "Native Java Serializer is not allowed by default."
                                + "This means currently maybe being attacking by others. "
                                + "If you are sure this is a mistake, "
                                + "please set `"
                                + CommonConstants.ENABLE_NATIVE_JAVA_GENERIC_SERIALIZE + "` enable in configuration! "
                                + "Before doing so, please make sure you have configure JEP290 to prevent serialization attack.";
                        logger.error(CONFIG_FILTER_VALIDATION_EXCEPTION, "", "", notice);
                        throw new RpcException(new IllegalStateException(notice));
                    }

                    // 遍历参数，将字节数组反序列化为Java对象
                    for (int i = 0; i < args.length; i++) {
                        if (byte[].class == args[i].getClass()) {
                            try (UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream((byte[]) args[i])) {
                                args[i] = applicationModel
                                        .getExtensionLoader(Serialization.class)
                                        .getExtension(GENERIC_SERIALIZATION_NATIVE_JAVA)
                                        .deserialize(null, is)
                                        .readObject();
                            } catch (Exception e) {
                                throw new RpcException("Deserialize argument [" + (i + 1) + "] failed.", e);
                            }
                        } else {
                            throw new RpcException("Generic serialization [" + GENERIC_SERIALIZATION_NATIVE_JAVA
                                    + "] only support message type "
                                    + byte[].class
                                    + " and your message type is "
                                    + args[i].getClass());
                        }
                    }
                } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                    // 使用JavaBean方式反序列化参数，要求参数必须为JavaBeanDescriptor类型
                    for (int i = 0; i < args.length; i++) {
                        if (args[i] != null) {
                            if (args[i] instanceof JavaBeanDescriptor) {
                                args[i] = JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) args[i]);
                            } else {
                                throw new RpcException("Generic serialization [" + GENERIC_SERIALIZATION_BEAN
                                        + "] only support message type "
                                        + JavaBeanDescriptor.class.getName()
                                        + " and your message type is "
                                        + args[i].getClass().getName());
                            }
                        }
                    }
                } else if (ProtocolUtils.isProtobufGenericSerialization(generic)) {
                    // 使用Protobuf方式反序列化参数，仅支持单个String类型的protobuf数据
                    if (args.length == 1 && args[0] instanceof String) {
                        try (UnsafeByteArrayInputStream is =
                                new UnsafeByteArrayInputStream(((String) args[0]).getBytes(StandardCharsets.UTF_8))) {
                            args[0] = applicationModel
                                    .getExtensionLoader(Serialization.class)
                                    .getExtension(GENERIC_SERIALIZATION_PROTOBUF)
                                    .deserialize(null, is)
                                    .readObject(method.getParameterTypes()[0]);
                        } catch (Exception e) {
                            throw new RpcException("Deserialize argument failed.", e);
                        }
                    } else {
                        throw new RpcException("Generic serialization [" + GENERIC_SERIALIZATION_PROTOBUF
                                + "] only support one "
                                + String.class.getName() + " argument and your message size is "
                                + args.length
                                + " and type is" + args[0].getClass().getName());
                    }
                }

                // 构建新的RpcInvocation对象，携带反序列化后的参数和执行上下文
                RpcInvocation rpcInvocation = new RpcInvocation(
                        inv.getTargetServiceUniqueName(),
                        invoker.getUrl().getServiceModel(),
                        method.getName(),
                        invoker.getInterface().getName(),
                        invoker.getUrl().getProtocolServiceKey(),
                        method.getParameterTypes(),
                        args,
                        inv.getObjectAttachments(),
                        inv.getInvoker(),
                        inv.getAttributes(),
                        inv instanceof RpcInvocation ? ((RpcInvocation) inv).getInvokeMode() : null);

                // 执行实际的RPC调用
                return invoker.invoke(rpcInvocation);
            } catch (NoSuchMethodException | ClassNotFoundException e) {
                // 捕获方法查找失败的异常并重新抛出
                throw new RpcException(e.getMessage(), e);
            }
        }

        // 非泛化调用请求，直接放行
        return invoker.invoke(inv);
    }


    private Object[] getGsonGenericArgs(final Object[] args, Type[] types) {
        return IntStream.range(0, args.length)
                .mapToObj(i -> {
                    if (args[i] == null) {
                        return null;
                    }
                    if (!(args[i] instanceof String)) {
                        throw new RpcException(
                                "When using GSON to deserialize generic dubbo request arguments, the arguments must be of type String");
                    }
                    String str = args[i].toString();
                    try {
                        return GsonUtils.fromJson(str, types[i]);
                    } catch (RuntimeException ex) {
                        throw new RpcException(ex.getMessage());
                    }
                })
                .toArray();
    }

    private String getGenericValueFromRpcContext() {
        String generic = RpcContext.getServerAttachment().getAttachment(GENERIC_KEY);
        if (StringUtils.isBlank(generic)) {
            generic = RpcContext.getClientAttachment().getAttachment(GENERIC_KEY);
        }
        return generic;
    }

    /**
     * 根据方法签名查找对应的Method对象
     * 支持两种查找模式：无参数类型时通过方法名唯一匹配，有参数类型时通过方法名和参数类型精确匹配
     * 使用类加载器缓存优化类型转换性能
     *
     * @param clazz 目标类，在该类中查找方法
     * @param methodName 方法名称
     * @param parameterTypes 参数类型名称数组，可以为null表示不指定参数类型
     * @param serviceModel 服务模型，用于从服务描述符中获取方法信息，可以为null
     * @return 找到的Method对象
     * @throws NoSuchMethodException 当找不到匹配的方法时抛出
     * @throws ClassNotFoundException 当参数类型名称无法转换为Class对象时抛出
     */
    public Method findMethodByMethodSignature(
            Class<?> clazz, String methodName, String[] parameterTypes, ServiceModel serviceModel)
            throws NoSuchMethodException, ClassNotFoundException {
        Method method;

        // 当未指定参数类型时，通过方法名进行唯一性匹配
        if (parameterTypes == null) {
            List<Method> found = new ArrayList<>();
            for (Method m : clazz.getMethods()) {
                if (m.getName().equals(methodName)) {
                    found.add(m);
                }
            }

            // 检查是否找到匹配的方法，且必须唯一
            if (found.isEmpty()) {
                throw new NoSuchMethodException("No such method " + methodName + " in class " + clazz);
            }
            if (found.size() > 1) {
                String msg = String.format(
                        "Not unique method for method name(%s) in class(%s), find %d methods.",
                        methodName, clazz.getName(), found.size());
                throw new IllegalStateException(msg);
            }
            method = found.get(0);
        } else {
            // 当指定参数类型时，将类型名称转换为Class对象并进行精确匹配

            // 将参数类型名称数组转换为Class对象数组，使用缓存提升性能
            Class<?>[] types = new Class<?>[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                ClassLoader classLoader = ClassUtils.getClassLoader();

                // 从缓存中获取或创建类加载器对应的类型映射表
                Map<String, Class<?>> cacheMap = classCache.get(classLoader);
                if (cacheMap == null) {
                    cacheMap = new ConcurrentHashMap<>();
                    classCache.putIfAbsent(classLoader, cacheMap);
                    cacheMap = classCache.get(classLoader);
                }

                // 先从缓存查找类型，不存在则通过反射转换并缓存
                types[i] = cacheMap.get(parameterTypes[i]);
                if (types[i] == null) {
                    types[i] = ReflectUtils.name2class(parameterTypes[i]);
                    cacheMap.put(parameterTypes[i], types[i]);
                }
            }

            // 根据是否有服务模型，选择不同的方法查找策略
            if (serviceModel != null) {
                // 优先使用服务模型中的方法描述符，支持更精确的方法匹配
                MethodDescriptor methodDescriptor =
                        serviceModel.getServiceModel().getMethod(methodName, types);
                if (methodDescriptor == null) {
                    throw new NoSuchMethodException("No such method " + methodName + " in class " + clazz);
                }
                method = methodDescriptor.getMethod();
            } else {
                // 直接使用反射API查找方法
                method = clazz.getMethod(methodName, types);
            }
        }
        return method;
    }


        /**
     * 处理泛化调用的响应结果，根据指定的泛化类型对返回值进行序列化或转换
     * 仅当调用的是$invoke/$invokeAsync方法且目标服务不是GenericService实现类时执行
     *
     * @param appResponse RPC调用的应用层响应结果，包含返回值或异常信息
     * @param invoker 调用器对象，用于获取服务接口类型等信息
     * @param inv 调用上下文对象，包含方法名、参数、附件中的泛化类型标识等
     */
    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation inv) {
        // 校验是否为泛化调用请求（方法名为$invoke或$invokeAsync，且有3个参数，且非GenericService接口）
        if ((inv.getMethodName().equals($INVOKE) || inv.getMethodName().equals($INVOKE_ASYNC))
                && inv.getArguments() != null
                && inv.getArguments().length == 3
                && !GenericService.class.isAssignableFrom(invoker.getInterface())) {

            // 从附件或RpcContext中获取泛化调用类型标识
            String generic = inv.getAttachment(GENERIC_KEY);
            if (StringUtils.isBlank(generic)) {
                generic = getGenericValueFromRpcContext();
            }

            // 处理响应异常，根据Dubbo2兼容性配置将异常转换为GenericException类型
            if (appResponse.hasException()
                    && Dubbo2CompactUtils.isEnabled()
                    && Dubbo2GenericExceptionUtils.isGenericExceptionClassLoaded()) {
                Throwable appException = appResponse.getException();
                if (appException instanceof GenericException) {
                    // 重新创建GenericException以确保使用正确的类加载器版本
                    GenericException tmp = (GenericException) appException;
                    GenericException recreated = Dubbo2GenericExceptionUtils.newGenericException(
                            tmp.getMessage(), tmp.getCause(), tmp.getExceptionClass(), tmp.getExceptionMessage());
                    if (recreated != null) {
                        appException = recreated;
                    }
                    appException.setStackTrace(tmp.getStackTrace());
                }
                // 如果异常不是GenericException类型，则进行转换
                if (!(Dubbo2GenericExceptionUtils.getGenericExceptionClass()
                        .isAssignableFrom(appException.getClass()))) {
                    GenericException recreated = Dubbo2GenericExceptionUtils.newGenericException(appException);
                    if (recreated != null) {
                        appException = recreated;
                    }
                }
                appResponse.setException(appException);
            }

            // 如果配置为返回原始结果，则跳过后续的泛化处理
            if (ProtocolUtils.isGenericReturnRawResult(generic)) {
                return;
            }

            // 根据不同的泛化序列化类型，对返回值进行相应的序列化处理
            if (ProtocolUtils.isJavaGenericSerialization(generic)) {
                // 使用Native Java序列化方式，将返回值序列化为字节数组
                try {
                    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream(512);
                    applicationModel
                            .getExtensionLoader(Serialization.class)
                            .getExtension(GENERIC_SERIALIZATION_NATIVE_JAVA)
                            .serialize(null, os)
                            .writeObject(appResponse.getValue());
                    appResponse.setValue(os.toByteArray());
                } catch (IOException e) {
                    throw new RpcException(
                            "Generic serialization [" + GENERIC_SERIALIZATION_NATIVE_JAVA
                                    + "] serialize result failed.",
                            e);
                }
            } else if (ProtocolUtils.isBeanGenericSerialization(generic)) {
                // 使用JavaBean序列化方式，将返回值转换为Map结构
                appResponse.setValue(JavaBeanSerializeUtil.serialize(appResponse.getValue(), JavaBeanAccessor.METHOD));
            } else if (ProtocolUtils.isProtobufGenericSerialization(generic)) {
                // 使用Protobuf序列化方式，将返回值序列化并转换为字符串
                try {
                    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream(512);
                    applicationModel
                            .getExtensionLoader(Serialization.class)
                            .getExtension(GENERIC_SERIALIZATION_PROTOBUF)
                            .serialize(null, os)
                            .writeObject(appResponse.getValue());
                    appResponse.setValue(os.toString());
                } catch (IOException e) {
                    throw new RpcException(
                            "Generic serialization [" + GENERIC_SERIALIZATION_PROTOBUF + "] serialize result failed.",
                            e);
                }
            } else if (ProtocolUtils.isGsonGenericSerialization(generic)) {
                // 使用Gson JSON序列化方式，将返回值转换为JSON字符串
                appResponse.setValue(GsonUtils.toJson(appResponse.getValue()));
            } else {
                // 默认使用POJO泛化方式，将复杂对象转换为通用的Map/List/String结构
                appResponse.setValue(PojoUtils.generalize(appResponse.getValue()));
            }
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {}
}
