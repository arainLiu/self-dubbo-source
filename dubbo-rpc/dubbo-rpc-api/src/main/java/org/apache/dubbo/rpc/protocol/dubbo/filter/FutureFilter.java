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
package org.apache.dubbo.rpc.protocol.dubbo.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.AsyncMethodInfo;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_REQUEST;

/**
 * EventFilter
 */
@Activate(group = CommonConstants.CONSUMER)
public class FutureFilter implements ClusterFilter, ClusterFilter.Listener {

    public static final String ASYNC_METHOD_INFO = "async-method-info";

    protected static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(FutureFilter.class);

    /**
     * 在执行远程调用前触发 oninvoke 回调，并继续执行后续的过滤器链。
     * <p>
     * 该方法是 FutureFilter 的核心入口，负责在请求正式发出之前执行用户定义的 oninvoke 逻辑（通常用于参数预处理或状态记录）。
     * 随后，它将调用委托给下一个 Invoker 节点。
     * </p>
     * <p>
     * 注意：根据代码中的注释提示，如果希望在调用前就能判断是否需要返回 Future 对象，需要在此处进行额外的配置检查。
     * 这通常与 Dubbo 的异步调用模型（如 AsyncInfo）相关，用于优化同步调用场景下的性能开销。
     * </p>
     *
     * @param invoker 当前调用的执行器，代表远程服务的代理
     * @param invocation 封装了方法名、参数及异步信息的调用对象
     * @return 远程调用的执行结果，可能是同步返回值也可能是异步 Future
     * @throws RpcException 当调用过程中发生异常时抛出
     */
    @Override
    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
        /*
         * 执行调用前的 oninvoke 回调逻辑
         */
        fireInvokeCallback(invoker, invocation);
        // need to configure if there's return value before the invocation in order to help invoker to judge if it's
        // necessary to return future.
        return invoker.invoke(invocation);
    }


    /**
     * 处理异步调用完成后的回调逻辑，根据执行结果触发 onreturn 或 onthrow 回调。
     * <p>
     * 该方法是 {@link Filter.Listener} 接口的一部分，在远程调用结束并收到服务端响应时被自动调用。
     * 它负责检查 {@link Result} 对象的状态：如果包含异常，则调用 {@link #fireThrowCallback} 执行异常回调（onthrow）；
     * 如果执行成功，则调用 {@link #fireReturnCallback} 执行返回结果回调（onreturn）。
     * </p>
     *
     * @param result 远程调用的执行结果，包含返回值或异常信息
     * @param invoker 当前调用的执行器，用于获取服务配置和元数据
     * @param invocation 封装了方法名、参数及异步信息的调用对象
     */
    @Override
    public void onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        if (result.hasException()) {
            /*
             * 调用失败时触发 onthrow 异常回调
             */
            fireThrowCallback(invoker, invocation, result.getException());
        } else {
            /*
             * 调用成功时触发 onreturn 返回结果回调
             */
            fireReturnCallback(invoker, invocation, result.getValue());
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        fireThrowCallback(invoker, invocation, t);
    }

    /**
     * 执行异步调用前的回调方法（oninvoke），在远程请求发出前触发用户自定义逻辑。
     * <p>
     * 该方法从配置中提取 oninvoke 对应的实例和方法，并进行严格的合法性校验：
     * 如果配置了回调，则方法和实例必须同时存在，否则抛出 IllegalStateException。
     * 随后，它通过反射执行回调方法，并将原始调用参数传递给用户逻辑。
     * 如果回调执行过程中发生异常，会自动触发 onthrow 回调逻辑进行异常处理。
     * </p>
     *
     * @param invoker 当前调用的执行器，用于获取服务键和URL信息以定位配置
     * @param invocation 封装了方法名、参数及异步信息的调用对象
     */
    private void fireInvokeCallback(final Invoker<?> invoker, final Invocation invocation) {
        final AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }
        final Method onInvokeMethod = asyncMethodInfo.getOninvokeMethod();
        final Object onInvokeInst = asyncMethodInfo.getOninvokeInstance();

        if (onInvokeMethod == null && onInvokeInst == null) {
            return;
        }
        /*
         * 校验 oninvoke 配置的一致性：方法和实例必须成对出现
         */
        if (onInvokeMethod == null || onInvokeInst == null) {
            throw new IllegalStateException(
                    "service:" + invoker.getUrl().getServiceKey() + " has a oninvoke callback config , but no such "
                            + (onInvokeMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onInvokeMethod.isAccessible()) {
            onInvokeMethod.setAccessible(true);
        }

        Object[] params = invocation.getArguments();
        try {
            /*
             * 执行用户定义的 oninvoke 回调逻辑
             */
            onInvokeMethod.invoke(onInvokeInst, params);
        } catch (InvocationTargetException e) {
            /*
             * 如果回调执行出错，触发 onthrow 异常回调
             */
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }


    /**
     * 执行异步调用成功后的回调方法（onreturn），在获取到远程返回值时触发用户自定义逻辑。
     * <p>
     * 该方法负责处理远程调用成功返回后的业务增强逻辑。它首先从配置中提取 onreturn 对应的实例和方法，
     * 并进行严格的合法性校验：如果配置了回调，则方法和实例必须同时存在。
     * 随后，它根据回调方法的签名动态组装参数数组：
     * <ul>
     *   <li>如果方法只有一个参数，则仅传入远程调用的返回值（result）。</li>
     *   <li>如果方法有多个参数且第二个参数兼容 Object[]，则传入返回值和原始调用参数数组。</li>
     *   <li>其他多参情况则将返回值作为第一个参数，原始调用参数依次向后排列。</li>
     * </ul>
     * 最后通过反射执行回调，若执行过程中发生异常，则自动触发 onthrow 回调逻辑。
     * </p>
     *
     * @param invoker 当前调用的执行器，用于获取服务键和URL信息以定位配置
     * @param invocation 封装了方法名、参数及异步信息的调用对象
     * @param result 远程调用成功返回的结果对象
     */
    private void fireReturnCallback(final Invoker<?> invoker, final Invocation invocation, final Object result) {
        final AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }

        final Method onReturnMethod = asyncMethodInfo.getOnreturnMethod();
        final Object onReturnInst = asyncMethodInfo.getOnreturnInstance();

        // not set onreturn callback
        if (onReturnMethod == null && onReturnInst == null) {
            return;
        }

        /*
         * 校验 onreturn 配置的一致性：方法和实例必须成对出现
         */
        if (onReturnMethod == null || onReturnInst == null) {
            throw new IllegalStateException(
                    "service:" + invoker.getUrl().getServiceKey() + " has a onreturn callback config , but no such "
                            + (onReturnMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onReturnMethod.isAccessible()) {
            onReturnMethod.setAccessible(true);
        }

        Object[] args = invocation.getArguments();
        Object[] params;
        Class<?>[] rParaTypes = onReturnMethod.getParameterTypes();
        /*
         * 根据回调方法的参数签名动态组装传递给用户的参数列表
         */
        if (rParaTypes.length > 1) {
            if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                params = new Object[2];
                params[0] = result;
                params[1] = args;
            } else {
                params = new Object[args.length + 1];
                params[0] = result;
                System.arraycopy(args, 0, params, 1, args.length);
            }
        } else {
            params = new Object[] {result};
        }
        try {
            /*
             * 执行用户定义的 onreturn 回调逻辑
             */
            onReturnMethod.invoke(onReturnInst, params);
        } catch (InvocationTargetException e) {
            /*
             * 如果回调执行出错，触发 onthrow 异常回调
             */
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }


    /**
     * 执行异步调用异常时的回调方法（onthrow），在远程调用发生错误时触发用户自定义逻辑。
     * <p>
     * 该方法负责处理远程调用失败后的异常补偿或记录逻辑。它首先从配置中提取 onthrow 对应的实例和方法，
     * 并进行严格的合法性校验：如果配置了回调，则方法和实例必须同时存在。
     * </p>
     * <p>
     * 参数组装与类型匹配：
     * <ul>
     *   <li>首先检查回调方法的第一个参数是否与抛出的异常类型兼容，如果不兼容则直接记录错误日志并放弃调用。</li>
     *   <li>如果类型匹配，则根据方法签名动态组装参数：
     *     <ul>
     *       <li>单参模式：仅传入异常对象。</li>
     *       <li>双参且第二参为 Object[]：传入异常对象和原始调用参数数组。</li>
     *       <li>其他多参模式：将异常作为第一个参数，原始调用参数依次平铺在后面。</li>
     *     </ul>
     *   </li>
     * </ul>
     * </p>
     * <p>
     * 异常处理：如果在执行回调方法本身时发生异常，会捕获并记录错误日志，防止回调逻辑掩盖原始的调用异常。
     * </p>
     *
     * @param invoker 当前调用的执行器，用于获取服务键和URL信息以定位配置
     * @param invocation 封装了方法名、参数及异步信息的调用对象
     * @param exception 远程调用过程中抛出的原始异常对象
     */
    private void fireThrowCallback(final Invoker<?> invoker, final Invocation invocation, final Throwable exception) {
        final AsyncMethodInfo asyncMethodInfo = getAsyncMethodInfo(invoker, invocation);
        if (asyncMethodInfo == null) {
            return;
        }

        final Method onthrowMethod = asyncMethodInfo.getOnthrowMethod();
        final Object onthrowInst = asyncMethodInfo.getOnthrowInstance();

        // onthrow callback not configured
        if (onthrowMethod == null && onthrowInst == null) {
            return;
        }
        /*
         * 校验 onthrow 配置的一致性：方法和实例必须成对出现
         */
        if (onthrowMethod == null || onthrowInst == null) {
            throw new IllegalStateException(
                    "service:" + invoker.getUrl().getServiceKey() + " has a onthrow callback config , but no such "
                            + (onthrowMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onthrowMethod.isAccessible()) {
            onthrowMethod.setAccessible(true);
        }
        Class<?>[] rParaTypes = onthrowMethod.getParameterTypes();
        /*
         * 只有当回调方法的第一个参数能够接收当前异常类型时，才执行回调
         */
        if (rParaTypes[0].isAssignableFrom(exception.getClass())) {
            try {
                Object[] args = invocation.getArguments();
                Object[] params;

                /*
                 * 根据回调方法的参数签名动态组装传递给用户的参数列表
                 */
                if (rParaTypes.length > 1) {
                    if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                        params = new Object[2];
                        params[0] = exception;
                        params[1] = args;
                    } else {
                        params = new Object[args.length + 1];
                        params[0] = exception;
                        System.arraycopy(args, 0, params, 1, args.length);
                    }
                } else {
                    params = new Object[] {exception};
                }
                onthrowMethod.invoke(onthrowInst, params);
            } catch (Throwable e) {
                logger.error(
                        PROTOCOL_FAILED_REQUEST,
                        "",
                        "",
                        RpcUtils.getMethodName(invocation) + ".call back method invoke error . callback method :"
                                + onthrowMethod + ", url:" + invoker.getUrl(),
                        e);
            }
        } else {
            logger.error(
                    PROTOCOL_FAILED_REQUEST,
                    "",
                    "",
                    RpcUtils.getMethodName(invocation) + ".call back method invoke error . callback method :"
                            + onthrowMethod + ", url:" + invoker.getUrl(),
                    exception);
        }
    }

    private AsyncMethodInfo getAsyncMethodInfo(Invoker<?> invoker, Invocation invocation) {
        AsyncMethodInfo asyncMethodInfo = (AsyncMethodInfo) invocation.get(ASYNC_METHOD_INFO);
        if (asyncMethodInfo != null) {
            return asyncMethodInfo;
        }

        ServiceModel serviceModel = invocation.getServiceModel();
        if (!(serviceModel instanceof ConsumerModel)) {
            return null;
        }

        String methodName = RpcUtils.getMethodName(invocation);

        return ((ConsumerModel) serviceModel).getAsyncInfo(methodName);
    }
}
