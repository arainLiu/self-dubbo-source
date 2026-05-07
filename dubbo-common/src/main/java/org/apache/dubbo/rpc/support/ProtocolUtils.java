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
package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_RAW_RETURN;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_BEAN;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_DEFAULT;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_GSON;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_NATIVE_JAVA;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_PROTOBUF;

public class ProtocolUtils {

    private static final ConcurrentMap<String, GroupServiceKeyCache> groupServiceKeyCacheMap =
            new ConcurrentHashMap<>();

    private ProtocolUtils() {}

    public static String serviceKey(URL url) {
        return serviceKey(url.getPort(), url.getPath(), url.getVersion(), url.getGroup());
    }

    public static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
        serviceGroup = serviceGroup == null ? "" : serviceGroup;
        GroupServiceKeyCache groupServiceKeyCache = groupServiceKeyCacheMap.get(serviceGroup);
        if (groupServiceKeyCache == null) {
            groupServiceKeyCacheMap.putIfAbsent(serviceGroup, new GroupServiceKeyCache(serviceGroup));
            groupServiceKeyCache = groupServiceKeyCacheMap.get(serviceGroup);
        }
        return groupServiceKeyCache.getServiceKey(serviceName, serviceVersion, port);
    }

    /**
     * 判断指定的泛化调用类型标识是否有效，支持多种序列化方式。
     * <p>
     * 该方法用于校验服务引用配置中的generic参数是否为Dubbo支持的泛化调用模式。
     * 泛化调用允许消费者在不依赖服务端接口类的情况下发起RPC调用，常用于网关、测试平台等场景。
     * </p>
     * <p>
     * 支持的泛化调用类型包括：
     * <ul>
     *   <li><b>default（普通泛化调用）</b>：使用Dubbo默认的序列化方式，将参数序列化为Map结构</li>
     *   <li><b>nativejava（JDK序列化）</b>：支持流式泛化调用，使用JDK原生序列化机制</li>
     *   <li><b>bean（Bean泛化）</b>：基于JavaBean规范的泛化调用方式</li>
     *   <li><b>protobuf（Protobuf序列化）</b>：使用Google Protocol Buffers进行高效序列化</li>
     *   <li><b>gson（Gson序列化）</b>：使用Google Gson库进行JSON序列化</li>
     *   <li><b>raw（原始返回值）</b>：返回未经处理的原始数据格式</li>
     * </ul>
     * </p>
     *
     * @param generic 泛化调用类型标识字符串，通常从URL参数或配置文件中获取
     * @return 如果generic非空且属于上述支持的任一类型则返回true，否则返回false
     */
    public static boolean isGeneric(String generic) {
        return StringUtils.isNotEmpty(generic)
                && (GENERIC_SERIALIZATION_DEFAULT.equalsIgnoreCase(generic) /* Normal generalization cal */
                        || GENERIC_SERIALIZATION_NATIVE_JAVA.equalsIgnoreCase(
                                generic) /* Streaming generalization call supporting jdk serialization */
                        || GENERIC_SERIALIZATION_BEAN.equalsIgnoreCase(generic)
                        || GENERIC_SERIALIZATION_PROTOBUF.equalsIgnoreCase(generic)
                        || GENERIC_SERIALIZATION_GSON.equalsIgnoreCase(generic)
                        || GENERIC_RAW_RETURN.equalsIgnoreCase(generic));
    }

    public static boolean isValidGenericValue(String generic) {
        return isGeneric(generic) || Boolean.FALSE.toString().equalsIgnoreCase(generic);
    }

    public static boolean isDefaultGenericSerialization(String generic) {
        return isGeneric(generic) && GENERIC_SERIALIZATION_DEFAULT.equalsIgnoreCase(generic);
    }

    public static boolean isJavaGenericSerialization(String generic) {
        return isGeneric(generic) && GENERIC_SERIALIZATION_NATIVE_JAVA.equalsIgnoreCase(generic);
    }

    public static boolean isGsonGenericSerialization(String generic) {
        return isGeneric(generic) && GENERIC_SERIALIZATION_GSON.equalsIgnoreCase(generic);
    }

    public static boolean isBeanGenericSerialization(String generic) {
        return isGeneric(generic) && GENERIC_SERIALIZATION_BEAN.equals(generic);
    }

    public static boolean isProtobufGenericSerialization(String generic) {
        return isGeneric(generic) && GENERIC_SERIALIZATION_PROTOBUF.equals(generic);
    }

    public static boolean isGenericReturnRawResult(String generic) {
        return GENERIC_RAW_RETURN.equals(generic);
    }
}
