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
package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.LoggerCodeConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstanceCustomizer;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.setEndpoints;

/**
 * A Class to customize the ports of {@link Protocol protocols} into
 * {@link ServiceInstance#getMetadata() the metadata of service instance}
 *
 * @since 2.7.5
 */
public class ProtocolPortsMetadataCustomizer implements ServiceInstanceCustomizer {
    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(ProtocolPortsMetadataCustomizer.class);

    /**
     * 自定义服务实例的协议端口元数据，将暴露的服务URL按协议和端口进行归类并设置到实例中。
     * <p>
     * 该方法从服务实例的元数据信息中提取所有已导出的服务URL，统计每个协议监听的端口号，
     * 并将结果以endpoints的形式存储到服务实例的元数据中。主要用于多协议场景下，
     * 让消费者能够感知提供者支持的所有协议及其对应的端口。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>检查服务元数据和导出URL集合是否为空，如果为空则直接返回</li>
     *   <li>遍历所有导出的URL集合，提取协议名和端口号，构建protocol->port映射关系</li>
     *   <li>如果同一协议在不同端口上监听（旧端口与新端口不同），记录警告日志，后出现的端口会覆盖先前的端口</li>
     *   <li>如果protocols映射不为空（存在至少一个协议），调用setEndpoints()将协议端口信息设置到服务实例中</li>
     * </ol>
     * </p>
     * <p>
     * 注意：当前实现对于同一协议监听多个端口的场景仅保留最后一个端口（见TODO注释），
     * 这可能导致部分端口信息丢失。未来可能需要支持同一协议的多端口注册。
     * </p>
     *
     * @param serviceInstance 要定制的服务实例对象，会被添加协议端口元数据
     * @param applicationModel 应用模型，提供定制所需的上下文信息
     */
    @Override
    public void customize(ServiceInstance serviceInstance, ApplicationModel applicationModel) {
        MetadataInfo metadataInfo = serviceInstance.getServiceMetadata();
        if (metadataInfo == null || CollectionUtils.isEmptyMap(metadataInfo.getExportedServiceURLs())) {
            return;
        }

        Map<String, Integer> protocols = new HashMap<>();
        Set<URL> urls = metadataInfo.collectExportedURLSet();
        urls.forEach(url -> {
            // TODO, same protocol listen on different ports will override with each other.
            String protocol = url.getProtocol();
            Integer oldPort = protocols.get(protocol);
            int newPort = url.getPort();
            if (oldPort != null && oldPort != newPort) {
                LOGGER.warn(
                        LoggerCodeConstants.PROTOCOL_INCORRECT_PARAMETER_VALUES,
                        "the protocol is listening multiple ports",
                        "",
                        "Same protocol " + "[" + protocol + "]" + " listens on different ports " + "[" + oldPort + ","
                                + newPort + "]" + " will override with each other" + ". The port [" + oldPort
                                + "] is overridden with port [" + newPort + "].");
            }
            protocols.put(protocol, newPort);
        });

        /*
         * 仅在存在至少一个协议时设置endpoints元数据
         */
        if (protocols.size() > 0) { // set endpoints only for multi-protocol scenario
            setEndpoints(serviceInstance, protocols);
        }
    }
}
