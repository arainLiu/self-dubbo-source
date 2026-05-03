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
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.AbstractServiceNameMapping;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.registry.client.RegistryClusterIdentifier;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_PROPERTY_TYPE_MISMATCH;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.registry.Constants.CAS_RETRY_TIMES_KEY;
import static org.apache.dubbo.registry.Constants.CAS_RETRY_WAIT_TIME_KEY;
import static org.apache.dubbo.registry.Constants.DEFAULT_CAS_RETRY_TIMES;
import static org.apache.dubbo.registry.Constants.DEFAULT_CAS_RETRY_WAIT_TIME;

public class MetadataServiceNameMapping extends AbstractServiceNameMapping {

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private static final List<String> IGNORED_SERVICE_INTERFACES =
            Collections.singletonList(MetadataService.class.getName());

    private final int casRetryTimes;
    private final int casRetryWaitTime;
    protected MetadataReportInstance metadataReportInstance;

    public MetadataServiceNameMapping(ApplicationModel applicationModel) {
        super(applicationModel);
        metadataReportInstance = applicationModel.getBeanFactory().getBean(MetadataReportInstance.class);
        casRetryTimes = ConfigurationUtils.getGlobalConfiguration(applicationModel)
                .getInt(CAS_RETRY_TIMES_KEY, DEFAULT_CAS_RETRY_TIMES);
        casRetryWaitTime = ConfigurationUtils.getGlobalConfiguration(applicationModel)
                .getInt(CAS_RETRY_WAIT_TIME_KEY, DEFAULT_CAS_RETRY_WAIT_TIME);
    }

    @Override
    public boolean hasValidMetadataCenter() {
        return !CollectionUtils.isEmpty(
                applicationModel.getApplicationConfigManager().getMetadataConfigs());
    }

    /**
     * 将服务接口名映射到应用名并注册到所有元数据中心，支持应用级服务发现。
     * <p>
     * 该方法负责建立 {interface -> appName} 的映射关系，使得消费者可以通过接口名查找到对应的提供者应用。
     * 支持多元数据中心并发注册和CAS（Compare-And-Set）乐观锁机制保证并发安全。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>配置校验</b>：检查是否配置了元数据中心，未配置则直接返回false</li>
     *   <li><b>忽略检查</b>：对于MetadataService等内部接口，跳过映射直接返回true</li>
     *   <li><b>遍历元数据中心</b>：向所有配置的元数据中心注册映射关系</li>
     *   <li><b>直接注册尝试</b>：优先调用registerServiceAppMapping尝试直接写入映射</li>
     *   <li><b>CAS重试机制</b>：如果直接注册失败，通过CAS方式读取-修改-写入，支持多次重试</li>
     *   <li><b>结果汇总</b>：任意元数据中心注册失败都会导致最终返回false</li>
     * </ol>
     * </p>
     * <p>
     * CAS并发控制逻辑：
     * <br>1. 读取当前配置内容和版本号（ticket）
     * <br>2. 在原有内容基础上追加当前应用名（逗号分隔）
     * <br>3. 携带ticket进行原子更新，失败则随机等待后重试
     * <br>4. 最多重试casRetryTimes次（默认3次），每次等待时间随机分布在0~casRetryWaitTime之间
     * </p>
     *
     * @param url 服务的URL地址，用于提取服务接口名和分组信息
     * @return 所有元数据中心都注册成功返回true，任意一个失败返回false
     */
    @Override
    public boolean map(URL url) {
        if (CollectionUtils.isEmpty(
                applicationModel.getApplicationConfigManager().getMetadataConfigs())) {
            logger.warn(
                    COMMON_PROPERTY_TYPE_MISMATCH,
                    "",
                    "",
                    "[METADATA_REGISTER] No valid metadata config center found for mapping report.");
            return false;
        }
        String serviceInterface = url.getServiceInterface();
        /*
         * 过滤内部服务接口：
         * MetadataService等框架内置接口不需要进行应用名映射
         */
        if (IGNORED_SERVICE_INTERFACES.contains(serviceInterface)) {
            return true;
        }

        boolean result = true;
        for (Map.Entry<String, MetadataReport> entry :
                metadataReportInstance.getMetadataReports(true).entrySet()) {
            MetadataReport metadataReport = entry.getValue();
            String appName = applicationModel.getApplicationName();
            try {
                if (metadataReport.registerServiceAppMapping(serviceInterface, appName, url)) {
                    /*
                     * 直接注册成功：
                     * 元数据中心支持原子性注册操作，无需CAS流程
                     */
                    // MetadataReport support directly register service-app mapping
                    continue;
                }

                /*
                 * CAS重试机制：
                 * 当直接注册不支持或失败时，采用CAS方式进行并发安全的追加操作
                 */
                boolean succeeded = false;
                int currentRetryTimes = 1;
                String newConfigContent = appName;
                do {
                    /*
                     * 读取当前配置：
                     * 获取最新的映射内容和版本号（ticket），用于后续CAS比较
                     */
                    ConfigItem configItem = metadataReport.getConfigItem(serviceInterface, DEFAULT_MAPPING_GROUP);
                    String oldConfigContent = configItem.getContent();
                    if (StringUtils.isNotEmpty(oldConfigContent)) {
                        String[] oldAppNames = oldConfigContent.split(",");
                        if (oldAppNames.length > 0) {
                            /*
                             * 幂等性检查：
                             * 如果当前应用名已存在于映射列表中，无需重复注册
                             */
                            for (String oldAppName : oldAppNames) {
                                if (StringUtils.trim(oldAppName).equals(appName)) {
                                    succeeded = true;
                                    break;
                                }
                            }
                        }
                        if (succeeded) {
                            break;
                        }
                        /*
                         * 构建新配置内容：
                         * 在原有应用列表基础上追加当前应用名，格式：app1,app2,app3
                         */
                        newConfigContent = oldConfigContent + COMMA_SEPARATOR + appName;
                    }
                    /*
                     * CAS原子更新：
                     * 携带ticket进行版本校验，只有当配置未被其他线程修改时才更新成功
                     */
                    succeeded = metadataReport.registerServiceAppMapping(
                            serviceInterface, DEFAULT_MAPPING_GROUP, newConfigContent, configItem.getTicket());
                    if (!succeeded) {
                        /*
                         * CAS冲突处理：
                         * 随机等待一段时间后重试，避免高并发下的活锁问题
                         */
                        int waitTime = ThreadLocalRandom.current().nextInt(casRetryWaitTime);
                        logger.info("Failed to publish service name mapping to metadata center by cas operation. "
                                + "Times: "
                                + currentRetryTimes + ". " + "Next retry delay: "
                                + waitTime + ". " + "Service Interface: "
                                + serviceInterface + ". " + "Origin Content: "
                                + oldConfigContent + ". " + "Ticket: "
                                + configItem.getTicket() + ". " + "Expected Content: "
                                + newConfigContent);
                        Thread.sleep(waitTime);
                    }
                } while (!succeeded && currentRetryTimes++ <= casRetryTimes);

                if (!succeeded) {
                    result = false;
                }
            } catch (Exception e) {
                result = false;
                logger.warn(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Failed registering mapping to remote." + metadataReport,
                        e);
            }
        }

        return result;
    }

    /**
     * 从元数据中心查询接口名到应用名的映射关系，返回已注册该接口的所有应用名集合。
     * <p>
     * 该方法是ServiceNameMapping接口的核心查询实现，负责根据服务URL从远程元数据中心（如Zookeeper、Nacos）
     * 拉取接口级别的应用映射信息。与getAndListen不同，该方法仅执行一次性查询，不注册动态监听器。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>提取接口名</b>：从URL中获取serviceInterface参数，作为查询元数据的主键</li>
     *   <li><b>确定注册中心集群</b>：调用getRegistryCluster解析URL中的集群标识，支持多元数据中心场景下的精确查询</li>
     *   <li><b>获取元数据报告实例</b>：从metadataReportInstance中获取指定集群的MetadataReport对象，可能为null（集群未配置或连接失败）</li>
     *   <li><b>空值保护</b>：如果metadataReport为null，返回空集合避免NullPointerException，调用方需处理无映射关系的场景</li>
     *   <li><b>执行查询</b>：调用metadataReport.getServiceAppMapping从远程存储中读取映射数据，返回Set<String>格式的应用名列表</li>
     * </ol>
     * </p>
     *
     * @param url 服务URL，包含接口名、版本、分组等信息，用于构建元数据查询的key
     * @return 已注册该接口的应用名集合，如果元数据中心不可用或暂无映射关系则返回空集合（非null）
     */
    @Override
    public Set<String> get(URL url) {
        String serviceInterface = url.getServiceInterface();
        String registryCluster = getRegistryCluster(url);
        MetadataReport metadataReport = metadataReportInstance.getMetadataReport(registryCluster);
        if (metadataReport == null) {
            return Collections.emptySet();
        }
        return metadataReport.getServiceAppMapping(serviceInterface, url);
    }

    /**
     * 从元数据中心查询接口名到应用名的映射关系并注册监听器，支持动态感知映射变更。
     * <p>
     * 该方法是ServiceNameMapping接口的核心订阅实现，在get方法的基础上增加了监听器注册功能。
     * 当元数据中心检测到新的应用注册同一接口时，会自动触发mappingListener回调，通知消费者重新订阅新应用的实例地址。
     * 在多元数据中心场景下，随机选择一个集群进行查询即可，因为所有集群的映射数据保持一致。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>提取接口名</b>：从URL中获取serviceInterface参数，作为查询元数据的主键</li>
     *   <li><b>确定注册中心集群</b>：调用getRegistryCluster解析URL中的集群标识，支持多元数据中心场景下的精确路由</li>
     *   <li><b>获取元数据报告实例</b>：从metadataReportInstance中获取指定集群的MetadataReport对象，可能为null（集群未配置或连接失败）</li>
     *   <li><b>空值保护</b>：如果metadataReport为null，返回空集合避免NullPointerException，调用方需处理无映射关系的场景</li>
     *   <li><b>执行查询并注册监听器</b>：调用metadataReport.getServiceAppMapping同时完成数据查询和监听器注册，返回当前已注册的应用名集合</li>
     * </ol>
     * </p>
     *
     * @param url             服务URL，包含接口名、版本、分组等信息，用于构建元数据查询的key
     * @param mappingListener 映射变更监听器，当检测到新应用注册同一接口时触发onEvent回调，传递更新后的应用名集合
     * @return 已注册该接口的应用名集合，如果元数据中心不可用或暂无映射关系则返回空集合（非null）
     */
    @Override
    public Set<String> getAndListen(URL url, MappingListener mappingListener) {
        String serviceInterface = url.getServiceInterface();
        // randomly pick one metadata report is ok for it's guaranteed all metadata report will have the same mapping
        // data.
        String registryCluster = getRegistryCluster(url);
        MetadataReport metadataReport = metadataReportInstance.getMetadataReport(registryCluster);
        if (metadataReport == null) {
            return Collections.emptySet();
        }
        return metadataReport.getServiceAppMapping(serviceInterface, mappingListener, url);
    }

    @Override
    protected void removeListener(URL url, MappingListener mappingListener) {
        String serviceInterface = url.getServiceInterface();
        // randomly pick one metadata report is ok for it's guaranteed each metadata report will have the same mapping
        // content.
        String registryCluster = getRegistryCluster(url);
        MetadataReport metadataReport = metadataReportInstance.getMetadataReport(registryCluster);
        if (metadataReport == null) {
            return;
        }
        metadataReport.removeServiceAppMappingListener(serviceInterface, mappingListener);
    }

    protected String getRegistryCluster(URL url) {
        String registryCluster = RegistryClusterIdentifier.getExtension(url).providerKey(url);
        if (registryCluster == null) {
            registryCluster = DEFAULT_KEY;
        }
        int i = registryCluster.indexOf(",");
        if (i > 0) {
            registryCluster = registryCluster.substring(0, i);
        }
        return registryCluster;
    }
}
