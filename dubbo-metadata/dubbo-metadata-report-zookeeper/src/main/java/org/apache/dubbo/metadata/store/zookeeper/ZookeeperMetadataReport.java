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
package org.apache.dubbo.metadata.store.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.identifier.BaseMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.remoting.zookeeper.curator5.DataListener;
import org.apache.dubbo.remoting.zookeeper.curator5.EventType;
import org.apache.dubbo.remoting.zookeeper.curator5.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.curator5.ZookeeperClientManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.data.Stat;

import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_ZOOKEEPER_EXCEPTION;
import static org.apache.dubbo.metadata.ServiceNameMapping.DEFAULT_MAPPING_GROUP;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;

public class ZookeeperMetadataReport extends AbstractMetadataReport {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(ZookeeperMetadataReport.class);

    private final String root;

    ZookeeperClient zkClient;

    private ConcurrentMap<String, MappingDataListener> casListenerMap = new ConcurrentHashMap<>();

    public ZookeeperMetadataReport(URL url, ZookeeperClientManager zookeeperClientManager) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getGroup(DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
        zkClient = zookeeperClientManager.connect(url);
    }

    protected String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String value) {
        storeMetadata(consumerMetadataIdentifier, value);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        zkClient.createOrUpdate(getNodePath(metadataIdentifier), URL.encode(url.toFullString()), false);
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        zkClient.delete(getNodePath(metadataIdentifier));
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        String content = zkClient.getContent(getNodePath(metadataIdentifier));
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(Collections.singletonList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urls) {
        zkClient.createOrUpdate(getNodePath(subscriberMetadataIdentifier), urls, false);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        return zkClient.getContent(getNodePath(subscriberMetadataIdentifier));
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return zkClient.getContent(getNodePath(metadataIdentifier));
    }

    private void storeMetadata(MetadataIdentifier metadataIdentifier, String v) {
        zkClient.createOrUpdate(getNodePath(metadataIdentifier), v, false);
    }

    String getNodePath(BaseMetadataIdentifier metadataIdentifier) {
        return toRootDir() + metadataIdentifier.getUniqueKey(KeyTypeEnum.PATH);
    }

    /**
     * 发布应用级别的元数据信息到ZooKeeper存储节点。
     * <p>
     * 该方法采用"首次创建"策略：只有当目标ZooKeeper节点不存在或内容为空，且待发布的元数据内容非空时，
     * 才会创建或更新节点。这种设计确保了元数据的稳定性，避免频繁的覆盖操作，同时保证元数据一旦发布就不会被意外修改。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>根据SubscriberMetadataIdentifier生成ZooKeeper节点路径</li>
     *   <li>检查节点当前内容是否为空，并且待发布的元数据内容是否非空</li>
     *   <li>如果条件满足，调用zkClient.createOrUpdate()创建持久节点并写入元数据内容</li>
     * </ol>
     * </p>
     *
     * @param identifier 订阅者元数据标识符，包含应用名称、版本、分组等信息，用于生成唯一的存储路径
     * @param metadataInfo 应用的完整元数据信息对象，包含所有服务接口的定义和配置
     */
    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo) {
        String path = getNodePath(identifier);
        /*
         * 仅在节点内容为空且待发布内容非空时才执行创建或更新操作，实现首次发布保护
         */
        if (StringUtils.isBlank(zkClient.getContent(path)) && StringUtils.isNotEmpty(metadataInfo.getContent())) {
            zkClient.createOrUpdate(path, metadataInfo.getContent(), false);
        }
    }

    @Override
    public void unPublishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo) {
        String path = getNodePath(identifier);
        if (StringUtils.isNotEmpty(zkClient.getContent(path))) {
            zkClient.delete(path);
        }
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata) {
        String content = zkClient.getContent(getNodePath(identifier));
        return JsonUtils.toJavaObject(content, MetadataInfo.class);
    }

    /**
     * 从Zookeeper查询接口名到应用名的映射关系并注册监听器，支持动态感知映射变更。
     * <p>
     * 该方法是ZookeeperMetadataReport的核心订阅实现，负责从Zookeeper节点读取映射数据并注册Watcher监听器。
     * 通过casListenerMap实现监听器的复用和缓存，避免对同一路径重复注册Zookeeper Watcher导致资源浪费。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>构建Zookeeper路径</b>：调用buildPathKey生成完整的Zookeeper节点路径，格式为：/{group}/{serviceKey}（如：/dubbo/interface:version@group）</li>
     *   <li><b>查找或创建CAS监听器</b>：从casListenerMap中查找是否已有该路径的MappingDataListener，没有则创建新的并注册到Zookeeper客户端</li>
     *   <li><b>Zookeeper Watcher注册</b>：调用zkClient.addDataListener注册Watcher，当节点内容变化时触发MappingDataListener.childChanged回调</li>
     *   <li><b>添加业务监听器</b>：将传入的MappingListener添加到MappingDataListener的内部列表中，实现一对多的事件分发</li>
     *   <li><b>读取当前数据</b>：调用zkClient.getContent读取Zookeeper节点的当前内容，返回逗号分隔的应用名列表字符串</li>
     *   <li><b>解析并返回</b>：调用getAppNames将字符串解析为Set<String>格式的应用名集合，返回给调用方</li>
     * </ol>
     * </p>
     *
     * @param serviceKey 服务接口的唯一标识，格式为：interface:version@group，用于构建Zookeeper节点路径
     * @param listener   映射变更监听器，当Zookeeper节点内容变化时触发回调，通知消费者重新订阅新应用的实例地址
     * @param url        服务URL，包含注册中心连接信息和查询参数，用于构建路径和路由
     * @return 已注册该接口的应用名集合，如果节点不存在或内容为空则返回空集合
     */
    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        String path = buildPathKey(DEFAULT_MAPPING_GROUP, serviceKey);
        /*
         * 复用CAS监听器：
         * 确保同一Zookeeper路径只注册一个Watcher，避免重复监听导致的性能问题
         */
        MappingDataListener mappingDataListener = ConcurrentHashMapUtils.computeIfAbsent(casListenerMap, path, _k -> {
            MappingDataListener newMappingListener = new MappingDataListener(serviceKey, path);
            zkClient.addDataListener(path, newMappingListener);
            return newMappingListener;
        });
        /*
         * 添加业务层监听器：
         * 将上层传入的MappingListener注册到MappingDataListener，实现事件的分发和转发
         */
        mappingDataListener.addListener(listener);
        //通过zk获取数据信息
        return getAppNames(zkClient.getContent(path));
    }

    @Override
    public void removeServiceAppMappingListener(String serviceKey, MappingListener listener) {
        String path = buildPathKey(DEFAULT_MAPPING_GROUP, serviceKey);
        if (null != casListenerMap.get(path)) {
            removeCasServiceMappingListener(path, listener);
        }
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, URL url) {
        String path = buildPathKey(DEFAULT_MAPPING_GROUP, serviceKey);
        return getAppNames(zkClient.getContent(path));
    }

    @Override
    public ConfigItem getConfigItem(String serviceKey, String group) {
        String path = buildPathKey(group, serviceKey);
        return zkClient.getConfigItem(path);
    }

    @Override
    public boolean registerServiceAppMapping(String key, String group, String content, Object ticket) {
        try {
            if (ticket != null && !(ticket instanceof Stat)) {
                throw new IllegalArgumentException("zookeeper publishConfigCas requires stat type ticket");
            }
            String pathKey = buildPathKey(group, key);
            zkClient.createOrUpdate(pathKey, content, false, ticket == null ? null : ((Stat) ticket).getVersion());
            return true;
        } catch (Exception e) {
            logger.warn(REGISTRY_ZOOKEEPER_EXCEPTION, "", "", "zookeeper publishConfigCas failed.", e);
            return false;
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        // release zk client reference, but should not close it
        zkClient = null;
    }

    private String buildPathKey(String group, String serviceKey) {
        return toRootDir() + group + PATH_SEPARATOR + serviceKey;
    }

    private void removeCasServiceMappingListener(String path, MappingListener listener) {
        MappingDataListener mappingDataListener = casListenerMap.get(path);
        mappingDataListener.removeListener(listener);
        if (mappingDataListener.isEmpty()) {
            zkClient.removeDataListener(path, mappingDataListener);
            casListenerMap.remove(path, mappingDataListener);
        }
    }

    private static class MappingDataListener implements DataListener {

        private String serviceKey;
        private String path;
        private Set<MappingListener> listeners;

        public MappingDataListener(String serviceKey, String path) {
            this.serviceKey = serviceKey;
            this.path = path;
            this.listeners = new HashSet<>();
        }

        public void addListener(MappingListener listener) {
            this.listeners.add(listener);
        }

        public void removeListener(MappingListener listener) {
            this.listeners.remove(listener);
        }

        public boolean isEmpty() {
            return listeners.isEmpty();
        }

        @Override
        public void dataChanged(String path, Object value, EventType eventType) {
            if (!this.path.equals(path)) {
                return;
            }
            if (EventType.NodeCreated != eventType && EventType.NodeDataChanged != eventType) {
                return;
            }

            Set<String> apps = getAppNames((String) value);

            MappingChangedEvent event = new MappingChangedEvent(serviceKey, apps);

            listeners.forEach(mappingListener -> mappingListener.onEvent(event));
        }
    }
}
