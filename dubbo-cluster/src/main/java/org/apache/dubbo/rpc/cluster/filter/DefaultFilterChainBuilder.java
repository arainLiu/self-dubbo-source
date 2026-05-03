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
package org.apache.dubbo.rpc.cluster.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionDirector;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Activate
public class DefaultFilterChainBuilder implements FilterChainBuilder {

    /**
     * build consumer/provider filter chain
     */
    /**
     * 构建消费者或服务提供者的过滤器链，将原始Invoker包装为具备横切关注点能力的增强Invoker。
     * <p>
     * 该方法是Dubbo RPC调用链的核心组装逻辑，负责根据URL配置和SPI激活机制，动态加载并组装所有需要的Filter（如日志、监控、限流、认证等）。
     * 采用责任链模式，将多个Filter依次包装成嵌套的Invoker节点，形成从外到内的调用链路。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>获取模块模型</b>：调用getModuleModelsFromUrl从URL中提取关联的ModuleModel列表，用于确定Filter的加载范围（单模块或多模块场景）</li>
     *   <li><b>加载激活的Filter</b>：
     *     <ul>
     *       <li>单模块场景：直接通过ScopeModelUtil.getExtensionLoader获取该模块的Filter扩展加载器，调用getActivateExtension(url, key, group)加载所有匹配的Filter</li>
     *       <li>多模块场景：遍历所有ModuleModel，分别加载各自的Filter并合并，然后调用sortingAndDeduplication进行排序和去重，解决跨模块Filter的优先级冲突</li>
     *       <li>无模块场景：使用全局ExtensionLoader加载Filter，作为兜底方案</li>
     *     </ul>
     *   </li>
     *   <li><b>构建责任链</b>：
     *     <ul>
     *       <li>如果filters为空，直接返回原始originalInvoker，避免不必要的包装开销</li>
     *       <li>从后往前遍历filters列表（保证order小的在外层），依次创建CopyOfFilterChainNode节点，每个节点持有下一个节点的引用和当前Filter对象</li>
     *       <li>最终形成：Filter1(Filter2(Filter3(originalInvoker)))的嵌套结构</li>
     *     </ul>
     *   </li>
     *   <li><b>注册回调</b>：将最外层的last节点和完整filters列表封装为CallbackRegistrationInvoker，负责在Exporter销毁时自动清理Filter资源</li>
     * </ol>
     * </p>
     *
     * @param originalInvoker 原始的Invoker对象，包含服务接口、实现类引用、URL配置等信息，是过滤器链的起点
     * @param key             激活配置的参数键，服务端为"service.filter"，消费端为"reference.filter"，决定从哪些Filter中筛选
     * @param group           分组标识，CommonConstants.PROVIDER或CommonConstants.CONSUMER，确保只加载对应端的Filter
     * @return 经过过滤器链增强的Invoker对象，调用其invoke方法时会依次执行所有Filter的逻辑
     */
    @Override
    public <T> Invoker<T> buildInvokerChain(final Invoker<T> originalInvoker, String key, String group) {
        Invoker<T> last = originalInvoker;
        URL url = originalInvoker.getUrl();
        List<ModuleModel> moduleModels = getModuleModelsFromUrl(url);
        List<Filter> filters;
        if (moduleModels != null && moduleModels.size() == 1) {
            /*
             * 单模块场景：
             * 直接从当前模块的ExtensionLoader中加载激活的Filter
             */
            filters = ScopeModelUtil.getExtensionLoader(Filter.class, moduleModels.get(0))
                    .getActivateExtension(url, key, group);
        } else if (moduleModels != null && moduleModels.size() > 1) {
            /*
             * 多模块场景：
             * 合并所有模块的Filter，并进行排序和去重处理
             */
            filters = new ArrayList<>();
            List<ExtensionDirector> directors = new ArrayList<>();
            for (ModuleModel moduleModel : moduleModels) {
                List<Filter> tempFilters = ScopeModelUtil.getExtensionLoader(Filter.class, moduleModel)
                        .getActivateExtension(url, key, group);
                filters.addAll(tempFilters);
                directors.add(moduleModel.getExtensionDirector());
            }
            filters = sortingAndDeduplication(filters, directors);

        } else {
            /*
             * 无模块场景：
             * 使用全局ExtensionLoader作为兜底方案
             */
            filters = ScopeModelUtil.getExtensionLoader(Filter.class, null).getActivateExtension(url, key, group);
        }

        if (!CollectionUtils.isEmpty(filters)) {
            /*
             * 构建责任链：
             * 从后往前遍历，将Filter依次包装成嵌套的Invoker节点
             */
            for (int i = filters.size() - 1; i >= 0; i--) {
                final Filter filter = filters.get(i);
                final Invoker<T> next = last;
                last = new CopyOfFilterChainNode<>(originalInvoker, next, filter);
            }
            return new CallbackRegistrationInvoker<>(last, filters);
        }

        return last;
    }

    /**
     * build consumer cluster filter chain
     */
    @Override
    public <T> ClusterInvoker<T> buildClusterInvokerChain(
            final ClusterInvoker<T> originalInvoker, String key, String group) {
        ClusterInvoker<T> last = originalInvoker;
        URL url = originalInvoker.getUrl();
        List<ModuleModel> moduleModels = getModuleModelsFromUrl(url);
        List<ClusterFilter> filters;
        if (moduleModels != null && moduleModels.size() == 1) {
            filters = ScopeModelUtil.getExtensionLoader(ClusterFilter.class, moduleModels.get(0))
                    .getActivateExtension(url, key, group);
        } else if (moduleModels != null && moduleModels.size() > 1) {
            filters = new ArrayList<>();
            List<ExtensionDirector> directors = new ArrayList<>();
            for (ModuleModel moduleModel : moduleModels) {
                List<ClusterFilter> tempFilters = ScopeModelUtil.getExtensionLoader(ClusterFilter.class, moduleModel)
                        .getActivateExtension(url, key, group);
                filters.addAll(tempFilters);
                directors.add(moduleModel.getExtensionDirector());
            }
            filters = sortingAndDeduplication(filters, directors);

        } else {
            filters =
                    ScopeModelUtil.getExtensionLoader(ClusterFilter.class, null).getActivateExtension(url, key, group);
        }

        if (!CollectionUtils.isEmpty(filters)) {
            for (int i = filters.size() - 1; i >= 0; i--) {
                final ClusterFilter filter = filters.get(i);
                final Invoker<T> next = last;
                last = new CopyOfClusterFilterChainNode<>(originalInvoker, next, filter);
            }
            return new ClusterCallbackRegistrationInvoker<>(originalInvoker, last, filters);
        }

        return last;
    }

    private <T> List<T> sortingAndDeduplication(List<T> filters, List<ExtensionDirector> directors) {
        Map<Class<?>, T> filtersSet = new TreeMap<>(new ActivateComparator(directors));
        for (T filter : filters) {
            filtersSet.putIfAbsent(filter.getClass(), filter);
        }
        return new ArrayList<>(filtersSet.values());
    }

    /**
     * When the application-level service registration and discovery strategy is adopted, the URL will be of type InstanceAddressURL,
     * and InstanceAddressURL belongs to the application layer and holds the ApplicationModel,
     * but the filter is at the module layer and holds the ModuleModel,
     * so it needs to be based on the url in the ScopeModel type to parse out all the moduleModels held by the url
     * to obtain the filter configuration.
     *
     * @param url URL
     * @return All ModuleModels in the url
     */
    private List<ModuleModel> getModuleModelsFromUrl(URL url) {
        List<ModuleModel> moduleModels = null;
        ScopeModel scopeModel = url.getScopeModel();
        if (scopeModel instanceof ApplicationModel) {
            moduleModels = ((ApplicationModel) scopeModel).getPubModuleModels();
        } else if (scopeModel instanceof ModuleModel) {
            moduleModels = new ArrayList<>();
            moduleModels.add((ModuleModel) scopeModel);
        }
        return moduleModels;
    }
}
