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
package org.apache.dubbo.config.spring.reference;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.util.DubboBeanUtils;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_DUBBO_BEAN_INITIALIZER;

public class ReferenceBeanManager implements ApplicationContextAware {
    public static final String BEAN_NAME = "dubboReferenceBeanManager";
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    // reference key -> reference bean names
    private ConcurrentMap<String, CopyOnWriteArrayList<String>> referenceKeyMap = new ConcurrentHashMap<>();

    // reference alias -> reference bean name
    private ConcurrentMap<String, String> referenceAliasMap = new ConcurrentHashMap<>();

    // reference bean name -> ReferenceBean
    private ConcurrentMap<String, ReferenceBean> referenceBeanMap = new ConcurrentHashMap<>();

    // reference key -> ReferenceConfig instance
    private ConcurrentMap<String, ReferenceConfig> referenceConfigMap = new ConcurrentHashMap<>();

    private ApplicationContext applicationContext;
    private volatile boolean initialized = false;
    private ModuleModel moduleModel;

    /**
     * 添加ReferenceBean到管理器
     * <p>
     * 该方法负责将ReferenceBean注册到管理器中，并进行以下处理：
     * 1. 验证ReferenceBean的ID不为空
     * 2. 检查是否在DubboConfigBeanInitializer之前被提前初始化，如果是则记录警告
     * 3. 生成或获取ReferenceBean的唯一标识key
     * 4. 检测并阻止重复的ReferenceBean注册（相同ID但不同实例）
     * 5. 将ReferenceBean保存到内部映射表中
     * 6. 注册reference key与bean name的映射关系
     * 7. 如果已经完成prepare阶段，则立即初始化该ReferenceBean
     * <p>
     * 注意：如果在BeanPostProcessor尚未加载时就调用此方法（提前初始化），
     * 可能会导致某些组件（如Seata）工作异常。
     *
     * @param referenceBean 要添加的ReferenceBean对象
     * @throws Exception 当发现重复的ReferenceBean或验证失败时抛出异常
     */
    public void addReference(ReferenceBean referenceBean) throws Exception {
        String referenceBeanName = referenceBean.getId();
        Assert.notEmptyString(referenceBeanName, "The id of ReferenceBean cannot be empty");

        // 检查是否提前初始化，此时BeanPostProcessor可能尚未加载
        if (!initialized) {
            // TODO add issue url to describe early initialization
            logger.warn(
                    CONFIG_DUBBO_BEAN_INITIALIZER,
                    "",
                    "",
                    "Early initialize reference bean before DubboConfigBeanInitializer,"
                            + " the BeanPostProcessor has not been loaded at this time, which may cause abnormalities in some components (such as seata): "
                            + referenceBeanName
                            + " = " + ReferenceBeanSupport.generateReferenceKey(referenceBean, applicationContext));
        }

        // 生成或获取ReferenceBean的唯一标识key
        String referenceKey = getReferenceKeyByBeanName(referenceBeanName);
        if (StringUtils.isEmpty(referenceKey)) {
            referenceKey = ReferenceBeanSupport.generateReferenceKey(referenceBean, applicationContext);
        }

        // 检查是否存在重复的ReferenceBean
        ReferenceBean oldReferenceBean = referenceBeanMap.get(referenceBeanName);
        if (oldReferenceBean != null) {
            if (referenceBean != oldReferenceBean) {
                String oldReferenceKey =
                        ReferenceBeanSupport.generateReferenceKey(oldReferenceBean, applicationContext);
                throw new IllegalStateException("Found duplicated ReferenceBean with id: " + referenceBeanName
                        + ", old: " + oldReferenceKey + ", new: " + referenceKey);
            }
            return;
        }

        // 保存ReferenceBean到映射表
        referenceBeanMap.put(referenceBeanName, referenceBean);

        // save cache, map reference key to referenceBeanName，注册key与name的映射关系
        this.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);

        // if add reference after prepareReferenceBeans(), should init it immediately.
        // 如果已经完成prepare阶段，立即初始化新添加的ReferenceBean
        if (initialized) {
            initReferenceBean(referenceBean);
        }
    }

    private String getReferenceKeyByBeanName(String referenceBeanName) {
        Set<Map.Entry<String, CopyOnWriteArrayList<String>>> entries = referenceKeyMap.entrySet();
        for (Map.Entry<String, CopyOnWriteArrayList<String>> entry : entries) {
            if (entry.getValue().contains(referenceBeanName)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public void registerReferenceKeyAndBeanName(String referenceKey, String referenceBeanNameOrAlias) {
        CopyOnWriteArrayList<String> list = ConcurrentHashMapUtils.computeIfAbsent(
                referenceKeyMap, referenceKey, (key) -> new CopyOnWriteArrayList<>());
        if (list.addIfAbsent(referenceBeanNameOrAlias)) {
            // register bean name as alias
            referenceAliasMap.put(referenceBeanNameOrAlias, list.get(0));
        }
    }

    public ReferenceBean getById(String referenceBeanNameOrAlias) {
        String referenceBeanName = transformName(referenceBeanNameOrAlias);
        return referenceBeanMap.get(referenceBeanName);
    }

    // convert reference name/alias to referenceBeanName
    private String transformName(String referenceBeanNameOrAlias) {
        return referenceAliasMap.getOrDefault(referenceBeanNameOrAlias, referenceBeanNameOrAlias);
    }

    public List<String> getBeanNamesByKey(String key) {
        return Collections.unmodifiableList(referenceKeyMap.getOrDefault(key, new CopyOnWriteArrayList<>()));
    }

    public Collection<ReferenceBean> getReferences() {
        return new HashSet<>(referenceBeanMap.values());
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Initialize all reference beans, call at Dubbo starting
     *
     * @throws Exception
     */
    public void prepareReferenceBeans() throws Exception {
        // get moduleModel here (called by DubboConfigBeanInitializer#afterPropertiesSet) to avoid getting null result.
        moduleModel = DubboBeanUtils.getModuleModel(applicationContext);
        initialized = true;
        for (ReferenceBean referenceBean : getReferences()) {
            initReferenceBean(referenceBean);
        }
    }

    /**
     * 初始化ReferenceBean，创建并关联ReferenceConfig
     * <p>
     * 该方法负责将ReferenceBean转换为真正的ReferenceConfig对象，并完成以下操作：
     * 1. 检查ReferenceBean是否已经初始化（通过referenceConfig是否为null判断）
     * 2. 生成或获取ReferenceBean的唯一标识key
     * 3. 从ReferenceBean中提取属性，使用ReferenceCreator创建ReferenceConfig
     * 4. 设置ReferenceConfig的ID（如果不是自动生成的名称）
     * 5. 缓存ReferenceConfig到内部映射表
     * 6. 将ReferenceConfig注册到ConfigManager
     * 7. 设置ModuleDeployer状态为pending，触发模块重新评估部署状态
     * 8. 将ReferenceConfig与ReferenceBean关联
     * <p>
     * 注意：该方法必须在所有Dubbo配置Bean和属性解析器加载完成后才能调用，
     * 以确保能够正确解析配置中的占位符和引用。
     *
     * @param referenceBean 需要初始化的ReferenceBean对象
     * @throws Exception 当初始化失败时抛出异常
     */
    public synchronized void initReferenceBean(ReferenceBean referenceBean) throws Exception {

        // 如果ReferenceConfig已存在，说明已经初始化过，直接返回
        if (referenceBean.getReferenceConfig() != null) {
            return;
        }

        // TOTO check same unique service name but difference reference key (means difference attributes).

        // 生成或获取ReferenceBean的唯一标识key
        String referenceKey = getReferenceKeyByBeanName(referenceBean.getId());
        if (StringUtils.isEmpty(referenceKey)) {
            referenceKey = ReferenceBeanSupport.generateReferenceKey(referenceBean, applicationContext);
        }

        // 查找是否已存在相同key的ReferenceConfig
        ReferenceConfig referenceConfig = referenceConfigMap.get(referenceKey);
        if (referenceConfig == null) {
            // create real ReferenceConfig，从ReferenceBean提取属性并创建ReferenceConfig
            Map<String, Object> referenceAttributes = ReferenceBeanSupport.getReferenceAttributes(referenceBean);
            referenceConfig = ReferenceCreator.create(referenceAttributes, applicationContext)
                    .defaultInterfaceClass(referenceBean.getObjectType())
                    .build();

            // set id if it is not a generated name，设置ReferenceConfig的ID
            if (referenceBean.getId() != null && !referenceBean.getId().contains("#")) {
                referenceConfig.setId(referenceBean.getId());
            }

            // cache referenceConfig，缓存ReferenceConfig
            referenceConfigMap.put(referenceKey, referenceConfig);

            // register ReferenceConfig，注册到ConfigManager并设置模块状态为pending
            moduleModel.getConfigManager().addReference(referenceConfig);
            moduleModel.getDeployer().setPending();
        }

        // associate referenceConfig to referenceBean，将ReferenceConfig与ReferenceBean关联
        referenceBean.setKeyAndReferenceConfig(referenceKey, referenceConfig);
    }

}
