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
package org.apache.dubbo.config.spring.context;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.spring.aot.AotWithSpringDetector;
import org.apache.dubbo.config.spring.util.DubboBeanUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModel;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.ObjectUtils;

/**
 * Dubbo spring initialization entry point
 */
public class DubboSpringInitializer {

    private static final Logger logger = LoggerFactory.getLogger(DubboSpringInitializer.class);

    private static final Map<BeanDefinitionRegistry, DubboSpringInitContext> REGISTRY_CONTEXT_MAP =
            new ConcurrentHashMap<>();

    public DubboSpringInitializer() {}

        /**
         * 初始化Dubbo Spring容器
         * <p>
         * 该方法负责初始化Dubbo与Spring的集成环境，包括：
         * 1. 创建DubboSpringInitContext上下文对象
         * 2. 确保每个BeanDefinitionRegistry只初始化一次
         * 3. 查找Spring BeanFactory
         * 4. 初始化Dubbo上下文并绑定到Spring容器
         *
         * @param registry Bean定义注册表，用于注册和管理Dubbo相关的Bean定义
         */
        public static void initialize(BeanDefinitionRegistry registry) {

            // prepare context and do customize
            DubboSpringInitContext context = new DubboSpringInitContext();

            // Spring ApplicationContext may not ready at this moment (e.g. load from xml), so use registry as key
            if (REGISTRY_CONTEXT_MAP.putIfAbsent(registry, context) != null) {
                return;
            }

            // find beanFactory
            ConfigurableListableBeanFactory beanFactory = findBeanFactory(registry);

            // init dubbo context
            initContext(context, registry, beanFactory);
        }

    public static boolean remove(BeanDefinitionRegistry registry) {
        return REGISTRY_CONTEXT_MAP.remove(registry) != null;
    }

    public static boolean remove(ApplicationContext springContext) {
        AutowireCapableBeanFactory autowireCapableBeanFactory = springContext.getAutowireCapableBeanFactory();
        for (Map.Entry<BeanDefinitionRegistry, DubboSpringInitContext> entry : REGISTRY_CONTEXT_MAP.entrySet()) {
            DubboSpringInitContext initContext = entry.getValue();
            if (initContext.getApplicationContext() == springContext
                    || initContext.getBeanFactory() == autowireCapableBeanFactory
                    || initContext.getRegistry() == autowireCapableBeanFactory) {
                DubboSpringInitContext context = REGISTRY_CONTEXT_MAP.remove(entry.getKey());
                logger.info("Unbind " + safeGetModelDesc(context.getModuleModel()) + " from spring container: "
                        + ObjectUtils.identityToString(entry.getKey()));
                return true;
            }
        }
        return false;
    }

    static Map<BeanDefinitionRegistry, DubboSpringInitContext> getContextMap() {
        return REGISTRY_CONTEXT_MAP;
    }

    static DubboSpringInitContext findBySpringContext(ApplicationContext applicationContext) {
        for (DubboSpringInitContext initContext : REGISTRY_CONTEXT_MAP.values()) {
            if (initContext.getApplicationContext() == applicationContext) {
                return initContext;
            }
        }
        return null;
    }

    /**
     * 初始化Dubbo上下文并将其绑定到Spring容器
     * <p>
     * 该方法执行以下核心逻辑：
     * 1. 设置上下文中的Registry和BeanFactory引用
     * 2. 通过SPI机制自定义上下文配置
     * 3. 初始化ModuleModel，如果未自定义则创建默认的ApplicationModel和ModuleModel
     * 4. 设置模块属性
     * 5. 将Dubbo上下文对象注册为Spring单例Bean
     * 6. 标记上下文为已绑定状态
     * 7. 注册通用Bean（非AOT模式）
     *
     * @param context Dubbo Spring初始化上下文对象
     * @param registry Bean定义注册表
     * @param beanFactory Spring可配置的列表式Bean工厂
     */
    private static void initContext(
            DubboSpringInitContext context,
            BeanDefinitionRegistry registry,
            ConfigurableListableBeanFactory beanFactory) {
        // 设置上下文的Registry和BeanFactory引用
        context.setRegistry(registry);
        context.setBeanFactory(beanFactory);

        // 通过SPI机制加载并执行DubboSpringInitCustomizer自定义器，允许用户自定义绑定的ModuleModel
        customize(context);

        // 初始化ModuleModel（Dubbo模块模型）
        ModuleModel moduleModel = context.getModuleModel();
        if (moduleModel == null) {
            // 如果未通过自定义器设置ModuleModel，则创建默认的ApplicationModel和ModuleModel
            ApplicationModel applicationModel;
            if (findContextForApplication(ApplicationModel.defaultModel()) == null) {
                // 第一个Spring容器使用默认的Application实例
                applicationModel = ApplicationModel.defaultModel();
                logger.info("Use default application: " + applicationModel.getDesc());
            } else {
                // 后续Spring容器创建新的Application实例，实现多应用隔离
                applicationModel = FrameworkModel.defaultModel().newApplication();
                logger.info("Create new application: " + applicationModel.getDesc());
            }

            // 从ApplicationModel获取默认的ModuleModel并绑定到上下文
            moduleModel = applicationModel.getDefaultModule();
            context.setModuleModel(moduleModel);
            logger.info("Use default module model of target application: " + moduleModel.getDesc());
        } else {
            // 使用自定义器设置的ModuleModel
            logger.info("Use module model from customizer: " + moduleModel.getDesc());
        }
        logger.info(
                "Bind " + moduleModel.getDesc() + " to spring container: " + ObjectUtils.identityToString(registry));

        // 将自定义的模块属性设置到ModuleModel中
        Map<String, Object> moduleAttributes = context.getModuleAttributes();
        if (moduleAttributes.size() > 0) {
            moduleModel.getAttributes().putAll(moduleAttributes);
        }

        // 将Dubbo初始化上下文对象注册为Spring容器的单例Bean
        registerContextBeans(beanFactory, context);

        // 标记上下文为已绑定状态，并设置ModuleModel的生命周期由外部管理
        context.markAsBound();
        moduleModel.setLifeCycleManagedExternally(true);

        // 在非AOT模式下，注册Dubbo通用的基础设施Bean
        if (!AotWithSpringDetector.useGeneratedArtifacts()) {
            // register common beans
            DubboBeanUtils.registerCommonBeans(registry);
        }
    }

    private static String safeGetModelDesc(ScopeModel scopeModel) {
        return scopeModel != null ? scopeModel.getDesc() : null;
    }

    private static ConfigurableListableBeanFactory findBeanFactory(BeanDefinitionRegistry registry) {
        ConfigurableListableBeanFactory beanFactory;
        if (registry instanceof ConfigurableListableBeanFactory) {
            beanFactory = (ConfigurableListableBeanFactory) registry;
        } else if (registry instanceof GenericApplicationContext) {
            GenericApplicationContext genericApplicationContext = (GenericApplicationContext) registry;
            beanFactory = genericApplicationContext.getBeanFactory();
        } else {
            throw new IllegalStateException("Can not find Spring BeanFactory from registry: "
                    + registry.getClass().getName());
        }
        return beanFactory;
    }

    private static void registerContextBeans(
            ConfigurableListableBeanFactory beanFactory, DubboSpringInitContext context) {
        // register singleton
        if (!beanFactory.containsSingleton(DubboSpringInitContext.class.getName())) {
            registerSingleton(beanFactory, context);
        }
        if (!beanFactory.containsSingleton(
                context.getApplicationModel().getClass().getName())) {
            registerSingleton(beanFactory, context.getApplicationModel());
        }
        if (!beanFactory.containsSingleton(context.getModuleModel().getClass().getName())) {
            registerSingleton(beanFactory, context.getModuleModel());
        }
    }

    private static void registerSingleton(ConfigurableListableBeanFactory beanFactory, Object bean) {
        beanFactory.registerSingleton(bean.getClass().getName(), bean);
    }

    private static DubboSpringInitContext findContextForApplication(ApplicationModel applicationModel) {
        for (DubboSpringInitContext initializationContext : REGISTRY_CONTEXT_MAP.values()) {
            if (initializationContext.getApplicationModel() == applicationModel) {
                return initializationContext;
            }
        }
        return null;
    }

    /**
     * 自定义Dubbo Spring初始化上下文
     * <p>
     * 该方法通过SPI机制和ThreadLocal方式加载并执行自定义器，允许用户自定义Dubbo Spring容器的初始化行为。
     * 主要流程包括：
     * 1. 通过SPI机制加载所有DubboSpringInitCustomizer实现类并执行自定义逻辑
     * 2. 从ThreadLocal holder中获取自定义器并执行（用于临时注册的自定义器）
     * 3. 清理ThreadLocal中的自定义器，避免内存泄漏
     * <p>
     * 自定义器可以通过{@link DubboSpringInitCustomizer}接口修改绑定的ModuleModel、
     * 设置模块属性等，实现灵活的Dubbo容器配置。
     *
     * @param context Dubbo Spring初始化上下文对象
     */
    private static void customize(DubboSpringInitContext context) {

        // find initialization customizers，通过SPI机制加载所有DubboSpringInitCustomizer实现
        Set<DubboSpringInitCustomizer> customizers = FrameworkModel.defaultModel()
                .getExtensionLoader(DubboSpringInitCustomizer.class)
                .getSupportedExtensionInstances();
        for (DubboSpringInitCustomizer customizer : customizers) {
            customizer.customize(context);
        }

        // load customizers in thread local holder，从ThreadLocal中获取并执行临时自定义器
        DubboSpringInitCustomizerHolder customizerHolder = DubboSpringInitCustomizerHolder.get();
        customizers = customizerHolder.getCustomizers();
        for (DubboSpringInitCustomizer customizer : customizers) {
            customizer.customize(context);
        }
        // 清理ThreadLocal中的自定义器，避免内存泄漏
        customizerHolder.clearCustomizers();
    }

}
