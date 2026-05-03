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

import org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor;
import org.apache.dubbo.config.spring.util.DubboBeanUtils;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

/**
 * Register some infrastructure beans if not exists.
 * This post-processor MUST impl BeanDefinitionRegistryPostProcessor,
 * in order to enable the registered BeanFactoryPostProcessor bean to be loaded and executed.
 *
 * @see org.springframework.context.support.PostProcessorRegistrationDelegate#invokeBeanFactoryPostProcessors(
 *org.springframework.beans.factory.config.ConfigurableListableBeanFactory, java.util.List)
 */
public class DubboInfraBeanRegisterPostProcessor implements BeanDefinitionRegistryPostProcessor {

    /**
     * The bean name of {@link ReferenceAnnotationBeanPostProcessor}
     */
    public static final String BEAN_NAME = "dubboInfraBeanRegisterPostProcessor";

    private BeanDefinitionRegistry registry;

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        this.registry = registry;
    }

    /**
     * 在Spring容器标准初始化后处理BeanFactory，注册Dubbo基础设施Bean。
     * <p>
     * 该方法的处理逻辑：
     * 1. 检查registry是否为空（兼容Spring 3.2.x版本的初始化顺序问题）；
     * 2. 提前注册ReferenceAnnotationBeanPostProcessor，确保它能在PropertySourcesPlaceholderConfigurer之前处理@DubboReference注解；
     * 3. 如果BeanFactory中不存在PropertySourcesPlaceholderConfigurer，则注册一个用于解析占位符；
     * 4. 从BeanDefinition注册表中移除自身，避免循环引用和内存泄漏。
     * </p>
     *
     * @param beanFactory Spring容器的BeanFactory实例，用于管理和获取Bean
     * @throws BeansException 当Bean操作失败时抛出异常
     */
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

        // 在Spring 3.2.x版本中，registry可能为null，因为postProcessBeanDefinitionRegistry方法可能在postProcessBeanFactory之后调用
        if (registry != null) {
            // 提前注册ReferenceAnnotationBeanPostProcessor，确保它能在PropertySourcesPlaceholderConfigurer/PropertyPlaceholderConfigurer之前处理早期初始化的ReferenceBean
            // 这样可以在占位符解析之前处理@DubboReference注解，支持URL中包含占位符的场景
            ReferenceAnnotationBeanPostProcessor referenceAnnotationBeanPostProcessor = beanFactory.getBean(
                    ReferenceAnnotationBeanPostProcessor.BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
            beanFactory.addBeanPostProcessor(referenceAnnotationBeanPostProcessor);

            // 如果BeanFactory中不存在PropertySourcesPlaceholderConfigurer，则注册一个用于解析${...}占位符
            DubboBeanUtils.registerPlaceholderConfigurerBeanIfNotExists(beanFactory, registry);
        }

        // 修复 https://github.com/apache/dubbo/issues/10278：从注册表中移除自身Bean定义，防止循环引用和内存泄漏
        if (registry != null) {
            registry.removeBeanDefinition(BEAN_NAME);
        }
    }

}
