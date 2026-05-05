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
package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor;
import org.apache.dubbo.config.spring.beans.factory.annotation.ServiceAnnotationPostProcessor;
import org.apache.dubbo.config.spring.context.DubboSpringInitializer;
import org.apache.dubbo.config.spring.util.SpringCompatUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * Dubbo {@link DubboComponentScan} Bean Registrar
 *
 * @see Service
 * @see DubboComponentScan
 * @see ImportBeanDefinitionRegistrar
 * @see ServiceAnnotationPostProcessor
 * @see ReferenceAnnotationBeanPostProcessor
 * @since 2.5.7
 */
public class DubboComponentScanRegistrar implements ImportBeanDefinitionRegistrar {

    /**
     * 注册Dubbo相关的Bean定义
     * <p>
     * 该方法在Spring导入Bean定义时被调用，主要执行以下操作：
     * 1. 初始化Dubbo Spring容器
     * 2. 获取需要扫描的包路径
     * 3. 注册服务注解后置处理器以处理@Service注解
     *
     * @param importingClassMetadata 导入类的注解元数据，用于获取@DubboComponentScan或@EnableDubbo的配置信息
     * @param registry Bean定义注册表，用于注册Dubbo相关的Bean定义
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {

        // initialize dubbo beans
        DubboSpringInitializer.initialize(registry);

        Set<String> packagesToScan = getPackagesToScan(importingClassMetadata);

        registerServiceAnnotationPostProcessor(packagesToScan, registry);
    }

    /**
     * 注册服务注解后置处理器
     * <p>
     * 该方法负责创建并注册{@link ServiceAnnotationPostProcessor} Bean，用于处理Dubbo的@Service注解。
     * 主要步骤包括：
     * 1. 构建BeanDefinition，指定ServiceAnnotationPostProcessor的类类型
     * 2. 设置构造函数参数（要扫描的包路径）
     * 3. 标记为基础设施Bean（ROLE_INFRASTRUCTURE）
     * 4. 使用生成的名称注册到BeanDefinitionRegistry
     *
     * @param packagesToScan 需要扫描的包路径集合，不包含占位符解析
     * @param registry Bean定义注册表，用于注册ServiceAnnotationPostProcessor
     */
    private void registerServiceAnnotationPostProcessor(Set<String> packagesToScan, BeanDefinitionRegistry registry) {

        // 构建ServiceAnnotationPostProcessor的BeanDefinition
        BeanDefinitionBuilder builder = rootBeanDefinition(SpringCompatUtils.serviceAnnotationPostProcessor());
        builder.addConstructorArgValue(packagesToScan);
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();

        // 使用生成的名称注册Bean定义
        BeanDefinitionReaderUtils.registerWithGeneratedName(beanDefinition, registry);
    }

    /**
     * 获取需要扫描的包路径集合
     * <p>
     * 该方法按照以下优先级获取包扫描路径：
     * 1. 从@DubboComponentScan注解的basePackages或basePackageClasses属性获取
     * 2. 如果未找到，则从@EnableDubbo注解的scanBasePackages或scanBasePackageClasses属性获取（兼容Spring 3.x）
     * 3. 如果仍未找到，则默认使用导入类所在的包路径
     *
     * @param metadata Spring注解元数据，用于获取注解配置信息
     * @return 需要扫描的包路径集合，不会返回null
     */
    private Set<String> getPackagesToScan(AnnotationMetadata metadata) {
        // get from @DubboComponentScan
        Set<String> packagesToScan =
                getPackagesToScan0(metadata, DubboComponentScan.class, "basePackages", "basePackageClasses");

        // get from @EnableDubbo, compatible with spring 3.x
        if (packagesToScan.isEmpty()) {
            packagesToScan =
                    getPackagesToScan0(metadata, EnableDubbo.class, "scanBasePackages", "scanBasePackageClasses");
        }

        // 如果未配置任何包路径，则默认使用导入类所在的包
        if (packagesToScan.isEmpty()) {
            return Collections.singleton(ClassUtils.getPackageName(metadata.getClassName()));
        }
        return packagesToScan;
    }

    private Set<String> getPackagesToScan0(
            AnnotationMetadata metadata,
            Class annotationClass,
            String basePackagesName,
            String basePackageClassesName) {

        AnnotationAttributes attributes =
                AnnotationAttributes.fromMap(metadata.getAnnotationAttributes(annotationClass.getName()));
        if (attributes == null) {
            return Collections.emptySet();
        }

        Set<String> packagesToScan = new LinkedHashSet<>();
        // basePackages
        String[] basePackages = attributes.getStringArray(basePackagesName);
        packagesToScan.addAll(Arrays.asList(basePackages));
        // basePackageClasses
        Class<?>[] basePackageClasses = attributes.getClassArray(basePackageClassesName);
        for (Class<?> basePackageClass : basePackageClasses) {
            packagesToScan.add(ClassUtils.getPackageName(basePackageClass));
        }
        // value
        if (attributes.containsKey("value")) {
            String[] value = attributes.getStringArray("value");
            packagesToScan.addAll(Arrays.asList(value));
        }
        return packagesToScan;
    }
}
