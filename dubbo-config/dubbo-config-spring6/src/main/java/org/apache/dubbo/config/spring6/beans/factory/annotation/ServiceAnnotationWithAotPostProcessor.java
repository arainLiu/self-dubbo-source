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
package org.apache.dubbo.config.spring6.beans.factory.annotation;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.beans.factory.annotation.ServiceAnnotationPostProcessor;
import org.apache.dubbo.config.spring.schema.AnnotationBeanDefinitionParser;
import org.apache.dubbo.config.spring6.utils.AotUtils;

import java.util.Collection;

import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.TypeReference;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.aot.BeanRegistrationCode;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 * The purpose of implementing {@link BeanRegistrationAotProcessor} is to
 * supplement for {@link ServiceAnnotationPostProcessor} ability of AOT.
 *
 * @see AnnotationBeanDefinitionParser
 * @see BeanDefinitionRegistryPostProcessor
 * @since 3.3
 */

/**
 * 支持AOT（Ahead-of-Time）编译的服务注解后置处理器
 * <p>
 * 该类继承自{@link ServiceAnnotationPostProcessor}，并实现{@link BeanRegistrationAotProcessor}接口，
 * 用于在Spring AOT编译阶段为Dubbo服务Bean生成必要的运行时提示（Runtime Hints），
 * 包括反射访问和序列化支持，以确保Dubbo服务在GraalVM原生镜像中正常工作。
 *
 * @see AnnotationBeanDefinitionParser
 * @see BeanDefinitionRegistryPostProcessor
 * @since 3.3
 */
public class ServiceAnnotationWithAotPostProcessor extends ServiceAnnotationPostProcessor
        implements BeanRegistrationAotProcessor {

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    /**
     * 构造函数，指定要扫描的包路径
     *
     * @param packagesToScan 需要扫描的包路径数组
     */
    public ServiceAnnotationWithAotPostProcessor(String... packagesToScan) {
        super(packagesToScan);
    }

    /**
     * 构造函数，指定要扫描的包路径集合
     *
     * @param packagesToScan 需要扫描的包路径集合
     */
    public ServiceAnnotationWithAotPostProcessor(Collection<?> packagesToScan) {
        super(packagesToScan);
    }

    /**
     * 在AOT编译阶段处理Bean注册，为Dubbo服务生成运行时提示
     * <p>
     * 该方法会检查Bean类型：
     * 1. 如果是ServiceBean，则从Bean定义中获取接口名称并创建AOT贡献
     * 2. 如果是已扫描的服务类，则直接为其创建AOT贡献
     * 3. 其他情况返回null
     *
     * @param registeredBean 已注册的Bean信息
     * @return AOT贡献对象，用于生成运行时提示；如果不需要AOT处理则返回null
     */
    @Override
    public BeanRegistrationAotContribution processAheadOfTime(RegisteredBean registeredBean) {
        Class<?> beanClass = registeredBean.getBeanClass();
        if (beanClass.equals(ServiceBean.class)) {
            // 处理ServiceBean类型，从Bean定义中提取接口信息
            RootBeanDefinition beanDefinition = registeredBean.getMergedBeanDefinition();
            String interfaceName = (String) beanDefinition.getPropertyValues().get("interface");
            try {
                Class<?> c = Class.forName(interfaceName);
                return new DubboServiceBeanRegistrationAotContribution(c);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (servicePackagesHolder.isClassScanned(beanClass.getName())) {
            // 处理已扫描的服务类
            return new DubboServiceBeanRegistrationAotContribution(beanClass);
        }

        return null;
    }

    /**
     * Dubbo服务Bean的AOT贡献实现类
     * <p>
     * 负责在AOT编译阶段为服务接口类注册反射访问权限和序列化支持
     */
    private static class DubboServiceBeanRegistrationAotContribution implements BeanRegistrationAotContribution {

        private final Class<?> cl;

        /**
         * 构造函数
         *
         * @param cl 需要注册AOT提示的类
         */
        public DubboServiceBeanRegistrationAotContribution(Class<?> cl) {
            this.cl = cl;
        }

        /**
         * 将AOT提示应用到运行时配置中
         * <p>
         * 为服务类注册：
         * 1. 反射访问公共方法的权限
         * 2. 序列化支持（通过AotUtils）
         *
         * @param generationContext AOT生成上下文
         * @param beanRegistrationCode Bean注册代码生成器
         */
        @Override
        public void applyTo(GenerationContext generationContext, BeanRegistrationCode beanRegistrationCode) {
            // 注册反射访问权限，允许调用公共方法
            generationContext
                    .getRuntimeHints()
                    .reflection()
                    .registerType(TypeReference.of(cl), MemberCategory.INVOKE_PUBLIC_METHODS);
            // 注册序列化支持
            AotUtils.registerSerializationForService(cl, generationContext.getRuntimeHints());
        }
    }

}
