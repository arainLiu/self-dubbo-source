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
package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.common.compact.Dubbo2CompactUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.spring.Constants;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.aot.AotWithSpringDetector;
import org.apache.dubbo.config.spring.context.event.DubboConfigInitEvent;
import org.apache.dubbo.config.spring.reference.ReferenceAttributes;
import org.apache.dubbo.config.spring.reference.ReferenceBeanManager;
import org.apache.dubbo.config.spring.reference.ReferenceBeanSupport;
import org.apache.dubbo.config.spring.util.AnnotationUtils;
import org.apache.dubbo.config.spring.util.SpringCompatUtils;
import org.apache.dubbo.rpc.service.GenericService;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Member;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.InjectionMetadata;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.MethodMetadata;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_DUBBO_BEAN_INITIALIZER;
import static org.apache.dubbo.common.utils.AnnotationUtils.filterDefaultValues;
import static org.springframework.util.StringUtils.hasText;

/**
 * <p>
 * Step 1:
 * The purpose of implementing {@link BeanFactoryPostProcessor} is to scan the registration reference bean definition earlier,
 * so that it can be shared with the xml bean configuration.
 * </p>
 *
 * <p>
 * Step 2:
 * By implementing {@link org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor},
 * inject the reference bean instance into the fields and setter methods which annotated with {@link DubboReference}.
 * </p>
 *
 * @see DubboReference
 * @see Reference
 * @see com.alibaba.dubbo.config.annotation.Reference
 * @since 2.5.7
 */
public class ReferenceAnnotationBeanPostProcessor extends AbstractAnnotationBeanPostProcessor
        implements ApplicationContextAware, BeanFactoryPostProcessor {

    /**
     * The bean name of {@link ReferenceAnnotationBeanPostProcessor}
     */
    public static final String BEAN_NAME = ReferenceAnnotationBeanPostProcessor.class.getName();

    /**
     * Cache size
     */
    private static final int CACHE_SIZE = Integer.getInteger(BEAN_NAME + ".cache.size", 32);

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private final ConcurrentMap<InjectionMetadata.InjectedElement, String> injectedFieldReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private final ConcurrentMap<InjectionMetadata.InjectedElement, String> injectedMethodReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    protected ApplicationContext applicationContext;

    protected ReferenceBeanManager referenceBeanManager;
    protected BeanDefinitionRegistry beanDefinitionRegistry;

    /**
     * {@link com.alibaba.dubbo.config.annotation.Reference @com.alibaba.dubbo.config.annotation.Reference} has been supported since 2.7.3
     * <p>
     * {@link DubboReference @DubboReference} has been supported since 2.7.7
     */
    public ReferenceAnnotationBeanPostProcessor() {
        super(loadAnnotationTypes());
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Annotation>[] loadAnnotationTypes() {
        if (Dubbo2CompactUtils.isEnabled() && Dubbo2CompactUtils.isReferenceClassLoaded()) {
            return (Class<? extends Annotation>[])
                    new Class<?>[] {DubboReference.class, Reference.class, Dubbo2CompactUtils.getReferenceClass()};
        } else {
            return (Class<? extends Annotation>[]) new Class<?>[] {DubboReference.class, Reference.class};
        }
    }

    /**
     * 处理Spring Bean工厂中的所有@DubboReference注解，完成依赖注入的准备工作。
     * <p>
     * 该方法是Spring Bean生命周期回调接口BeanFactoryPostProcessor的核心实现，在Spring容器初始化Bean定义后、实例化Bean之前执行。
     * 负责扫描所有Bean定义，查找带有@DubboReference注解的字段和方法，预创建ReferenceConfig并缓存，加速后续的实际注入过程。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>遍历Bean定义</b>：获取beanFactory中所有Bean的名称数组，逐个检查是否需要处理@DubboReference注解</li>
     *   <li><b>FactoryBean特殊处理</b>：
     *     <ul>
     *       <li>如果是ReferenceBean类型（通过XML或Java Config定义的<dubbo:reference>），跳过不处理，避免重复注入</li>
     *       <li>如果是带有@DubboReference注解的@Bean方法返回类型，调用processReferenceAnnotatedBeanDefinition处理Java Config场景</li>
     *       <li>其他FactoryBean则解析其beanClassName获取实际类型</li>
     *     </ul>
     *   </li>
     *   <li><b>查找注入元数据</b>：调用findInjectionMetadata反射扫描类结构，提取所有@DubboReference标注的字段和方法，构建AnnotatedInjectionMetadata对象</li>
     *   <li><b>准备注入</b>：调用prepareInjection为每个@DubboReference创建ReferenceConfig实例，建立serviceKey到ReferenceConfig的映射关系，但不立即建立RPC连接（延迟到首次get时）</li>
     *   <li><b>自我清理</b>：如果当前处理器已注册为BeanPostProcessor，从beanDefinitionRegistry中移除自身定义，避免被Spring再次注册导致BeanPostProcessorChecker误报</li>
     *   <li><b>发布初始化事件</b>：触发DubboConfigInitEvent通知监听器Dubbo配置已加载完成，低版本Spring（<4.2）不支持早期事件时记录警告日志</li>
     * </ol>
     * </p>
     *
     * @param beanFactory Spring的Bean工厂对象，包含所有Bean定义和依赖关系，用于扫描和预处理@DubboReference注解
     * @throws BeansException 当Bean定义解析失败、类加载异常或ReferenceConfig创建出错时抛出Spring容器异常
     */
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

        String[] beanNames = beanFactory.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Class<?> beanType;
            if (beanFactory.isFactoryBean(beanName)) {
                BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
                if (isReferenceBean(beanDefinition)) {
                    continue;
                }
                if (isAnnotatedReferenceBean(beanDefinition)) {
                    /*
                     * 处理Java Config中的@DubboReference：
                     * 支持在@Configuration类的@Bean方法参数上使用@DubboReference注解
                     */
                    // process @DubboReference at java-config @bean method
                    processReferenceAnnotatedBeanDefinition(beanName, (AnnotatedBeanDefinition) beanDefinition);
                    continue;
                }

                String beanClassName = beanDefinition.getBeanClassName();
                beanType = ClassUtils.resolveClass(beanClassName, getClassLoader());
            } else {
                beanType = beanFactory.getType(beanName);
            }
            if (beanType != null) {
                AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, beanType, null);
                try {
                    /*
                     * 准备Dubbo引用注入：
                     * 为每个@DubboReference创建ReferenceConfig并缓存，但此时不建立RPC连接
                     */
                    prepareInjection(metadata);
                } catch (BeansException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException("Prepare dubbo reference injection element failed", e);
                }
            }
        }

        if (beanFactory instanceof AbstractBeanFactory) {
            List<BeanPostProcessor> beanPostProcessors = ((AbstractBeanFactory) beanFactory).getBeanPostProcessors();
            for (BeanPostProcessor beanPostProcessor : beanPostProcessors) {
                if (beanPostProcessor == this) {
                    /*
                     * 自我清理：
                     * 当前处理器已通过DubboInfraBeanRegisterPostProcessor注册为BeanPostProcessor，
                     * 此处移除Bean定义防止Spring将其再次注册为普通Bean导致检测错误
                     */
                    // This bean has been registered as BeanPostProcessor at
                    // org.apache.dubbo.config.spring.context.DubboInfraBeanRegisterPostProcessor.postProcessBeanFactory()
                    // so destroy this bean here, prevent register it as BeanPostProcessor again, avoid cause
                    // BeanPostProcessorChecker detection error
                    beanDefinitionRegistry.removeBeanDefinition(BEAN_NAME);
                    break;
                }
            }
        }

        try {
            /*
             * 发布Dubbo配置初始化事件：
             * 通知监听器Dubbo配置已加载完成，可以开始执行依赖于此的初始化逻辑
             */
            // this is an early event, it will be notified at
            // org.springframework.context.support.AbstractApplicationContext.registerListeners()
            applicationContext.publishEvent(new DubboConfigInitEvent(applicationContext));
        } catch (Exception e) {
            // if spring version is less than 4.2, it does not support early application event
            logger.warn(
                    CONFIG_DUBBO_BEAN_INITIALIZER,
                    "",
                    "",
                    "publish early application event failed, please upgrade spring version to 4.2.x or later: " + e);
        }
    }

    // ... existing code ...


    /**
     * check whether is @DubboReference at java-config @bean method
     */
    private boolean isAnnotatedReferenceBean(BeanDefinition beanDefinition) {
        if (beanDefinition instanceof AnnotatedBeanDefinition) {
            AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition) beanDefinition;
            String beanClassName = SpringCompatUtils.getFactoryMethodReturnType(annotatedBeanDefinition);
            if (beanClassName != null && ReferenceBean.class.getName().equals(beanClassName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * process @DubboReference at java-config @bean method
     * <pre class="code">
     * &#064;Configuration
     * public class ConsumerConfig {
     *
     *      &#064;Bean
     *      &#064;DubboReference(group="demo", version="1.2.3")
     *      public ReferenceBean&lt;DemoService&gt; demoService() {
     *          return new ReferenceBean();
     *      }
     *
     * }
     * </pre>
     *
     * @param beanName
     * @param beanDefinition
     */
    protected void processReferenceAnnotatedBeanDefinition(String beanName, AnnotatedBeanDefinition beanDefinition) {

        MethodMetadata factoryMethodMetadata = SpringCompatUtils.getFactoryMethodMetadata(beanDefinition);

        // Extract beanClass from generic return type of java-config bean method: ReferenceBean<DemoService>
        // see
        // org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.getTypeForFactoryBeanFromMethod
        Class beanClass = getBeanFactory().getType(beanName);
        if (beanClass == Object.class) {
            beanClass = SpringCompatUtils.getGenericTypeOfReturnType(factoryMethodMetadata);
        }
        if (beanClass == Object.class) {
            // bean class is invalid, ignore it
            return;
        }

        if (beanClass == null) {
            String beanMethodSignature =
                    factoryMethodMetadata.getDeclaringClassName() + "#" + factoryMethodMetadata.getMethodName() + "()";
            throw new BeanCreationException(
                    "The ReferenceBean is missing necessary generic type, which returned by the @Bean method of Java-config class. "
                            + "The generic type of the returned ReferenceBean must be specified as the referenced interface type, "
                            + "such as ReferenceBean<DemoService>. Please check bean method: "
                            + beanMethodSignature);
        }

        // get dubbo reference annotation attributes
        Map<String, Object> annotationAttributes = null;
        // try all dubbo reference annotation types
        for (Class<? extends Annotation> annotationType : getAnnotationTypes()) {
            if (factoryMethodMetadata.isAnnotated(annotationType.getName())) {
                // Since Spring 5.2
                // return factoryMethodMetadata.getAnnotations().get(annotationType).filterDefaultValues().asMap();
                // Compatible with Spring 4.x
                annotationAttributes = factoryMethodMetadata.getAnnotationAttributes(annotationType.getName());
                annotationAttributes = filterDefaultValues(annotationType, annotationAttributes);
                break;
            }
        }

        if (annotationAttributes != null) {
            // @DubboReference on @Bean method
            LinkedHashMap<String, Object> attributes = new LinkedHashMap<>(annotationAttributes);
            // reset id attribute
            attributes.put(ReferenceAttributes.ID, beanName);
            // convert annotation props
            ReferenceBeanSupport.convertReferenceProps(attributes, beanClass);

            // get interface
            String interfaceName = (String) attributes.get(ReferenceAttributes.INTERFACE);

            // check beanClass and reference interface class
            if (!StringUtils.isEquals(interfaceName, beanClass.getName()) && beanClass != GenericService.class) {
                String beanMethodSignature = factoryMethodMetadata.getDeclaringClassName() + "#"
                        + factoryMethodMetadata.getMethodName() + "()";
                throw new BeanCreationException(
                        "The 'interfaceClass' or 'interfaceName' attribute value of @DubboReference annotation "
                                + "is inconsistent with the generic type of the ReferenceBean returned by the bean method. "
                                + "The interface class of @DubboReference is: "
                                + interfaceName + ", but return ReferenceBean<" + beanClass.getName() + ">. "
                                + "Please remove the 'interfaceClass' and 'interfaceName' attributes from @DubboReference annotation. "
                                + "Please check bean method: "
                                + beanMethodSignature);
            }

            Class interfaceClass = beanClass;

            // set attribute instead of property values
            beanDefinition.setAttribute(Constants.REFERENCE_PROPS, attributes);
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, interfaceClass);
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, interfaceName);
        } else {
            // raw reference bean
            // the ReferenceBean is not yet initialized
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, beanClass);
            if (beanClass != GenericService.class) {
                beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, beanClass.getName());
            }
        }

        // set id
        beanDefinition.getPropertyValues().add(ReferenceAttributes.ID, beanName);
    }

    @Override
    public void postProcessMergedBeanDefinition(RootBeanDefinition beanDefinition, Class<?> beanType, String beanName) {
        if (beanType != null) {
            if (isReferenceBean(beanDefinition)) {
                // mark property value as optional
                List<PropertyValue> propertyValues =
                        beanDefinition.getPropertyValues().getPropertyValueList();
                for (PropertyValue propertyValue : propertyValues) {
                    propertyValue.setOptional(true);
                }
            } else if (isAnnotatedReferenceBean(beanDefinition)) {
                // extract beanClass from java-config bean method generic return type: ReferenceBean<DemoService>
                // Class beanClass = getBeanFactory().getType(beanName);
            } else {
                AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, beanType, null);
                metadata.checkConfigMembers(beanDefinition);
                try {
                    prepareInjection(metadata);
                } catch (Exception e) {
                    throw new IllegalStateException("Prepare dubbo reference injection element failed", e);
                }
            }
        }
    }

    /**
     * Alternatives to the {@link #postProcessProperties(PropertyValues, Object, String)}, that removed as of Spring
     * Framework 6.0.0, and in favor of {@link #postProcessProperties(PropertyValues, Object, String)}.
     * <p>In order to be compatible with the lower version of Spring, it is still retained.
     *
     * @see #postProcessProperties
     */
    public PropertyValues postProcessPropertyValues(
            PropertyValues pvs, PropertyDescriptor[] pds, Object bean, String beanName) throws BeansException {
        return postProcessProperties(pvs, bean, beanName);
    }

    /**
     * Alternatives to the {@link #postProcessPropertyValues(PropertyValues, PropertyDescriptor[], Object, String)}.
     *
     * @see #postProcessPropertyValues
     */
    @Override
    public PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName)
            throws BeansException {
        try {
            AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, bean.getClass(), pvs);
            prepareInjection(metadata);
            metadata.inject(bean, beanName, pvs);
        } catch (BeansException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new BeanCreationException(
                    beanName, "Injection of @" + getAnnotationType().getSimpleName() + " dependencies is failed", ex);
        }
        return pvs;
    }

    private boolean isReferenceBean(BeanDefinition beanDefinition) {
        return ReferenceBean.class.getName().equals(beanDefinition.getBeanClassName());
    }

    /**
     * 预处理@DubboReference注解的注入元数据，注册ReferenceBean并缓存关联关系。
     * <p>
     * 该方法是Dubbo Spring集成中依赖注入准备阶段的核心逻辑，负责扫描类中的所有@DubboReference标注字段和方法，
     * 为每个引用创建唯一的ReferenceBean定义，建立注入点与Bean名称的映射关系，加速后续的实际注入过程。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>处理字段注入</b>：
     *     <ul>
     *       <li>遍历metadata.getFieldElements()获取所有@DubboReference标注的字段</li>
     *       <li>跳过已处理的字段（injectedObject != null），保证幂等性</li>
     *       <li>提取字段类型作为服务接口，从注解属性中获取group、version、timeout等配置</li>
     *       <li>调用registerReferenceBean创建或复用ReferenceBean，返回唯一的Bean名称</li>
     *       <li>将referenceBeanName赋值给fieldElement.injectedObject建立关联，同时存入injectedFieldReferenceBeanCache缓存</li>
     *     </ul>
     *   </li>
     *   <li><b>处理方法注入</b>：
     *     <ul>
     *       <li>遍历metadata.getMethodElements()获取所有@DubboReference标注的setter方法</li>
     *       <li>跳过已处理的方法，提取方法参数类型作为服务接口</li>
     *       <li>调用registerReferenceBean创建ReferenceBean，处理方式级别的注解属性合并</li>
     *       <li>将referenceBeanName赋值给methodElement.injectedObject，存入injectedMethodReferenceBeanCache缓存</li>
     *     </ul>
     *   </li>
     *   <li><b>异常处理</b>：捕获ClassNotFoundException（如服务接口类不存在），包装为BeanCreationException抛出，中断Spring容器启动</li>
     * </ol>
     * </p>
     *
     * @param metadata 注解注入元数据对象，包含目标类的所有@DubboReference字段和方法信息，由findInjectionMetadata通过反射扫描生成
     * @throws BeansException 当服务接口类加载失败、ReferenceBean注册异常或配置冲突时抛出Spring容器异常
     */
    protected void prepareInjection(AnnotatedInjectionMetadata metadata) throws BeansException {
        try {
            /*
             * 处理字段级别的@DubboReference注入：
             * 为每个字段注册ReferenceBean并建立双向关联
             */
            for (AnnotatedFieldElement fieldElement : metadata.getFieldElements()) {
                if (fieldElement.injectedObject != null) {
                    continue;
                }
                Class<?> injectedType = fieldElement.field.getType();
                AnnotationAttributes attributes = fieldElement.attributes;
                String referenceBeanName = registerReferenceBean(
                        fieldElement.getPropertyName(), injectedType, attributes, fieldElement.field);

                // associate fieldElement and reference bean
                fieldElement.injectedObject = referenceBeanName;
                injectedFieldReferenceBeanCache.put(fieldElement, referenceBeanName);
            }

            /*
             * 处理方法级别的@DubboReference注入：
             * 支持setter方法注入，处理方式参数的注解属性
             */
            for (AnnotatedMethodElement methodElement : metadata.getMethodElements()) {
                if (methodElement.injectedObject != null) {
                    continue;
                }
                Class<?> injectedType = methodElement.getInjectedType();
                AnnotationAttributes attributes = methodElement.attributes;
                // register reference bean
                String referenceBeanName = registerReferenceBean(methodElement.getPropertyName(), injectedType, attributes, methodElement.method);

                // associate methodElement and reference bean
                methodElement.injectedObject = referenceBeanName;
                injectedMethodReferenceBeanCache.put(methodElement, referenceBeanName);
            }
        } catch (ClassNotFoundException e) {
            throw new BeanCreationException("prepare reference annotation failed", e);
        }
    }

    /**
     * 注册ReferenceBean定义到Spring容器，处理命名冲突和重复注册逻辑。
     * <p>
     * 该方法是Dubbo Spring集成中Bean注册的核心逻辑，负责将@DubboReference注解转换为Spring的BeanDefinition并注册到容器。
     * 支持自动重命名解决冲突、相同配置复用、别名映射等高级特性，确保每个唯一的接口+版本+分组组合只创建一个ReferenceBean实例。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>确定Bean名称</b>：
     *     <ul>
     *       <li>优先使用注解中指定的id属性作为Bean名称，此时不允许自动重命名</li>
     *       <li>如果未指定id，使用字段名或方法参数名作为默认Bean名称，允许后续冲突时自动重命名</li>
     *     </ul>
     *   </li>
     *   <li><b>属性转换</b>：调用ReferenceBeanSupport.convertReferenceProps将注解属性转换为ReferenceConfig所需的配置格式，推断接口名</li>
     *   <li><b>接口名校验</b>：检查interfaceName是否为空，泛化调用时必须显式指定interfaceName或interfaceClass</li>
     *   <li><b>生成引用键</b>：调用ReferenceBeanSupport.generateReferenceKey生成唯一标识（格式：interface:version@group），用于判断是否为重复配置</li>
     *   <li><b>查找已注册Bean</b>：通过referenceBeanManager.getBeanNamesByKey(referenceKey)查询是否已有相同配置的ReferenceBean</li>
     *   <li><b>冲突检测与处理</b>：
     *     <ul>
     *       <li>如果当前Bean名称已被占用且类型不同：
     *         <ul>
     *           <li>不可重命名场景（指定了id或Java Config Bean）：抛出BeanCreationException异常，要求用户手动修改名称</li>
     *           <li>可重命名场景：自动追加"#2"、"#3"等后缀直到找到可用名称，记录WARN日志提醒用户</li>
     *         </ul>
     *       </li>
     *       <li>如果referenceKey已存在但Bean名称不同：注册别名指向已存在的Bean，避免重复创建</li>
     *     </ul>
     *   </li>
     *   <li><b>创建BeanDefinition</b>：
     *     <ul>
     *       <li>创建RootBeanDefinition，设置beanClass为ReferenceBean.class</li>
     *       <li>将注解属性存储到BeanDefinition的attribute中（而非propertyValues），避免提前实例化</li>
     *       <li>设置decoratedDefinition为接口类型的GenericBeanDefinition，用于Spring AOT处理时的类型推断</li>
     *       <li>设置OBJECT_TYPE_ATTRIBUTE为接口Class，解决Spring 5.2+的FactoryBean类型推断问题</li>
     *     </ul>
     *   </li>
     *   <li><b>注册到容器</b>：调用beanDefinitionRegistry.registerBeanDefinition将BeanDefinition注册到Spring容器，同时更新referenceBeanManager的索引映射</li>
     *   <li><b>记录日志</b>：输出INFO日志记录注册的Bean名称、引用键和注入位置，便于调试和问题排查</li>
     * </ol>
     * </p>
     *
     * @param propertyName 属性名称，取自字段名或setter方法参数名，用作默认的Bean名称
     * @param injectedType 注入点的类型，即服务接口的Class对象，决定ReferenceBean的泛型类型
     * @param attributes   注解属性Map，包含@DubboReference的所有配置项（如group、version、timeout、retries等）
     * @param member       反射成员对象，可以是Field或Method，用于定位注解位置和错误提示
     * @return 最终确定的ReferenceBean名称，可能与传入的propertyName不同（发生自动重命名时）
     * @throws BeanCreationException 当接口名缺失、Bean名称冲突且不可重命名、或配置转换失败时抛出创建异常
     */
    public String registerReferenceBean(
            String propertyName, Class<?> injectedType, Map<String, Object> attributes, Member member)
            throws BeansException {

        boolean renameable = true;
        // referenceBeanName
        String referenceBeanName = AnnotationUtils.getAttribute(attributes, ReferenceAttributes.ID);
        if (hasText(referenceBeanName)) {
            renameable = false;
        } else {
            referenceBeanName = propertyName;
        }

        String checkLocation = "Please check " + member.toString();

        /*
         * 转换注解属性：
         * 将@DubboReference的属性转换为ReferenceConfig所需的配置格式，推断接口名
         */
        // convert annotation props
        ReferenceBeanSupport.convertReferenceProps(attributes, injectedType);

        // get interface
        String interfaceName = (String) attributes.get(ReferenceAttributes.INTERFACE);
        if (StringUtils.isBlank(interfaceName)) {
            throw new BeanCreationException(
                    "Need to specify the 'interfaceName' or 'interfaceClass' attribute of '@DubboReference' if enable generic. "
                            + checkLocation);
        }

        /*
         * 生成引用键：
         * 格式为 interface:version@group，用于唯一标识一个Dubbo服务引用
         */
        String referenceKey = ReferenceBeanSupport.generateReferenceKey(attributes, applicationContext);

        /*
         * 查找已注册的相同配置的Bean：
         * 如果已有相同referenceKey的Bean，直接复用或注册别名
         */
        List<String> registeredReferenceBeanNames = referenceBeanManager.getBeanNamesByKey(referenceKey);
        if (registeredReferenceBeanNames.size() > 0) {
            // found same name and reference key
            if (registeredReferenceBeanNames.contains(referenceBeanName)) {
                return referenceBeanName;
            }
        }

        /*
         * 检查Bean名称冲突：
         * 如果当前名称已被占用，需要判断是重用、重命名还是报错
         */
        boolean isContains;
        if ((isContains = beanDefinitionRegistry.containsBeanDefinition(referenceBeanName))
                || beanDefinitionRegistry.isAlias(referenceBeanName)) {
            String preReferenceBeanName = referenceBeanName;
            if (!isContains) {
                // Look in the alias for the origin bean name
                String[] aliases = beanDefinitionRegistry.getAliases(referenceBeanName);
                if (ArrayUtils.isNotEmpty(aliases)) {
                    for (String alias : aliases) {
                        if (beanDefinitionRegistry.containsBeanDefinition(alias)) {
                            preReferenceBeanName = alias;
                            break;
                        }
                    }
                }
            }
            BeanDefinition prevBeanDefinition = beanDefinitionRegistry.getBeanDefinition(preReferenceBeanName);
            String prevBeanType = prevBeanDefinition.getBeanClassName();
            String prevBeanDesc = referenceBeanName + "[" + prevBeanType + "]";
            String newBeanDesc = referenceBeanName + "[" + referenceKey + "]";

            if (isReferenceBean(prevBeanDefinition)) {
                // check reference key
                String prevReferenceKey =
                        ReferenceBeanSupport.generateReferenceKey(prevBeanDefinition, applicationContext);
                if (StringUtils.isEquals(prevReferenceKey, referenceKey)) {
                    /*
                     * 相同配置的Bean已存在：
                     * 直接返回现有Bean名称，避免重复注册
                     */
                    // found matched dubbo reference bean, ignore register
                    return referenceBeanName;
                }
                // get interfaceName from attribute
                Assert.notNull(prevBeanDefinition, "The interface class of ReferenceBean is not initialized");
                prevBeanDesc = referenceBeanName + "[" + prevReferenceKey + "]";
            }

            /*
             * 处理名称冲突：
             * 如果不可重命名则抛异常，否则自动生成带后缀的新名称
             */
            // bean name from attribute 'id' or java-config bean, cannot be renamed
            if (!renameable) {
                throw new BeanCreationException(
                        "Already exists another bean definition with the same bean name [" + referenceBeanName + "], "
                                + "but cannot rename the reference bean name (specify the id attribute or java-config bean), "
                                + "please modify the name of one of the beans: "
                                + "prev: "
                                + prevBeanDesc + ", new: " + newBeanDesc + ". " + checkLocation);
            }

            // the prev bean type is different, rename the new reference bean
            int index = 2;
            String newReferenceBeanName = null;
            while (newReferenceBeanName == null
                    || beanDefinitionRegistry.containsBeanDefinition(newReferenceBeanName)
                    || beanDefinitionRegistry.isAlias(newReferenceBeanName)) {
                newReferenceBeanName = referenceBeanName + "#" + index;
                index++;
                // double check found same name and reference key
                if (registeredReferenceBeanNames.contains(newReferenceBeanName)) {
                    return newReferenceBeanName;
                }
            }
            newBeanDesc = newReferenceBeanName + "[" + referenceKey + "]";

            logger.warn(
                    CONFIG_DUBBO_BEAN_INITIALIZER,
                    "",
                    "",
                    "Already exists another bean definition with the same bean name [" + referenceBeanName + "], "
                            + "rename dubbo reference bean to ["
                            + newReferenceBeanName + "]. "
                            + "It is recommended to modify the name of one of the beans to avoid injection problems. "
                            + "prev: "
                            + prevBeanDesc + ", new: " + newBeanDesc + ". " + checkLocation);
            referenceBeanName = newReferenceBeanName;
        }
        attributes.put(ReferenceAttributes.ID, referenceBeanName);

        /*
         * 注册别名：
         * 如果已有相同配置的Bean，仅注册别名指向现有Bean，避免重复创建
         */
        if (registeredReferenceBeanNames.size() > 0) {
            beanDefinitionRegistry.registerAlias(registeredReferenceBeanNames.get(0), referenceBeanName);
            referenceBeanManager.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);
            return referenceBeanName;
        }

        Class interfaceClass = injectedType;

        /*
         * 创建BeanDefinition：
         * 设置ReferenceBean的类型、属性和装饰定义，准备注册到Spring容器
         */
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClassName(ReferenceBean.class.getName());
        beanDefinition.getPropertyValues().add(ReferenceAttributes.ID, referenceBeanName);

        // set attribute instead of property values
        beanDefinition.setAttribute(Constants.REFERENCE_PROPS, attributes);
        beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, interfaceClass);
        beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, interfaceName);

        beanDefinition.getPropertyValues().add(ReferenceAttributes.INTERFACE_CLASS, interfaceClass);
        beanDefinition.getPropertyValues().add(ReferenceAttributes.INTERFACE_NAME, interfaceName);

        if (AotWithSpringDetector.isAotProcessing()) {
            beanDefinition.getPropertyValues().add("referencePropsJson", JsonUtils.toJson(attributes));
        }
        /*
         * 设置装饰定义：
         * 用于Spring AOT处理时的类型推断，避免获取ReferenceBean类型时提前实例化
         */
        GenericBeanDefinition targetDefinition = new GenericBeanDefinition();
        targetDefinition.setBeanClass(interfaceClass);
        beanDefinition.setDecoratedDefinition(
                new BeanDefinitionHolder(targetDefinition, referenceBeanName + "_decorated"));

        // signal object type since Spring 5.2
        beanDefinition.setAttribute(Constants.OBJECT_TYPE_ATTRIBUTE, interfaceClass);

        beanDefinitionRegistry.registerBeanDefinition(referenceBeanName, beanDefinition);
        referenceBeanManager.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);
        logger.info("Register dubbo reference bean: " + referenceBeanName + " = " + referenceKey + " at " + member);
        return referenceBeanName;
    }

    @Override
    protected Object doGetInjectedBean(
            AnnotationAttributes attributes,
            Object bean,
            String beanName,
            Class<?> injectedType,
            AnnotatedInjectElement injectedElement)
            throws Exception {

        if (injectedElement.injectedObject == null) {
            throw new IllegalStateException(
                    "The AnnotatedInjectElement of @DubboReference should be inited before injection");
        }

        return getBeanFactory().getBean((String) injectedElement.injectedObject);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        this.referenceBeanManager =
                applicationContext.getBean(ReferenceBeanManager.BEAN_NAME, ReferenceBeanManager.class);
        this.beanDefinitionRegistry = (BeanDefinitionRegistry) applicationContext.getAutowireCapableBeanFactory();
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        this.injectedFieldReferenceBeanCache.clear();
        this.injectedMethodReferenceBeanCache.clear();
    }

    /**
     * Gets all beans of {@link ReferenceBean}
     *
     * @deprecated use {@link ReferenceBeanManager#getReferences()} instead
     */
    @Deprecated
    public Collection<ReferenceBean<?>> getReferenceBeans() {
        return Collections.emptyList();
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected field.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedFieldReferenceBeanMap() {
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> map = new HashMap<>();
        for (Map.Entry<InjectionMetadata.InjectedElement, String> entry : injectedFieldReferenceBeanCache.entrySet()) {
            map.put(entry.getKey(), referenceBeanManager.getById(entry.getValue()));
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected method.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedMethodReferenceBeanMap() {
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> map = new HashMap<>();
        for (Map.Entry<InjectionMetadata.InjectedElement, String> entry : injectedMethodReferenceBeanCache.entrySet()) {
            map.put(entry.getKey(), referenceBeanManager.getById(entry.getValue()));
        }
        return Collections.unmodifiableMap(map);
    }
}
