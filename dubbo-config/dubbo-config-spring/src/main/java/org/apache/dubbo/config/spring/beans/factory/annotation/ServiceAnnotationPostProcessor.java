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
import org.apache.dubbo.common.utils.AnnotationUtils;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.Constants;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.config.annotation.Method;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.aot.AotWithSpringDetector;
import org.apache.dubbo.config.spring.context.annotation.DubboClassPathBeanDefinitionScanner;
import org.apache.dubbo.config.spring.schema.AnnotationBeanDefinitionParser;
import org.apache.dubbo.config.spring.util.DubboAnnotationUtils;
import org.apache.dubbo.config.spring.util.ObjectUtils;
import org.apache.dubbo.config.spring.util.SpringCompatUtils;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ConfigurationClassPostProcessor;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.CollectionUtils;

import static java.util.Arrays.asList;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_DUPLICATED_BEAN_DEFINITION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_NO_ANNOTATIONS_FOUND;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_NO_BEANS_SCANNED;
import static org.apache.dubbo.common.utils.AnnotationUtils.filterDefaultValues;
import static org.apache.dubbo.config.spring.beans.factory.annotation.ServiceBeanNameBuilder.create;
import static org.apache.dubbo.config.spring.util.DubboAnnotationUtils.resolveInterfaceName;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.context.annotation.AnnotationConfigUtils.CONFIGURATION_BEAN_NAME_GENERATOR;
import static org.springframework.util.ClassUtils.resolveClassName;

/**
 * A {@link BeanFactoryPostProcessor} used for processing of {@link Service @Service} annotated classes and annotated bean in java config classes.
 * It's also the infrastructure class of XML {@link BeanDefinitionParser} on &lt;dubbo:annotation /&gt;
 *
 * @see AnnotationBeanDefinitionParser
 * @see BeanDefinitionRegistryPostProcessor
 * @since 2.7.7
 */
public class ServiceAnnotationPostProcessor
        implements BeanDefinitionRegistryPostProcessor,
                EnvironmentAware,
                ResourceLoaderAware,
                BeanClassLoaderAware,
                ApplicationContextAware,
                InitializingBean {

    public static final String BEAN_NAME = "dubboServiceAnnotationPostProcessor";

    private static final List<Class<? extends Annotation>> serviceAnnotationTypes = loadServiceAnnotationTypes();

    private static List<Class<? extends Annotation>> loadServiceAnnotationTypes() {
        if (Dubbo2CompactUtils.isEnabled() && Dubbo2CompactUtils.isServiceClassLoaded()) {
            return asList(
                    // @since 2.7.7 Add the @DubboService , the issue : https://github.com/apache/dubbo/issues/6007
                    DubboService.class,
                    // @since 2.7.0 the substitute @com.alibaba.dubbo.config.annotation.Service
                    Service.class,
                    // @since 2.7.3 Add the compatibility for legacy Dubbo's @Service , the issue :
                    // https://github.com/apache/dubbo/issues/4330
                    Dubbo2CompactUtils.getServiceClass());
        } else {
            return asList(
                    // @since 2.7.7 Add the @DubboService , the issue : https://github.com/apache/dubbo/issues/6007
                    DubboService.class,
                    // @since 2.7.0 the substitute @com.alibaba.dubbo.config.annotation.Service
                    Service.class);
        }
    }

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    protected final Set<String> packagesToScan;

    private Set<String> resolvedPackagesToScan;

    private Environment environment;

    private ResourceLoader resourceLoader;

    private ClassLoader classLoader;

    private BeanDefinitionRegistry registry;

    protected ServicePackagesHolder servicePackagesHolder;

    private volatile boolean scanned = false;

    public ServiceAnnotationPostProcessor(String... packagesToScan) {
        this(asList(packagesToScan));
    }

    public ServiceAnnotationPostProcessor(Collection<?> packagesToScan) {
        this.packagesToScan = (Set<String>) packagesToScan.stream().collect(Collectors.toSet());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.resolvedPackagesToScan = resolvePackagesToScan(packagesToScan);
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        this.registry = registry;
        scanServiceBeans(resolvedPackagesToScan, registry);
    }

    /**
     * 处理Bean工厂的后置处理逻辑
     * <p>
     * 该方法在Spring BeanFactory初始化后被调用，主要执行以下操作：
     * 1. 确保BeanDefinitionRegistry已初始化（兼容Spring 3.x）
     * 2. 遍历所有已注册的Bean定义，查找Java配置类中带有@DubboService/@Service注解的@Bean方法
     * 3. 处理这些注解的Bean定义，将其注册为ServiceBean
     * 4. 如果尚未执行过扫描，则执行服务类的包扫描（兼容Spring 3.x）
     *
     * @param beanFactory Spring可配置的列表式Bean工厂
     * @throws BeansException 当处理Bean定义发生异常时抛出
     */
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (this.registry == null) {
            // In spring 3.x, may be not call postProcessBeanDefinitionRegistry()
            this.registry = (BeanDefinitionRegistry) beanFactory;
        }

        // 遍历所有Bean定义，处理Java配置类中@bean方法上的@DubboService注解
        String[] beanNames = beanFactory.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            Map<String, Object> annotationAttributes = getServiceAnnotationAttributes(beanDefinition);
            if (annotationAttributes != null) {
                // process @DubboService at java-config @bean method
                processAnnotatedBeanDefinition(
                        beanName, (AnnotatedBeanDefinition) beanDefinition, annotationAttributes);
            }
        }

        // 如果尚未执行扫描，则在Spring 3.x环境下执行服务类包扫描
        if (!scanned) {
            // In spring 3.x, may be not call postProcessBeanDefinitionRegistry(), so scan service class here
            scanServiceBeans(resolvedPackagesToScan, registry);
        }
    }

    /**
     * 扫描并注册服务Bean
     * <p>
     * 该方法负责扫描指定包路径下所有使用{@link Service}或{@link DubboService}注解的类，
     * 并将它们注册为ServiceBean。主要流程包括：
     * 1. 验证扫描包路径的有效性
     * 2. 配置类路径扫描器，设置注解过滤器
     * 3. 遍历每个包路径进行扫描
     * 4. 避免重复扫描已处理的包
     * 5. 执行扫描并获取Bean定义
     * 6. 处理扫描到的Bean定义并记录扫描状态
     *
     * @param packagesToScan 需要扫描的基础包路径集合
     * @param registry Bean定义注册表，用于注册扫描到的ServiceBean
     */
    private void scanServiceBeans(Set<String> packagesToScan, BeanDefinitionRegistry registry) {

        scanned = true;
        if (CollectionUtils.isEmpty(packagesToScan)) {
            if (logger.isWarnEnabled()) {
                logger.warn(
                        CONFIG_NO_BEANS_SCANNED,
                        "",
                        "",
                        "packagesToScan is empty , ServiceBean registry will be ignored!");
            }
            return;
        }

        // 创建Dubbo专用的类路径Bean定义扫描器
        DubboClassPathBeanDefinitionScanner scanner =
                new DubboClassPathBeanDefinitionScanner(registry, environment, resourceLoader);

        // 解析并设置Bean名称生成器
        BeanNameGenerator beanNameGenerator = resolveBeanNameGenerator(registry);
        scanner.setBeanNameGenerator(beanNameGenerator);

        // 添加@Service、@DubboService等注解类型的包含过滤器
        for (Class<? extends Annotation> annotationType : serviceAnnotationTypes) {
            scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));
        }

        // 添加排除过滤器，避免重复扫描
        ScanExcludeFilter scanExcludeFilter = new ScanExcludeFilter();
        scanner.addExcludeFilter(scanExcludeFilter);

        // 遍历每个包路径进行扫描
        for (String packageToScan : packagesToScan) {

            // 避免重复扫描已经处理过的包
            if (servicePackagesHolder.isPackageScanned(packageToScan)) {
                if (logger.isInfoEnabled()) {
                    logger.info("Ignore package who has already bean scanned: " + packageToScan);
                }
                continue;
            }

            // 在AOT模式下，禁用注解配置包含
            if (AotWithSpringDetector.useGeneratedArtifacts()) {
                scanner.setIncludeAnnotationConfig(false);
            }

            // 执行扫描，注册@Service注解的Bean
            scanner.scan(packageToScan);

            // 查找所有@Service注解的Bean定义，无论是否通过@ComponentScan扫描到
            Set<BeanDefinitionHolder> beanDefinitionHolders =
                    findServiceBeanDefinitionHolders(scanner, packageToScan, registry, beanNameGenerator);

            if (!CollectionUtils.isEmpty(beanDefinitionHolders)) {
                // 记录扫描到的服务类信息
                if (logger.isInfoEnabled()) {
                    List<String> serviceClasses = new ArrayList<>(beanDefinitionHolders.size());
                    for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                        serviceClasses.add(
                                beanDefinitionHolder.getBeanDefinition().getBeanClassName());
                    }
                    logger.info("Found " + beanDefinitionHolders.size()
                            + " classes annotated by Dubbo @Service under package [" + packageToScan + "]: "
                            + serviceClasses);
                }

                // 处理每个扫描到的Bean定义，并记录已扫描的类
                for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                    processScannedBeanDefinition(beanDefinitionHolder);
                    servicePackagesHolder.addScannedClass(
                            beanDefinitionHolder.getBeanDefinition().getBeanClassName());
                }
            } else {
                // 警告：在指定包下未找到Dubbo服务注解
                if (logger.isWarnEnabled()) {
                    logger.warn(
                            CONFIG_NO_ANNOTATIONS_FOUND,
                            "No annotations were found on the class",
                            "",
                            "No class annotated by Dubbo @DubboService or @Service was found under package ["
                                    + packageToScan + "], ignore re-scanned classes: "
                                    + scanExcludeFilter.getExcludedCount());
                }
            }

            // 标记该包已扫描完成
            servicePackagesHolder.addScannedPackage(packageToScan);
        }
    }

    /**
     * It'd be better to use BeanNameGenerator instance that should reference
     * {@link ConfigurationClassPostProcessor#componentScanBeanNameGenerator},
     * thus it maybe a potential problem on bean name generation.
     *
     * @param registry {@link BeanDefinitionRegistry}
     * @return {@link BeanNameGenerator} instance
     * @see SingletonBeanRegistry
     * @see AnnotationConfigUtils#CONFIGURATION_BEAN_NAME_GENERATOR
     * @see ConfigurationClassPostProcessor#processConfigBeanDefinitions
     * @since 2.5.8
     */
    private BeanNameGenerator resolveBeanNameGenerator(BeanDefinitionRegistry registry) {

        BeanNameGenerator beanNameGenerator = null;

        if (registry instanceof SingletonBeanRegistry) {
            SingletonBeanRegistry singletonBeanRegistry = SingletonBeanRegistry.class.cast(registry);
            beanNameGenerator =
                    (BeanNameGenerator) singletonBeanRegistry.getSingleton(CONFIGURATION_BEAN_NAME_GENERATOR);
        }

        if (beanNameGenerator == null) {

            if (logger.isInfoEnabled()) {

                logger.info("BeanNameGenerator bean can't be found in BeanFactory with name ["
                        + CONFIGURATION_BEAN_NAME_GENERATOR + "]");
                logger.info("BeanNameGenerator will be a instance of " + AnnotationBeanNameGenerator.class.getName()
                        + " , it maybe a potential problem on bean name generation.");
            }

            beanNameGenerator = new AnnotationBeanNameGenerator();
        }

        return beanNameGenerator;
    }

    /**
     * Finds a {@link Set} of {@link BeanDefinitionHolder BeanDefinitionHolders} whose bean type annotated
     * {@link Service} Annotation.
     *
     * @param scanner       {@link ClassPathBeanDefinitionScanner}
     * @param packageToScan package to scan
     * @param registry      {@link BeanDefinitionRegistry}
     * @return non-null
     * @since 2.5.8
     */
    private Set<BeanDefinitionHolder> findServiceBeanDefinitionHolders(
            ClassPathBeanDefinitionScanner scanner,
            String packageToScan,
            BeanDefinitionRegistry registry,
            BeanNameGenerator beanNameGenerator) {

        Set<BeanDefinition> beanDefinitions = scanner.findCandidateComponents(packageToScan);

        Set<BeanDefinitionHolder> beanDefinitionHolders = new LinkedHashSet<>(beanDefinitions.size());

        for (BeanDefinition beanDefinition : beanDefinitions) {

            String beanName = beanNameGenerator.generateBeanName(beanDefinition, registry);
            BeanDefinitionHolder beanDefinitionHolder = new BeanDefinitionHolder(beanDefinition, beanName);
            beanDefinitionHolders.add(beanDefinitionHolder);
        }

        return beanDefinitionHolders;
    }

    /**
     * 处理扫描到的服务Bean定义并注册ServiceBean
     * <p>
     * 该方法用于处理通过包扫描发现的带有@DubboService或@Service注解的类，
     * 将其包装并注册为ServiceBean。主要流程包括：
     * 1. 解析Bean的类类型
     * 2. 查找服务注解（@DubboService或@Service）
     * 3. 提取注解属性
     * 4. 解析服务接口名称
     * 5. 生成ServiceBean的名称
     * 6. 构建ServiceBean的BeanDefinition
     * 7. 注册ServiceBean到Spring容器
     *
     * @param beanDefinitionHolder Bean定义持有者，包含扫描到的Bean定义信息
     */
    private void processScannedBeanDefinition(BeanDefinitionHolder beanDefinitionHolder) {

        // 解析获取Bean的实际类类型
        Class<?> beanClass = resolveClass(beanDefinitionHolder);

        // 查找类上的@DubboService或@Service注解
        Annotation service = findServiceAnnotation(beanClass);

        // The attributes of @Service annotation
        Map<String, Object> serviceAnnotationAttributes = AnnotationUtils.getAttributes(service, true);

        // 解析服务接口名称
        String serviceInterface = resolveInterfaceName(serviceAnnotationAttributes, beanClass);

        // 获取原始的服务实现类Bean名称
        String annotatedServiceBeanName = beanDefinitionHolder.getBeanName();

        // 生成ServiceBean的唯一标识名称
        String beanName = generateServiceBeanName(serviceAnnotationAttributes, serviceInterface);

        // 构建ServiceBean的BeanDefinition，包含所有必要的配置属性
        AbstractBeanDefinition serviceBeanDefinition =
                buildServiceBeanDefinition(serviceAnnotationAttributes, serviceInterface, annotatedServiceBeanName);

        // 将ServiceBean注册到Spring容器
        registerServiceBeanDefinition(beanName, serviceBeanDefinition, serviceInterface);
    }

    /**
     * Find the {@link Annotation annotation} of @Service
     *
     * @param beanClass the {@link Class class} of Bean
     * @return <code>null</code> if not found
     * @since 2.7.3
     */
    private Annotation findServiceAnnotation(Class<?> beanClass) {
        return serviceAnnotationTypes.stream()
                .map(annotationType -> ClassUtils.isPresent(
                                        "org.springframework.core.annotation.AnnotatedElementUtils",
                                        Thread.currentThread().getContextClassLoader())
                                && ReflectUtils.hasMethod(
                                        org.springframework.core.annotation.AnnotatedElementUtils.class,
                                        "findMergedAnnotation")
                        ? org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation(
                                beanClass, annotationType)
                        : org.apache.dubbo.common.utils.AnnotationUtils.findAnnotation(beanClass, annotationType))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Generates the bean name of {@link ServiceBean}
     *
     * @param serviceAnnotationAttributes
     * @param serviceInterface            the class of interface annotated {@link Service}
     * @return ServiceBean@interfaceClassName#annotatedServiceBeanName
     * @since 2.7.3
     */
    private String generateServiceBeanName(Map<String, Object> serviceAnnotationAttributes, String serviceInterface) {
        ServiceBeanNameBuilder builder = create(serviceInterface, environment)
                .group((String) serviceAnnotationAttributes.get("group"))
                .version((String) serviceAnnotationAttributes.get("version"));
        return builder.build();
    }

    private Class<?> resolveClass(BeanDefinitionHolder beanDefinitionHolder) {

        BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinition();

        return resolveClass(beanDefinition);
    }

    private Class<?> resolveClass(BeanDefinition beanDefinition) {

        String beanClassName = beanDefinition.getBeanClassName();

        return resolveClassName(beanClassName, classLoader);
    }

    private Set<String> resolvePackagesToScan(Set<String> packagesToScan) {
        Set<String> resolvedPackagesToScan = new LinkedHashSet<>(packagesToScan.size());
        for (String packageToScan : packagesToScan) {
            if (StringUtils.hasText(packageToScan)) {
                String resolvedPackageToScan = environment.resolvePlaceholders(packageToScan.trim());
                resolvedPackagesToScan.add(resolvedPackageToScan);
            }
        }
        return resolvedPackagesToScan;
    }

    /**
     * 构建ServiceBean的BeanDefinition
     * <p>
     * 该方法负责根据@Service或@DubboService注解的属性构建ServiceBean的BeanDefinition。
     * 主要配置项包括：
     * 1. 设置自动装配模式为构造函数装配
     * 2. 适配注解属性到Bean属性（排除需要特殊处理的属性）
     * 3. 设置服务引用（ref）指向实际的服务实现类Bean
     * 4. 配置服务接口名称
     * 5. 转换并配置参数、方法配置等
     * 6. 处理注册中心、协议等配置的ID引用
     * 7. 配置监控、模块、线程池等可选引用
     *
     * @param serviceAnnotationAttributes @Service或@DubboService注解的属性集合
     * @param serviceInterface 服务接口的全限定名
     * @param refServiceBeanName 服务实现类Bean的名称，用于ref属性引用
     * @return 构建完成的AbstractBeanDefinition对象
     * @since 2.7.3
     */
    private AbstractBeanDefinition buildServiceBeanDefinition(
            Map<String, Object> serviceAnnotationAttributes, String serviceInterface, String refServiceBeanName) {

        // 创建ServiceBean的BeanDefinition构建器
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceBean.class);

        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
        // 设置自动装配模式为构造函数装配
        beanDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);

        MutablePropertyValues propertyValues = beanDefinition.getPropertyValues();

        // 定义需要特殊处理的属性名列表，这些属性不直接映射到Bean属性
        String[] ignoreAttributeNames = ObjectUtils.of(
                "provider",
                "monitor",
                "application",
                "module",
                "registry",
                "protocol",
                "methods",
                "interfaceName",
                "parameters",
                "executor");

        // 将注解属性适配并添加到Bean属性中（排除需要特殊处理的属性）
        propertyValues.addPropertyValues(
                new AnnotationPropertyValuesAdapter(serviceAnnotationAttributes, environment, ignoreAttributeNames));

        // References "ref" property to annotated-@Service Bean，指向实际的服务实现类
        addPropertyReference(builder, "ref", refServiceBeanName);

        // Set interface，配置服务接口名称
        builder.addPropertyValue("interface", serviceInterface);

        // Convert parameters into map，转换参数数组为Map格式
        builder.addPropertyValue("parameters", DubboAnnotationUtils.convertParameters((String[])
                serviceAnnotationAttributes.get("parameters")));

        // Add methods parameters，处理方法级别的配置
        List<MethodConfig> methodConfigs = convertMethodConfigs(serviceAnnotationAttributes.get("methods"));
        if (!methodConfigs.isEmpty()) {
            if (AotWithSpringDetector.isAotProcessing()) {
                // AOT模式下，将方法配置转换为JSON字符串
                List<String> methodsJson = new ArrayList<>();
                methodConfigs.forEach(methodConfig -> methodsJson.add(JsonUtils.toJson(methodConfig)));
                builder.addPropertyValue("methodsJson", methodsJson);
            } else {
                // 常规模式，直接使用MethodConfig对象
                builder.addPropertyValue("methods", methodConfigs);
            }
        }

        // convert provider to providerIds，转换提供者配置ID
        String providerConfigId = (String) serviceAnnotationAttributes.get("provider");
        if (StringUtils.hasText(providerConfigId)) {
            addPropertyValue(builder, "providerIds", providerConfigId);
        }

        // Convert registry[] to registryIds，转换注册中心配置ID数组为逗号分隔的字符串
        String[] registryConfigIds = (String[]) serviceAnnotationAttributes.get("registry");
        if (registryConfigIds != null && registryConfigIds.length > 0) {
            resolveStringArray(registryConfigIds);
            builder.addPropertyValue("registryIds", StringUtils.join(registryConfigIds, ','));
        }

        // Convert protocol[] to protocolIds，转换协议配置ID数组为逗号分隔的字符串
        String[] protocolConfigIds = (String[]) serviceAnnotationAttributes.get("protocol");
        if (protocolConfigIds != null && protocolConfigIds.length > 0) {
            resolveStringArray(protocolConfigIds);
            builder.addPropertyValue("protocolIds", StringUtils.join(protocolConfigIds, ','));
        }

        // monitor reference，配置监控中心引用
        String monitorConfigId = (String) serviceAnnotationAttributes.get("monitor");
        if (StringUtils.hasText(monitorConfigId)) {
            addPropertyReference(builder, "monitor", monitorConfigId);
        }

        // module reference，配置模块引用
        String moduleConfigId = (String) serviceAnnotationAttributes.get("module");
        if (StringUtils.hasText(moduleConfigId)) {
            addPropertyReference(builder, "module", moduleConfigId);
        }

        // executor reference，配置线程池引用
        String executorBeanName = (String) serviceAnnotationAttributes.get("executor");
        if (StringUtils.hasText(executorBeanName)) {
            addPropertyReference(builder, "executor", executorBeanName);
        }

        // service bean definition should not be lazy，ServiceBean不应该懒加载
        builder.setLazyInit(false);

        return builder.getBeanDefinition();
    }

    private String[] resolveStringArray(String[] strs) {
        if (strs == null) {
            return null;
        }
        for (int i = 0; i < strs.length; i++) {
            strs[i] = environment.resolvePlaceholders(strs[i]);
        }
        return strs;
    }

    private List convertMethodConfigs(Object methodsAnnotation) {
        if (methodsAnnotation == null) {
            return Collections.EMPTY_LIST;
        }
        return MethodConfig.constructMethodConfig((Method[]) methodsAnnotation);
    }

    private void addPropertyReference(BeanDefinitionBuilder builder, String propertyName, String beanName) {
        String resolvedBeanName = environment.resolvePlaceholders(beanName);
        builder.addPropertyReference(propertyName, resolvedBeanName);
    }

    private void addPropertyValue(BeanDefinitionBuilder builder, String propertyName, String value) {
        String resolvedBeanName = environment.resolvePlaceholders(value);
        builder.addPropertyValue(propertyName, resolvedBeanName);
    }

    /**
     * 获取Java配置@Bean方法上的Dubbo服务注解属性
     * <p>
     * 该方法用于从Java配置类的@Bean方法中提取Dubbo服务注解（@DubboService或@Service）的属性。
     * 主要处理以下场景：
     * <pre>{@code
     * @Configuration
     * public class ProviderConfig {
     *     @Bean
     *     @DubboService(group="demo", version="1.2.3")
     *     public DemoService demoService() {
     *         return new DemoServiceImpl();
     *     }
     * }
     * }</pre>
     *
     * @param beanDefinition Bean定义对象，应该是AnnotatedBeanDefinition类型
     * @return 如果找到Dubbo服务注解，返回其属性Map；否则返回null
     */
    private Map<String, Object> getServiceAnnotationAttributes(BeanDefinition beanDefinition) {
        if (beanDefinition instanceof AnnotatedBeanDefinition) {
            AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition) beanDefinition;
            MethodMetadata factoryMethodMetadata = SpringCompatUtils.getFactoryMethodMetadata(annotatedBeanDefinition);
            if (factoryMethodMetadata != null) {
                // 遍历所有Dubbo服务注解类型（@DubboService、@Service等）
                for (Class<? extends Annotation> annotationType : serviceAnnotationTypes) {
                    if (factoryMethodMetadata.isAnnotated(annotationType.getName())) {
                        // 获取注解属性，兼容Spring 4.x和5.2+版本
                        Map<String, Object> annotationAttributes =
                                factoryMethodMetadata.getAnnotationAttributes(annotationType.getName());
                        // 过滤掉注解的默认值，只保留显式设置的属性
                        return filterDefaultValues(annotationType, annotationAttributes);
                    }
                }
            }
        }
        return null;
    }

    /**
     * 处理Java配置类中@Bean方法上的@DubboService注解
     * <p>
     * 该方法用于处理Spring Java配置方式定义的Dubbo服务，典型场景如下：
     * <pre>{@code
     * @Configuration
     * public class ProviderConfig {
     *     @Bean
     *     @DubboService(group="demo", version="1.2.3")
     *     public DemoService demoService() {
     *         return new DemoServiceImpl();
     *     }
     * }
     * }</pre>
     * <p>
     * 主要处理流程：
     * 1. 提取@Service注解的属性
     * 2. 从@Bean方法的返回类型获取服务实现类
     * 3. 解析服务接口名称
     * 4. 生成ServiceBean的名称
     * 5. 构建ServiceBean的BeanDefinition
     * 6. 注册ServiceBean到Spring容器
     *
     * @param refServiceBeanName @Bean方法创建的服务实现类Bean的名称
     * @param refServiceBeanDefinition 带有@DubboService注解的Bean定义
     * @param attributes @DubboService注解的属性集合
     */
    private void processAnnotatedBeanDefinition(
            String refServiceBeanName,
            AnnotatedBeanDefinition refServiceBeanDefinition,
            Map<String, Object> attributes) {

        // 复制注解属性，避免修改原始数据
        Map<String, Object> serviceAnnotationAttributes = new LinkedHashMap<>(attributes);

        // 从@Bean方法的返回类型获取服务实现类
        String returnTypeName = SpringCompatUtils.getFactoryMethodReturnType(refServiceBeanDefinition);
        Class<?> beanClass = resolveClassName(returnTypeName, classLoader);

        // 解析服务接口名称
        String serviceInterface = resolveInterfaceName(serviceAnnotationAttributes, beanClass);

        // 生成ServiceBean的唯一标识名称
        String serviceBeanName = generateServiceBeanName(serviceAnnotationAttributes, serviceInterface);

        // 构建ServiceBean的BeanDefinition，包含所有必要的配置属性
        AbstractBeanDefinition serviceBeanDefinition =
                buildServiceBeanDefinition(serviceAnnotationAttributes, serviceInterface, refServiceBeanName);

        // 设置ServiceBean的ID属性
        serviceBeanDefinition.getPropertyValues().add(Constants.ID, serviceBeanName);

        // 将ServiceBean注册到Spring容器
        registerServiceBeanDefinition(serviceBeanName, serviceBeanDefinition, serviceInterface);
    }

    private void registerServiceBeanDefinition(
            String serviceBeanName, AbstractBeanDefinition serviceBeanDefinition, String serviceInterface) {
        // check service bean
        if (registry.containsBeanDefinition(serviceBeanName)) {
            BeanDefinition existingDefinition = registry.getBeanDefinition(serviceBeanName);
            if (existingDefinition.equals(serviceBeanDefinition)) {
                // exist equipment bean definition
                return;
            }

            String msg = "Found duplicated BeanDefinition of service interface [" + serviceInterface
                    + "] with bean name [" + serviceBeanName + "], existing definition [ " + existingDefinition
                    + "], new definition [" + serviceBeanDefinition + "]";
            logger.error(CONFIG_DUPLICATED_BEAN_DEFINITION, "", "", msg);
            throw new BeanDefinitionStoreException(
                    serviceBeanDefinition.getResourceDescription(), serviceBeanName, msg);
        }

        registry.registerBeanDefinition(serviceBeanName, serviceBeanDefinition);
        if (logger.isInfoEnabled()) {
            logger.info("Register ServiceBean[" + serviceBeanName + "]: " + serviceBeanDefinition);
        }
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.servicePackagesHolder =
                applicationContext.getBean(ServicePackagesHolder.BEAN_NAME, ServicePackagesHolder.class);
    }

    private class ScanExcludeFilter implements TypeFilter {

        private int excludedCount;

        @Override
        public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory)
                throws IOException {
            String className = metadataReader.getClassMetadata().getClassName();
            boolean excluded = servicePackagesHolder.isClassScanned(className);
            if (excluded) {
                excludedCount++;
            }
            return excluded;
        }

        public int getExcludedCount() {
            return excludedCount;
        }
    }
}
