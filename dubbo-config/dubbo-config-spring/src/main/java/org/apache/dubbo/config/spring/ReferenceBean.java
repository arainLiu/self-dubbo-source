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
package org.apache.dubbo.config.spring;

import org.apache.dubbo.common.aot.NativeDetector;
import org.apache.dubbo.common.bytecode.Proxy;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.spring.aot.AotWithSpringDetector;
import org.apache.dubbo.config.spring.context.DubboConfigApplicationListener;
import org.apache.dubbo.config.spring.context.DubboConfigBeanInitializer;
import org.apache.dubbo.config.spring.reference.ReferenceAttributes;
import org.apache.dubbo.config.spring.reference.ReferenceBeanManager;
import org.apache.dubbo.config.spring.reference.ReferenceBeanSupport;
import org.apache.dubbo.config.spring.schema.DubboBeanDefinitionParser;
import org.apache.dubbo.config.spring.util.LazyTargetInvocationHandler;
import org.apache.dubbo.config.spring.util.LazyTargetSource;
import org.apache.dubbo.config.spring.util.LockUtils;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.rpc.proxy.AbstractProxyFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_DUBBO_BEAN_INITIALIZER;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED;

/**
 * <p>
 * Spring FactoryBean for {@link ReferenceConfig}.
 * </p>
 *
 *
 * <p></p>
 * Step 1: Register ReferenceBean in Java-config class:
 * <pre class="code">
 * &#64;Configuration
 * public class ReferenceConfiguration {
 *     &#64;Bean
 *     &#64;DubboReference(group = "demo")
 *     public ReferenceBean&lt;HelloService&gt; helloService() {
 *         return new ReferenceBean();
 *     }
 *
 *     // As GenericService
 *     &#64;Bean
 *     &#64;DubboReference(group = "demo", interfaceClass = HelloService.class)
 *     public ReferenceBean&lt;GenericService&gt; genericHelloService() {
 *         return new ReferenceBean();
 *     }
 * }
 * </pre>
 * <p>
 * Or register ReferenceBean in xml:
 * <pre class="code">
 * &lt;dubbo:reference id="helloService" interface="org.apache.dubbo.config.spring.api.HelloService"/&gt;
 * &lt;!-- As GenericService --&gt;
 * &lt;dubbo:reference id="genericHelloService" interface="org.apache.dubbo.config.spring.api.HelloService" generic="true"/&gt;
 * </pre>
 * <p>
 * Step 2: Inject ReferenceBean by @Autowired
 * <pre class="code">
 * public class FooController {
 *     &#64;Autowired
 *     private HelloService helloService;
 *
 *     &#64;Autowired
 *     private GenericService genericHelloService;
 * }
 * </pre>
 *
 * @see org.apache.dubbo.config.annotation.DubboReference
 * @see org.apache.dubbo.config.spring.reference.ReferenceBeanBuilder
 */
public class ReferenceBean<T>
        implements FactoryBean<T>,
                ApplicationContextAware,
                BeanClassLoaderAware,
                BeanNameAware,
                InitializingBean,
                DisposableBean {
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());
    private transient ApplicationContext applicationContext;

    private ClassLoader beanClassLoader;

    // lazy proxy of reference
    private Object lazyProxy;

    // beanName
    protected String id;

    // reference key
    private String key;

    /**
     * The interface class of the reference service
     */
    private Class<?> interfaceClass;

    /*
     * remote service interface class name
     */
    // 'interfaceName' field for compatible with seata-1.4.0:
    // io.seata.rm.tcc.remoting.parser.DubboRemotingParser#getServiceDesc()
    private String interfaceName;

    // proxy style
    private String proxy;

    // from annotation attributes
    private Map<String, Object> referenceProps;

    // from xml bean definition
    private MutablePropertyValues propertyValues;

    // actual reference config
    private volatile ReferenceConfig referenceConfig;

    // ReferenceBeanManager
    private ReferenceBeanManager referenceBeanManager;

    // Registration sources of this reference, may be xml file or annotation location
    private List<Map<String, Object>> sources = new ArrayList<>();

    public ReferenceBean() {
        super();
    }

    public ReferenceBean(Map<String, Object> referenceProps) {
        this.referenceProps = referenceProps;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.beanClassLoader = classLoader;
    }

    @Override
    public void setBeanName(String name) {
        this.setId(name);
    }

    /**
     * Create bean instance.
     *
     * <p></p>
     * Why we need a lazy proxy?
     * <p>
     * <p/>
     * When Spring searches beans by type, if Spring cannot determine the type of a factory bean, it may try to initialize it.
     * The ReferenceBean is also a FactoryBean.
     * <br/>
     * (This has already been resolved by decorating the BeanDefinition: {@link DubboBeanDefinitionParser#configReferenceBean})
     * <p>
     * <p/>
     * In addition, if some ReferenceBeans are dependent on beans that are initialized very early,
     * and dubbo config beans are not ready yet, there will be many unexpected problems if initializing the dubbo reference immediately.
     * <p>
     * <p/>
     * When it is initialized, only a lazy proxy object will be created,
     * and dubbo reference-related resources will not be initialized.
     * <br/>
     * In this way, the influence of Spring is eliminated, and the dubbo configuration initialization is controllable.
     *
     * @see DubboConfigBeanInitializer
     * @see ReferenceBeanManager#initReferenceBean(ReferenceBean)
     * @see DubboBeanDefinitionParser#configReferenceBean
     */
    /**
     * 获取引用服务的代理对象
     * <p>
     * 该方法是Spring FactoryBean接口的实现，用于返回Dubbo服务的代理对象。
     * 采用懒加载策略：
     * 1. 首次调用时创建懒加载代理对象
     * 2. 后续调用直接返回已创建的代理对象
     * <p>
     * 懒加载代理的设计使得服务引用在实际被使用时才真正初始化，
     * 避免了启动时立即建立所有远程连接，提升了应用启动速度。
     *
     * @return 服务接口的代理对象
     * @throws Exception 当创建代理对象失败时抛出异常
     */
    @Override
    public T getObject() {
        if (lazyProxy == null) {
            createLazyProxy();
        }
        return (T) lazyProxy;
    }

    @Override
    public Class<?> getObjectType() {
        return getInterfaceClass();
    }

    @Override
    @Parameter(excluded = true)
    public boolean isSingleton() {
        return true;
    }

    /**
     * 在Spring Bean属性设置完成后进行初始化
     * <p>
     * 该方法是Spring InitializingBean接口的实现，在Bean的所有属性设置完成后被调用。
     * 主要执行以下初始化逻辑：
     * 1. 获取BeanFactory和当前Bean的定义信息
     * 2. 从Bean定义中提取接口类和接口名称（区分AOT模式和非AOT模式）
     * 3. 根据不同的配置方式（@DubboReference注解、Java Config、XML）提取引用属性
     * 4. 验证必要的属性（接口类、接口名称）已正确初始化
     * 5. 获取ReferenceBeanManager并注册当前ReferenceBean
     * <p>
     * 该方法支持多种配置方式的兼容处理：
     * - @DubboReference注解在Java Config类的@Bean方法上
     * - @DubboReference注解在字段或setter方法上
     * - XML配置的reference bean
     *
     * @throws Exception 当初始化失败时抛出异常
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        ConfigurableListableBeanFactory beanFactory = getBeanFactory();

        // 预初始化XML reference bean或@DubboReference注解配置，验证Bean ID不为空
        Assert.notEmptyString(getId(), "The id of ReferenceBean cannot be empty");
        BeanDefinition beanDefinition = beanFactory.getBeanDefinition(getId());

        // 根据是否使用AOT模式，从不同位置获取接口类和接口名称
        if (AotWithSpringDetector.useGeneratedArtifacts()) {
            this.interfaceClass =
                    (Class<?>) beanDefinition.getPropertyValues().get(ReferenceAttributes.INTERFACE_CLASS);
            this.interfaceName = (String) beanDefinition.getPropertyValues().get(ReferenceAttributes.INTERFACE_NAME);

        } else {
            this.interfaceClass = (Class<?>) beanDefinition.getAttribute(ReferenceAttributes.INTERFACE_CLASS);
            this.interfaceName = (String) beanDefinition.getAttribute(ReferenceAttributes.INTERFACE_NAME);
        }
        Assert.notNull(this.interfaceClass, "The interface class of ReferenceBean is not initialized");

        // 根据不同的配置方式提取引用属性
        if (beanDefinition.hasAttribute(Constants.REFERENCE_PROPS)) {
            // @DubboReference annotation at java-config class @Bean method
            // @DubboReference annotation at reference field or setter method
            referenceProps = (Map<String, Object>) beanDefinition.getAttribute(Constants.REFERENCE_PROPS);
        } else {
            if (beanDefinition instanceof AnnotatedBeanDefinition) {
                // Return ReferenceBean in java-config class @Bean method
                if (referenceProps == null) {
                    referenceProps = new LinkedHashMap<>();
                }
                ReferenceBeanSupport.convertReferenceProps(referenceProps, interfaceClass);
                if (this.interfaceName == null) {
                    this.interfaceName = (String) referenceProps.get(ReferenceAttributes.INTERFACE);
                }
            } else {
                // xml reference bean，XML配置方式获取属性值
                propertyValues = beanDefinition.getPropertyValues();
            }
        }

        // 提取代理方式配置
        if (referenceProps != null) {
            this.proxy = (String) referenceProps.get(ReferenceAttributes.PROXY);
        }
        Assert.notNull(this.interfaceName, "The interface name of ReferenceBean is not initialized");

        // 获取ReferenceBeanManager并注册当前ReferenceBean
        this.referenceBeanManager = beanFactory.getBean(ReferenceBeanManager.BEAN_NAME, ReferenceBeanManager.class);
        referenceBeanManager.addReference(this);
    }

    private ConfigurableListableBeanFactory getBeanFactory() {
        return (ConfigurableListableBeanFactory) applicationContext.getAutowireCapableBeanFactory();
    }

    @Override
    public void destroy() {
        // do nothing
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The interface of this ReferenceBean, for injection purpose
     *
     * @return
     */
    public Class<?> getInterfaceClass() {
        // Compatible with seata-1.4.0: io.seata.rm.tcc.remoting.parser.DubboRemotingParser#getServiceDesc()
        return interfaceClass;
    }

    /**
     * The interface of remote service
     */
    public String getServiceInterface() {
        return interfaceName;
    }

    /**
     * The group of the service
     */
    public String getGroup() {
        // Compatible with seata-1.4.0: io.seata.rm.tcc.remoting.parser.DubboRemotingParser#getServiceDesc()
        return referenceConfig.getGroup();
    }

    /**
     * The version of the service
     */
    public String getVersion() {
        // Compatible with seata-1.4.0: io.seata.rm.tcc.remoting.parser.DubboRemotingParser#getServiceDesc()
        return referenceConfig.getVersion();
    }

    public String getKey() {
        return key;
    }

    public Map<String, Object> getReferenceProps() {
        return referenceProps;
    }

    public MutablePropertyValues getPropertyValues() {
        return propertyValues;
    }

    public ReferenceConfig getReferenceConfig() {
        return referenceConfig;
    }

    public void setKeyAndReferenceConfig(String key, ReferenceConfig referenceConfig) {
        this.key = key;
        this.referenceConfig = referenceConfig;
    }

    /**
     * 创建懒加载代理对象，延迟初始化RPC调用器直到首次方法调用。
     * <p>
     * 该方法是Dubbo Spring集成中懒加载机制的核心实现，通过动态代理技术创建一个轻量级占位符对象。
     * 在Spring容器启动阶段不建立网络连接，仅在业务代码首次调用代理方法时才触发init()流程，显著加快应用启动速度。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>收集代理接口</b>：
     *     <ul>
     *       <li>必须包含interfaceClass（服务接口），确保代理对象可以赋值给目标字段类型</li>
     *       <li>添加内部接口（如EchoService、Destroyable等），支持Dubbo框架的内置功能</li>
     *       <li>如果interfaceName与interfaceClass不同名（IDL场景或跨语言调用），尝试加载并添加实际的服务接口Class</li>
     *     </ul>
     *   </li>
     *   <li><b>Native Image特殊处理</b>：检测是否运行在GraalVM Native Image环境，如果是则强制使用JDK动态代理（因为字节码生成受限）</li>
     *   <li><b>Javassist优先策略</b>：如果lazyProxy为空且未指定proxy属性或使用默认值，优先尝试Javassist生成代理（性能优于JDK动态代理）</li>
     *   <li><b>JDK动态代理降级</b>：如果Javassist生成失败或不满足条件，回退到JDK原生动态代理作为兜底方案</li>
     * </ol>
     * </p>
     * <p>
     * 代理生成策略选择：
     * <ul>
     *   <li><b>Javassist</b>：通过字节码生成技术创建代理类，方法调用直接转发，性能接近硬编码，但需要额外的内存存储生成的Class</li>
     *   <li><b>JDK动态代理</b>：基于Java反射机制，无需生成额外Class文件，在Native Image环境下是唯一选择，但每次调用有轻微性能损耗</li>
     * </ul>
     * </p>
     */
    private void createLazyProxy() {

        /*
         * 收集需要代理的接口列表：
         * 包括服务接口、Dubbo内部接口（EchoService等），确保代理对象具备完整的功能
         */
        // set proxy interfaces
        // see also: org.apache.dubbo.rpc.proxy.AbstractProxyFactory.getProxy(org.apache.dubbo.rpc.Invoker<T>, boolean)
        List<Class<?>> interfaces = new ArrayList<>();
        interfaces.add(interfaceClass);
        Class<?>[] internalInterfaces = AbstractProxyFactory.getInternalInterfaces();
        Collections.addAll(interfaces, internalInterfaces);
        if (!StringUtils.isEquals(interfaceClass.getName(), interfaceName)) {
            /*
             * 添加实际的服务接口：
             * 处理IDL场景下interfaceName与interfaceClass不一致的情况
             */
            // add service interface
            try {
                Class<?> serviceInterface = ClassUtils.forName(interfaceName, beanClassLoader);
                interfaces.add(serviceInterface);
            } catch (ClassNotFoundException e) {
                // generic call maybe without service interface class locally
            }
        }

        if (NativeDetector.inNativeImage()) {
            /*
             * Native Image环境：
             * GraalVM不支持运行时字节码生成，强制使用JDK动态代理
             */
            generateFromJdk(interfaces);
        }

        if (this.lazyProxy == null
                && (StringUtils.isEmpty(this.proxy) || CommonConstants.DEFAULT_PROXY.equalsIgnoreCase(this.proxy))) {
            /*
             * 优先使用Javassist：
             * Javassist生成的代理性能更好，适合常规JVM环境
             */
            generateFromJavassistFirst(interfaces);
        }

        if (this.lazyProxy == null) {
            /*
             * JDK动态代理降级：
             * 当Javassist不可用或生成失败时，使用JDK原生代理作为兜底
             */
            generateFromJdk(interfaces);
        }
    }

    /**
     * 优先使用Javassist生成动态代理，失败时自动降级到JDK动态代理。
     * <p>
     * 该方法是Dubbo懒加载代理的核心生成逻辑，采用"Javassist优先+JDK降级"的双重保障策略。
     * Javassist生成的代理通过字节码直接调用，性能优于JDK反射代理；但在某些特殊环境（如模块化系统、安全限制）下可能失败，
     * 此时自动回退到JDK动态代理确保功能可用性。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>Javassist代理生成</b>：
     *     <ul>
     *       <li>调用Proxy.getProxy传入所有接口数组，生成动态代理类</li>
     *       <li>通过newInstance创建代理实例，传入LazyTargetInvocationHandler作为方法调用处理器</li>
     *       <li>DubboReferenceLazyInitTargetSource封装了延迟初始化逻辑，首次调用时才触发ReferenceBean.init()建立RPC连接</li>
     *     </ul>
     *   </li>
     *   <li><b>Javassist失败降级</b>：
     *     <ul>
     *       <li>捕获Throwable异常（包括LinkageError、SecurityException等），尝试使用java.lang.reflect.Proxy.newProxyInstance生成JDK动态代理</li>
     *       <li>记录ERROR日志说明Javassist失败但JDK代理成功，便于后续排查环境问题</li>
     *     </ul>
     *   </li>
     *   <li><b>双重失败处理</b>：
     *     <ul>
     *       <li>如果JDK代理也失败，分别记录Javassist和JDK的错误日志，提供完整的故障诊断信息</li>
     *       <li>抛出原始Javassist异常，中断Spring容器启动，提示用户检查类加载器配置或安全策略</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param interfaces 需要代理的接口列表，包含服务接口、Dubbo内部接口（EchoService等），决定代理对象的方法签名
     */
    private void generateFromJavassistFirst(List<Class<?>> interfaces) {
        try {
            this.lazyProxy = Proxy.getProxy(interfaces.toArray(new Class[0]))
                    .newInstance(new LazyTargetInvocationHandler(new DubboReferenceLazyInitTargetSource()));
        } catch (Throwable fromJavassist) {
            /*
             * Javassist失败降级：
             * 尝试使用JDK动态代理作为兜底方案，保证在受限环境下仍能正常工作
             */
            // try fall back to JDK proxy factory
            try {
                this.lazyProxy = java.lang.reflect.Proxy.newProxyInstance(
                        beanClassLoader,
                        interfaces.toArray(new Class[0]),
                        new LazyTargetInvocationHandler(new DubboReferenceLazyInitTargetSource()));
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy success. "
                                + "Interfaces: " + interfaces,
                        fromJavassist);
            } catch (Throwable fromJdk) {
                /*
                 * 双重失败处理：
                 * 记录两种代理方式的错误日志，抛出原始异常中断启动
                 */
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + interfaces + " Javassist Error.",
                        fromJavassist);
                logger.error(
                        PROXY_FAILED,
                        "",
                        "",
                        "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy is also failed. "
                                + "Interfaces: " + interfaces + " JDK Error.",
                        fromJdk);
                throw fromJavassist;
            }
        }
    }

    private void generateFromJdk(List<Class<?>> interfaces) {
        try {
            this.lazyProxy = java.lang.reflect.Proxy.newProxyInstance(
                    beanClassLoader,
                    interfaces.toArray(new Class[0]),
                    new LazyTargetInvocationHandler(new DubboReferenceLazyInitTargetSource()));
        } catch (Throwable fromJdk) {
            logger.error(
                    PROXY_FAILED,
                    "",
                    "",
                    "Failed to generate proxy by Javassist failed. Fallback to use JDK proxy is also failed. "
                            + "Interfaces: " + interfaces + " JDK Error.",
                    fromJdk);
            throw fromJdk;
        }
    }

    /**
     * 获取RPC调用代理对象，处理懒加载场景下的ReferenceConfig初始化和同步控制。
     * <p>
     * 该方法是Dubbo Spring集成中懒加载机制的核心执行逻辑，在首次调用代理方法时被触发。
     * 负责确保ReferenceConfig已正确初始化，并通过双重检查锁保证线程安全和避免重复初始化。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>空值检查与初始化</b>：
     *     <ul>
     *       <li>如果referenceConfig为null，说明Spring容器启动阶段未完成Bean的完整初始化</li>
     *       <li>通过双重检查锁（DCL）模式，调用referenceBeanManager.initReferenceBean手动触发初始化</li>
     *       <li>初始化DubboConfigApplicationListener确保配置监听器已就绪</li>
     *       <li>记录WARN日志提醒用户应在Dubbo启动完成后才调用引用方法，避免时序问题</li>
     *     </ul>
     *   </li>
     *   <li><b>快速获取</b>：如果referenceConfig已完成配置初始化且非懒加载场景，直接调用referenceConfig.get()返回代理，避免不必要的同步开销</li>
     *   <li><b>同步获取</b>：在同步块中调用referenceConfig.get()，确保多线程环境下只创建一个Invoker代理实例，防止并发创建导致的资源浪费和状态不一致</li>
     * </ol>
     * </p>
     *
     * @return RPC代理对象，类型为服务接口，调用其方法时会通过网络远程执行提供者逻辑
     * @throws Exception 当服务引用失败、注册中心连接异常、提供者不可用或配置错误时抛出异常
     */
    private Object getCallProxy() throws Exception {
        if (referenceConfig == null) {
            /*
             * 延迟初始化保护：
             * 当代理方法在Spring容器完全启动前被调用时，手动触发ReferenceConfig的初始化流程
             */
            synchronized (LockUtils.getSingletonMutex(applicationContext)) {
                if (referenceConfig == null) {
                    referenceBeanManager.initReferenceBean(this);
                    applicationContext
                            .getBean(
                                    DubboConfigApplicationListener.class.getName(),
                                    DubboConfigApplicationListener.class)
                            .init();
                    logger.warn(
                            CONFIG_DUBBO_BEAN_INITIALIZER,
                            "",
                            "",
                            "ReferenceBean is not ready yet, please make sure to "
                                    + "call reference interface method after dubbo is started.");
                }
            }
        }
        /*
         * 获取引用代理：
         * 使用Spring容器的单例锁进行同步，避免与Spring自身的Bean创建逻辑产生死锁
         */
        // get reference proxy
        // Subclasses should synchronize on the given Object if they perform any sort of extended singleton creation
        // phase.
        // In particular, subclasses should not have their own mutexes involved in singleton creation, to avoid the
        // potential for deadlocks in lazy-init situations.
        // The redundant type cast is to be compatible with earlier than spring-4.2
        if (referenceConfig.configInitialized()) {
            return referenceConfig.get();
        }
        synchronized (LockUtils.getSingletonMutex(applicationContext)) {
            return referenceConfig.get();
        }
    }

    private class DubboReferenceLazyInitTargetSource implements LazyTargetSource {
    /**
     * 获取懒加载代理的目标对象，触发ReferenceConfig的初始化并返回RPC代理。
     * <p>
     * 该方法是LazyTargetSource接口的核心实现，在首次调用代理方法时被LazyTargetInvocationHandler触发。
     * 负责延迟执行ReferenceConfig.get()完成服务引用、建立网络连接、创建Invoker链，最终返回可调用的RPC代理对象。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>委托调用</b>：直接调用getCallProxy()方法，该方法内部会检查ReferenceConfig是否已初始化，未初始化则执行完整的init流程</li>
     *   <li><b>返回代理</b>：返回referenceConfig.get()生成的RPC代理对象，后续所有方法调用都会直接转发到该代理，不再经过懒加载逻辑</li>
     * </ol>
     * </p>
     *
     * @return RPC代理对象，类型为服务接口，调用其方法时会通过网络远程执行提供者逻辑
     * @throws Exception 当服务引用失败、注册中心连接异常或提供者不可用时抛出异常
     */
    @Override
    public Object getTarget() throws Exception {
        return getCallProxy();
    }
    }

    public void setInterfaceClass(Class<?> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    public void setInterfaceName(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    /**
     * It is only used in native scenarios to get referenceProps
     * because attribute is not passed by BeanDefinition by default.
     * @param referencePropsJson
     */
    public void setReferencePropsJson(String referencePropsJson) {
        if (StringUtils.isNotEmpty(referencePropsJson)) {
            this.referenceProps = JsonUtils.toJavaObject(referencePropsJson, Map.class);
        }
    }
}
