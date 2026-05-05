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

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.spring.context.event.DubboConfigInitEvent;
import org.apache.dubbo.config.spring.util.DubboBeanUtils;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_DUBBO_BEAN_NOT_FOUND;
import static org.springframework.util.ObjectUtils.nullSafeEquals;

/**
 * An ApplicationListener to load config beans
 */
/**
 * Dubbo配置初始化事件监听器
 * <p>
 * 该类负责监听{@link DubboConfigInitEvent}事件，在Spring容器加载非懒加载单例Bean之前，
 * 初始化Dubbo的配置Bean。主要功能包括：
 * 1. 接收DubboConfigInitEvent事件通知
 * 2. 确保初始化逻辑只执行一次（使用AtomicBoolean保证线程安全）
 * 3. 加载并触发DubboConfigBeanInitializer初始化配置Bean
 * 4. 调用ModuleDeployer.prepare()完成Dubbo配置的最终初始化
 * <p>
 * 该监听器的设计确保在所有BeanFactoryPostProcessor执行完成后、
 * 非懒加载单例Bean加载之前完成Dubbo配置的初始化。
 *
 * @since 3.0
 */
public class DubboConfigApplicationListener
        implements ApplicationListener<DubboConfigInitEvent>, ApplicationContextAware {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(DubboConfigApplicationListener.class);

    private ApplicationContext applicationContext;

    private ModuleModel moduleModel;

    private final AtomicBoolean initialized = new AtomicBoolean();

    /**
     * 设置Spring ApplicationContext并获取ModuleModel
     *
     * @param applicationContext Spring应用上下文
     * @throws BeansException 当获取Bean发生异常时抛出
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        this.moduleModel = DubboBeanUtils.getModuleModel(applicationContext);
    }

    /**
     * 处理Dubbo配置初始化事件
     * <p>
     * 仅处理来自当前ApplicationContext的事件，触发Dubbo配置初始化流程。
     *
     * @param event Dubbo配置初始化事件
     */
    @Override
    public void onApplicationEvent(DubboConfigInitEvent event) {
        if (nullSafeEquals(applicationContext, event.getSource())) {
            init();
        }
    }

    /**
     * 初始化Dubbo配置Bean（线程安全）
     * <p>
     * 该方法使用CAS操作确保初始化逻辑只执行一次，即使被多次调用也只会初始化一次。
     * 这是为了确保在Spring容器注册监听器阶段（AbstractApplicationContext.registerListeners()）
     * 能够及时收到通知，在非懒加载单例Bean加载之前完成Dubbo配置的初始化。
     */
    public synchronized void init() {
        // It's expected to be notified at
        // org.springframework.context.support.AbstractApplicationContext.registerListeners(),
        // before loading non-lazy singleton beans. At this moment, all BeanFactoryPostProcessor have been processed,
        if (initialized.compareAndSet(false, true)) {
            initDubboConfigBeans();
        }
    }

    /**
     * 初始化Dubbo配置Bean
     * <p>
     * 该方法执行以下操作：
     * 1. 查找并加载DubboConfigBeanInitializer Bean，触发Dubbo配置Bean的初始化
     * 2. 如果未找到DubboConfigBeanInitializer，记录警告日志
     * 3. 调用ModuleDeployer.prepare()方法，完成所有基础设施配置Bean的最终初始化
     */
    private void initDubboConfigBeans() {
        // load DubboConfigBeanInitializer to init config beans
        if (applicationContext.containsBean(DubboConfigBeanInitializer.BEAN_NAME)) {
            applicationContext.getBean(DubboConfigBeanInitializer.BEAN_NAME, DubboConfigBeanInitializer.class);
        } else {
            logger.warn(
                    CONFIG_DUBBO_BEAN_NOT_FOUND,
                    "",
                    "",
                    "Bean '" + DubboConfigBeanInitializer.BEAN_NAME + "' was not found");
        }

        // All infrastructure config beans are loaded, initialize dubbo here
        moduleModel.getDeployer().prepare();
    }

}
