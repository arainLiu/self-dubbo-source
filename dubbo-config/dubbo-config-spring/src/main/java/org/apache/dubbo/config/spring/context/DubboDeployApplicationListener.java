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

import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.deploy.DeployListenerAdapter;
import org.apache.dubbo.common.deploy.DeployState;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.spring.context.event.DubboApplicationStateEvent;
import org.apache.dubbo.config.spring.context.event.DubboModuleStateEvent;
import org.apache.dubbo.config.spring.util.DubboBeanUtils;
import org.apache.dubbo.config.spring.util.LockUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ModelConstants;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.concurrent.Future;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_FAILED_START_MODEL;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_STOP_DUBBO_ERROR;
import static org.springframework.util.ObjectUtils.nullSafeEquals;

/**
 * An ApplicationListener to control Dubbo application.
 */
/**
 * Dubbo部署应用事件监听器
 * <p>
 * 该类负责监听Spring ApplicationContext的生命周期事件，并协调Dubbo模块的部署生命周期。
 * 主要功能包括：
 * 1. 监听Spring容器刷新事件（ContextRefreshedEvent），触发Dubbo模块启动
 * 2. 监听Spring容器关闭事件（ContextClosedEvent），触发Dubbo模块销毁
 * 3. 监听Dubbo ApplicationModel和ModuleModel的部署状态变化，并发布对应的Spring事件
 * 4. 支持同步/异步启动模式，可配置是否等待启动完成
 * 5. 支持在Spring容器关闭时保持Dubbo服务运行的配置选项
 * <p>
 * 该监听器的执行优先级设置为最低（LOWEST_PRECEDENCE），确保在其他监听器之后执行。
 *
 * @since 3.0
 */
public class DubboDeployApplicationListener
        implements ApplicationListener<ApplicationContextEvent>, ApplicationContextAware, Ordered {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(DubboDeployApplicationListener.class);

    private ApplicationContext applicationContext;

    private ApplicationModel applicationModel;
    private ModuleModel moduleModel;

    /**
     * 设置Spring ApplicationContext并初始化部署监听器
     * <p>
     * 该方法在Spring容器初始化Bean时被调用，主要执行以下操作：
     * 1. 保存ApplicationContext引用
     * 2. 从Spring容器中获取ApplicationModel和ModuleModel
     * 3. 注册ApplicationModel的部署状态监听器，将状态变化发布为Spring事件
     * 4. 注册ModuleModel的部署状态监听器，将状态变化发布为Spring事件
     *
     * @param applicationContext Spring应用上下文
     * @throws BeansException 当获取Bean发生异常时抛出
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        this.applicationModel = DubboBeanUtils.getApplicationModel(applicationContext);
        this.moduleModel = DubboBeanUtils.getModuleModel(applicationContext);

        // 监听ApplicationModel的部署事件并发布DubboApplicationStateEvent
        applicationModel.getDeployer().addDeployListener(new DeployListenerAdapter<ApplicationModel>() {
            @Override
            public void onStarting(ApplicationModel scopeModel) {
                publishApplicationEvent(DeployState.STARTING);
            }

            @Override
            public void onStarted(ApplicationModel scopeModel) {
                publishApplicationEvent(DeployState.STARTED);
            }

            @Override
            public void onCompletion(ApplicationModel scopeModel) {
                publishApplicationEvent(DeployState.COMPLETION);
            }

            @Override
            public void onStopping(ApplicationModel scopeModel) {
                publishApplicationEvent(DeployState.STOPPING);
            }

            @Override
            public void onStopped(ApplicationModel scopeModel) {
                publishApplicationEvent(DeployState.STOPPED);
            }

            @Override
            public void onFailure(ApplicationModel scopeModel, Throwable cause) {
                publishApplicationEvent(DeployState.FAILED, cause);
            }
        });

        // 监听ModuleModel的部署事件并发布DubboModuleStateEvent
        moduleModel.getDeployer().addDeployListener(new DeployListenerAdapter<ModuleModel>() {
            @Override
            public void onStarting(ModuleModel scopeModel) {
                publishModuleEvent(DeployState.STARTING);
            }

            @Override
            public void onStarted(ModuleModel scopeModel) {
                publishModuleEvent(DeployState.STARTED);
            }

            @Override
            public void onCompletion(ModuleModel scopeModel) {
                publishModuleEvent(DeployState.COMPLETION);
            }

            @Override
            public void onStopping(ModuleModel scopeModel) {
                publishModuleEvent(DeployState.STOPPING);
            }

            @Override
            public void onStopped(ModuleModel scopeModel) {
                publishModuleEvent(DeployState.STOPPED);
            }

            @Override
            public void onFailure(ModuleModel scopeModel, Throwable cause) {
                publishModuleEvent(DeployState.FAILED, cause);
            }
        });
    }

    private void publishApplicationEvent(DeployState state) {
        applicationContext.publishEvent(new DubboApplicationStateEvent(applicationModel, state));
    }

    private void publishApplicationEvent(DeployState state, Throwable cause) {
        applicationContext.publishEvent(new DubboApplicationStateEvent(applicationModel, state, cause));
    }

    private void publishModuleEvent(DeployState state) {
        applicationContext.publishEvent(new DubboModuleStateEvent(moduleModel, state));
    }

    private void publishModuleEvent(DeployState state, Throwable cause) {
        applicationContext.publishEvent(new DubboModuleStateEvent(moduleModel, state, cause));
    }

    /**
     * 处理Spring ApplicationContext事件
     * <p>
     * 仅处理来自当前ApplicationContext的事件：
     * - ContextRefreshedEvent: 容器刷新完成，触发Dubbo模块启动
     * - ContextClosedEvent: 容器关闭，触发Dubbo模块销毁
     *
     * @param event Spring应用上下文事件
     */
    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (nullSafeEquals(applicationContext, event.getSource())) {
            if (event instanceof ContextRefreshedEvent) {
                onContextRefreshedEvent((ContextRefreshedEvent) event);
            } else if (event instanceof ContextClosedEvent) {
                onContextClosedEvent((ContextClosedEvent) event);
            }
        }
    }

    /**
     * 处理Spring容器刷新事件，启动Dubbo模块
     * <p>
     * 该方法在Spring容器刷新完成后被调用，负责启动Dubbo模块：
     * 1. 获取ModuleDeployer并启动模块
     * 2. 如果配置为非后台启动模式，则同步等待启动完成
     * 3. 处理启动过程中的中断和异常情况
     *
     * @param event Spring容器刷新事件
     */
    private void onContextRefreshedEvent(ContextRefreshedEvent event) {
        ModuleDeployer deployer = moduleModel.getDeployer();
        Assert.notNull(deployer, "Module deployer is null");
        Object singletonMutex = LockUtils.getSingletonMutex(applicationContext);

        // 同步启动模块，避免并发启动问题
        Future future = null;
        synchronized (singletonMutex) {
            future = deployer.start();
        }

        // 如果模块不是后台启动，则等待启动完成
        if (!deployer.isBackground()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                logger.warn(
                        CONFIG_FAILED_START_MODEL,
                        "",
                        "",
                        "Interrupted while waiting for dubbo module start: " + e.getMessage());
            } catch (Exception e) {
                logger.warn(
                        CONFIG_FAILED_START_MODEL,
                        "",
                        "",
                        "An error occurred while waiting for dubbo module start: " + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * 处理Spring容器关闭事件，销毁Dubbo模块
     * <p>
     * 该方法在Spring容器关闭时被调用，负责清理Dubbo模块资源：
     * 1. 检查是否配置了保持运行（KEEP_RUNNING_ON_SPRING_CLOSED）
     * 2. 如果未配置保持运行且模块未销毁，则执行模块销毁
     * 3. 移除Spring上下文与Dubbo上下文的绑定关系
     * 4. 处理销毁过程中的异常情况
     *
     * @param event Spring容器关闭事件
     */
    private void onContextClosedEvent(ContextClosedEvent event) {
        try {
            // 检查是否配置了在Spring关闭时保持运行
            Object value = moduleModel.getAttribute(ModelConstants.KEEP_RUNNING_ON_SPRING_CLOSED);
            if (value == null) {
                value = ConfigurationUtils.getProperty(moduleModel, ModelConstants.KEEP_RUNNING_ON_SPRING_CLOSED_KEY);
            }
            boolean keepRunningOnClosed = Boolean.parseBoolean(String.valueOf(value));
            if (!keepRunningOnClosed && !moduleModel.isDestroyed()) {
                moduleModel.destroy();
            }
        } catch (Exception e) {
            logger.error(
                    CONFIG_STOP_DUBBO_ERROR,
                    "",
                    "",
                    "Unexpected error occurred when stop dubbo module: " + e.getMessage(),
                    e);
        }

        // 移除Spring上下文与Dubbo上下文的绑定缓存
        DubboSpringInitializer.remove(event.getApplicationContext());
    }

    /**
     * 获取监听器的执行顺序
     * <p>
     * 返回最低优先级，确保该监听器在其他监听器之后执行。
     *
     * @return 优先级值，Integer.MAX_VALUE表示最低优先级
     */
    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
