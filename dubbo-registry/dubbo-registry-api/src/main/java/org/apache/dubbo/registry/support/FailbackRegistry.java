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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.ProviderFirstParams;
import org.apache.dubbo.registry.retry.FailedRegisteredTask;
import org.apache.dubbo.registry.retry.FailedSubscribedTask;
import org.apache.dubbo.registry.retry.FailedUnregisteredTask;
import org.apache.dubbo.registry.retry.FailedUnsubscribedTask;
import org.apache.dubbo.remoting.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.IS_EXTRA;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_NOTIFY_EVENT;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;

/**
 * 具备失败重试能力的注册中心抽象基类
 * 继承自AbstractRegistry，通过HashedWheelTimer定时任务机制处理注册、注销、订阅、反订阅过程中的临时性故障
 * 确保在网络抖动或注册中心短暂不可用时，服务能够通过自动重试最终达成一致状态
 *  * A template implementation of registry service that provides auto-retry ability.
 *  * (SPI, Prototype, ThreadSafe)
 */
public abstract class FailbackRegistry extends AbstractRegistry {

    /*  retry task map */

    /** 存储注册失败的任务，以URL为键 */
    private final ConcurrentMap<URL, FailedRegisteredTask> failedRegistered = new ConcurrentHashMap<>();

    /** 存储注销失败的任务，以URL为键 */
    private final ConcurrentMap<URL, FailedUnregisteredTask> failedUnregistered = new ConcurrentHashMap<>();

    /** 存储订阅失败的任务，以Holder（URL+Listener）为键 */
    private final ConcurrentMap<Holder, FailedSubscribedTask> failedSubscribed = new ConcurrentHashMap<>();

    /** 存储反订阅失败的任务，以Holder（URL+Listener）为键 */
    private final ConcurrentMap<Holder, FailedUnsubscribedTask> failedUnsubscribed = new ConcurrentHashMap<>();

    /**
     * 重试执行器的等待周期，单位为毫秒
     */
    private final int retryPeriod;

    // 用于失败重试的定时器，定期检查是否有失败请求，如果有则执行无限次重试
    private final HashedWheelTimer retryTimer;

    public FailbackRegistry(URL url) {
        super(url);
        // 从URL参数中获取重试周期，默认为DEFAULT_REGISTRY_RETRY_PERIOD
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);

        // since the retry task will not be very much. 128 ticks is enough.
        // 初始化时间轮定时器，使用守护线程，128个槽位足以应对常规的重试任务量
        retryTimer = new HashedWheelTimer(
                new NamedThreadFactory("DubboRegistryRetryTimer", true), retryPeriod, TimeUnit.MILLISECONDS, 128);
    }

    public void removeFailedRegisteredTask(URL url) {
        failedRegistered.remove(url);
    }

    public void removeFailedUnregisteredTask(URL url) {
        failedUnregistered.remove(url);
    }

    public void removeFailedSubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedSubscribed.remove(h);
    }

    public void removeFailedUnsubscribedTask(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        failedUnsubscribed.remove(h);
    }

    /**
     * 将注册失败的URL加入重试队列
     * 如果该URL已经在重试队列中，则不重复添加
     */
    private void addFailedRegistered(URL url) {
        FailedRegisteredTask oldOne = failedRegistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedRegisteredTask newTask = new FailedRegisteredTask(url, this);
        oldOne = failedRegistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            // 首次添加失败任务时，启动定时器进行周期性重试
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 从重试队列中移除注册失败的任务，并取消对应的定时任务
     */
    private void removeFailedRegistered(URL url) {
        FailedRegisteredTask f = failedRegistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 将注销失败的URL加入重试队列
     */
    private void addFailedUnregistered(URL url) {
        FailedUnregisteredTask oldOne = failedUnregistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedUnregisteredTask newTask = new FailedUnregisteredTask(url, this);
        oldOne = failedUnregistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 从重试队列中移除注销失败的任务，并取消对应的定时任务
     */
    private void removeFailedUnregistered(URL url) {
        FailedUnregisteredTask f = failedUnregistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 将订阅失败的URL和监听器加入重试队列
     */
    protected void addFailedSubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedSubscribedTask oldOne = failedSubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedSubscribedTask newTask = new FailedSubscribedTask(url, this, listener);
        oldOne = failedSubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 从重试队列中移除订阅失败的任务，同时也会移除相关的反订阅失败任务
     */
    public void removeFailedSubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedSubscribedTask f = failedSubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
        removeFailedUnsubscribed(url, listener);
    }

    /**
     * 将反订阅失败的URL和监听器加入重试队列
     */
    private void addFailedUnsubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask oldOne = failedUnsubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedUnsubscribedTask newTask = new FailedUnsubscribedTask(url, this, listener);
        oldOne = failedUnsubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 从重试队列中移除反订阅失败的任务，并取消对应的定时任务
     */
    private void removeFailedUnsubscribed(URL url, NotifyListener listener) {
        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask f = failedUnsubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
    }

    protected URL removeParamsFromConsumer(URL consumer) {
        Set<ProviderFirstParams> providerFirstParams = consumer.getOrDefaultApplicationModel()
                .getExtensionLoader(ProviderFirstParams.class)
                .getSupportedExtensionInstances();
        if (CollectionUtils.isEmpty(providerFirstParams)) {
            return consumer;
        }

        for (ProviderFirstParams paramsFilter : providerFirstParams) {
            consumer = consumer.removeParameters(paramsFilter.params());
        }
        return consumer;
    }

    ConcurrentMap<URL, FailedRegisteredTask> getFailedRegistered() {
        return failedRegistered;
    }

    ConcurrentMap<URL, FailedUnregisteredTask> getFailedUnregistered() {
        return failedUnregistered;
    }

    ConcurrentMap<Holder, FailedSubscribedTask> getFailedSubscribed() {
        return failedSubscribed;
    }

    ConcurrentMap<Holder, FailedUnsubscribedTask> getFailedUnsubscribed() {
        return failedUnsubscribed;
    }

    /**
     * 注册服务提供者地址到注册中心，具备自动重试和失败容错能力。
     * <p>
     * 该方法是FailbackRegistry的核心实现，在AbstractRegistry的基础上增加了失败重试机制。
     * 当注册失败时，根据check参数决定是否立即抛出异常还是异步重试。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>前置校验</b>：通过shouldRegister判断URL是否需要注册（如协议类型过滤、extra标记检查）</li>
     *   <li><b>状态清理</b>：移除之前可能存在的失败注册和取消记录，避免脏数据干扰</li>
     *   <li><b>执行注册</b>：调用doRegister模板方法将URL写入注册中心（由子类实现具体逻辑）</li>
     *   <li><b>异常处理</b>：根据check参数和异常类型决定是抛出异常还是加入重试队列</li>
     *   <li><b>失败重试</b>：对于非致命异常，将URL添加到failedRegistered列表，通过定时任务定期重试</li>
     * </ol>
     * </p>
     *
     * @param url 需要注册的服务提供者URL，包含服务接口、协议、主机、端口等完整信息
     * @throws IllegalStateException 当满足以下条件之一时抛出：
     *                               <ul>
     *                                 <li>check参数为true且注册失败（启动时严格检查）</li>
     *                                 <li>异常为SkipFailbackWrapperException类型（包装的致命异常）</li>
     *                               </ul>
     */
    @Override
    public void register(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
        super.register(url);
        /*
         * 清理历史失败记录：
         * 确保本次注册不受之前失败操作的影响，避免重复重试
         */
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            /*
             * 执行实际注册操作：
             * 调用子类的doRegister实现（如ZookeeperRegistry的节点创建），可能抛出异常
             */
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;

            /*
             * 判断是否应该直接抛出异常：
             * check=true表示启动阶段需要严格检查，端口为0表示本地服务无需严格检查
             * SkipFailbackWrapperException表示不可恢复的致命异常，需要立即抛出
             */
            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && (url.getPort() != 0);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to register " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                /*
                 * 记录失败日志并安排重试：
                 * 对于临时性故障（如网络抖动），通过定时任务定期重试，保证最终一致性
                 */
                logger.error(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            /*
             * 加入失败重试队列：
             * 创建FailedRegisteredTask并通过HashedWheelTimer定时重试（默认周期5秒）
             */
            // Record a failed registration request to a failed list, retry regularly
            addFailedRegistered(url);
        }
    }

    /**
     * 判断给定的提供者URL是否应该被注册
     *
     * @param providerURL 提供者URL
     * @return 如果应该注册返回true，否则返回false
     */
    protected boolean shouldRegister(URL providerURL) {
        // extra protocol url must not be registered for interface based service discovery
        // 如果是额外协议的URL，在基于接口的服务发现模式下不进行注册
        if (providerURL.getParameter(IS_EXTRA, false)) {
            return false;
        }
        // 检查注册中心是否接受该协议类型的服务
        if (!acceptable(providerURL)) {
            logger.info("URL " + providerURL + " will not be registered to Registry. Registry " + this.getUrl()
                    + " does not accept service of this protocol type.");
            return false;
        }
        return true;
    }

    @Override
    public void reExportRegister(URL url) {
        if (!acceptable(url)) {
            logger.info("URL " + url + " will not be registered to Registry. Registry " + url
                    + " does not accept service of this protocol type.");
            return;
        }
        super.register(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                        "Failed to register (re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * 从注册中心注销服务提供者地址，具备自动重试能力
     *
     * @param url 需要注销的服务提供者URL
     */
    @Override
    public void unregister(URL url) {
        super.unregister(url);
        // 清理之前可能存在的失败注册或注销记录
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a cancellation request to the server side
            // 执行实际的注销操作
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            // 判断是否需要立即抛出异常
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && (url.getPort() != 0);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to unregister " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                // 记录错误日志，等待定时任务重试
                logger.error(
                        INTERNAL_ERROR,
                        "unknown error in registry module",
                        "",
                        "Failed to unregister " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            // Record a failed registration request to a failed list, retry regularly
            // 将失败的注销任务加入重试队列
            addFailedUnregistered(url);
        }
    }

    @Override
    public void reExportUnregister(URL url) {
        super.unregister(url);
        removeFailedRegistered(url);
        removeFailedUnregistered(url);
        try {
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            if (!(e instanceof SkipFailbackWrapperException)) {
                throw new IllegalStateException(
                        "Failed to unregister(re-export) " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * 订阅服务提供者地址和配置规则，具备失败重试和本地缓存降级能力。
     * <p>
     * 该方法是FailbackRegistry的核心实现，在AbstractRegistry的基础上增加了订阅失败的容错机制。
     * 当订阅失败时，优先使用本地缓存的URL列表进行降级，如果缓存也不存在则根据check参数决定是否抛出异常或异步重试。
     * </p>
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>父类订阅</b>：调用super.subscribe将URL和Listener注册到本地缓存，建立订阅关系的基础数据结构</li>
     *   <li><b>清理失败记录</b>：移除之前可能存在的失败订阅记录，避免脏数据干扰本次订阅</li>
     *   <li><b>执行订阅</b>：调用doSubscribe模板方法向注册中心发起订阅请求（由子类实现Zookeeper/Nacos等具体逻辑）</li>
     *   <li><b>异常处理</b>：
     *     <ul>
     *       <li><b>有缓存降级</b>：从getCacheUrls获取本地缓存的提供者地址列表，立即通知Listener使用缓存数据，记录ERROR日志但不中断启动</li>
     *       <li><b>无缓存且check=true</b>：当开启启动检查或异常为SkipFailbackWrapperException时，直接抛出IllegalStateException阻止应用启动</li>
     *       <li><b>无缓存且check=false</b>：记录ERROR日志并调用addFailedSubscribed将订阅任务加入重试队列，通过定时任务定期重试（默认5秒间隔）</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param url      订阅URL，包含服务接口名、版本、分组、分类（providers/configurators/routers）等订阅条件
     * @param listener 通知监听器，当注册中心推送地址变更或配置更新时触发回调，重新生成Invoker链
     * @throws IllegalStateException 当订阅失败且check参数为true或异常为SkipFailbackWrapperException时抛出
     */
    @Override
    public void subscribe(URL url, NotifyListener listener) {
        super.subscribe(url, listener);
        /*
         * 清理历史失败记录：
         * 确保本次订阅不受之前失败操作的影响，避免重复重试
         */
        removeFailedSubscribed(url, listener);
        try {
            /*
             * 执行实际订阅操作：
             * 调用子类的doSubscribe实现（如ZookeeperRegistry的create持久化监听器），可能抛出异常
             */
            // Sending a subscription request to the server side
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            List<URL> urls = getCacheUrls(url);
            if (CollectionUtils.isNotEmpty(urls)) {
                /*
                 * 缓存降级策略：
                 * 当注册中心不可用时，使用本地磁盘缓存的地址列表继续运行，保证服务的可用性
                 */
                notify(url, listener, urls);
                logger.error(
                        REGISTRY_FAILED_NOTIFY_EVENT,
                        "",
                        "",
                        "Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: "
                                + getCacheFile().getName() + ", cause: " + t.getMessage(),
                        t);
            } else {
                /*
                 * 判断是否应该直接抛出异常：
                 * check=true表示启动阶段需要严格检查，必须确保订阅成功才能继续
                 * SkipFailbackWrapperException表示不可恢复的致命异常，需要立即抛出
                 */
                // If the startup detection is opened, the Exception is thrown directly.
                boolean check =
                        getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackWrapperException;
                if (check || skipFailback) {
                    if (skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    /*
                     * 记录失败并安排重试：
                     * 对于临时性故障（如网络抖动），通过定时任务定期重试，保证最终一致性
                     */
                    logger.error(
                            REGISTRY_FAILED_NOTIFY_EVENT,
                            "",
                            "",
                            "Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(),
                            t);
                }
            }

            /*
             * 加入失败重试队列：
             * 创建FailedSubscribedTask并通过HashedWheelTimer定时重试（默认周期5秒）
             */
            // Record a failed registration request to a failed list, retry regularly
            addFailedSubscribed(url, listener);
        }
    }

    /**
     * 取消订阅服务提供者地址和配置规则，具备自动重试能力
     *
     * @param url 订阅URL
     * @param listener 通知监听器
     */
    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        super.unsubscribe(url, listener);
        // 清理之前可能存在的失败订阅记录
        removeFailedSubscribed(url, listener);
        try {
            // Sending a canceling subscription request to the server side
            // 执行实际的取消订阅操作
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            // 判断是否需要立即抛出异常
            boolean check =
                    getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                        "Failed to unsubscribe " + url + " to registry " + getUrl().getAddress() + ", cause: "
                                + t.getMessage(),
                        t);
            } else {
                // 记录错误日志，等待定时任务重试
                logger.error(
                        REGISTRY_FAILED_NOTIFY_EVENT,
                        "",
                        "",
                        "Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(),
                        t);
            }

            // Record a failed registration request to a failed list, retry regularly
            // 将失败的取消订阅任务加入重试队列
            addFailedUnsubscribed(url, listener);
        }
    }

    @Override
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        try {
            doNotify(url, listener, urls);
        } catch (Exception t) {
            // Record a failed registration request to a failed list
            // 通知失败时记录日志，但不影响主流程
            logger.error(
                    REGISTRY_FAILED_NOTIFY_EVENT,
                    "",
                    "",
                    "Failed to notify addresses for subscribe " + url + ", cause: " + t.getMessage(),
                    t);
        }
    }

    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {
        super.notify(url, listener, urls);
    }

    /**
     * 恢复注册和订阅状态
     * 通常在注册中心重连后调用，将所有已注册和已订阅的URL重新加入重试队列
     */
    @Override
    protected void recover() throws Exception {
        // register
        // 恢复所有已注册的URL
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                // remove fail registry or unRegistry task first.
                // 先清理旧的失败任务，再添加新的重试任务
                removeFailedRegistered(url);
                removeFailedUnregistered(url);
                addFailedRegistered(url);
            }
        }
        // subscribe
        // 恢复所有已订阅的URL
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    // First remove other tasks to ensure that addFailedSubscribed can succeed.
                    // 先清理旧的失败订阅任务，确保新任务能成功添加
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    /**
     * 销毁注册中心实例，停止重试定时器
     */
    @Override
    public void destroy() {
        super.destroy();
        // 停止时间轮定时器，释放相关资源
        retryTimer.stop();
    }

    // ==== Template method ====

    /**
     * 执行实际的注册操作，由子类实现
     *
     * @param url 需要注册的URL
     */
    public abstract void doRegister(URL url);

    /**
     * 执行实际的注销操作，由子类实现
     *
     * @param url 需要注销的URL
     */
    public abstract void doUnregister(URL url);

    /**
     * 执行实际的订阅操作，由子类实现
     *
     * @param url 订阅URL
     * @param listener 通知监听器
     */
    public abstract void doSubscribe(URL url, NotifyListener listener);

    /**
     * 执行实际的取消订阅操作，由子类实现
     *
     * @param url 订阅URL
     * @param listener 通知监听器
     */
    public abstract void doUnsubscribe(URL url, NotifyListener listener);

    /**
     * 持有URL和监听器的组合键，用于在Map中唯一标识订阅关系
     */
    static class Holder {

        private final URL url;

        private final NotifyListener notifyListener;

        Holder(URL url, NotifyListener notifyListener) {
            if (url == null || notifyListener == null) {
                throw new IllegalArgumentException();
            }
            this.url = url;
            this.notifyListener = notifyListener;
        }

        @Override
        public int hashCode() {
            return url.hashCode() + notifyListener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Holder) {
                Holder h = (Holder) obj;
                return this.url.equals(h.url) && this.notifyListener.equals(h.notifyListener);
            } else {
                return false;
            }
        }
    }
}
