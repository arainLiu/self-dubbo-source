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
package org.apache.dubbo.registry.retry;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.support.FailbackRegistry;

import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_EXECUTE_RETRYING_TASK;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_TIMES;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_TIMES_KEY;

/**
 * 抽象的重试任务基类
 * <p>
 * 该类实现了TimerTask接口，用于处理Dubbo注册中心的失败重试逻辑。
 * 支持以下特性：
 * 1. 可配置的重试周期和最大重试次数
 * 2. 任务取消机制
 * 3. 自动重调度功能
 * 4. 异常捕获和日志记录
 * <p>
 * 子类需要实现{@link #doRetry(URL, FailbackRegistry, Timeout)}方法来定义具体的重试逻辑。
 *
 * @see TimerTask
 * @see FailbackRegistry
 */
public abstract class AbstractRetryTask implements TimerTask {

    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    /**
     * url for retry task
     */
    protected final URL url;

    /**
     * registry for this task
     */
    protected final FailbackRegistry registry;

    /**
     * retry period
     */
    private final long retryPeriod;

    /**
     * define the most retry times
     */
    private final int retryTimes;

    /**
     * task name for this task
     */
    private final String taskName;

    /**
     * times of retry.
     * retry task is execute in single thread so that the times is not need volatile.
     */
    private int times = 1;

    private volatile boolean cancel;

    /**
     * 构造函数，初始化重试任务的配置参数
     *
     * @param url 注册中心的URL，包含重试配置参数
     * @param registry FailbackRegistry实例，用于执行重试操作
     * @param taskName 任务名称，用于日志标识
     * @throws IllegalArgumentException 当url为空或taskName为空白时抛出
     */
    AbstractRetryTask(URL url, FailbackRegistry registry, String taskName) {
        if (url == null || StringUtils.isBlank(taskName)) {
            throw new IllegalArgumentException();
        }
        this.url = url;
        this.registry = registry;
        this.taskName = taskName;
        this.cancel = false;

        // 从URL中获取重试周期配置，默认为DEFAULT_REGISTRY_RETRY_PERIOD
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);

        // 从URL中获取最大重试次数配置，默认为DEFAULT_REGISTRY_RETRY_TIMES
        this.retryTimes = url.getParameter(REGISTRY_RETRY_TIMES_KEY, DEFAULT_REGISTRY_RETRY_TIMES);
    }

    /**
     * 取消当前重试任务
     */
    public void cancel() {
        cancel = true;
    }

    /**
     * 检查任务是否已取消
     *
     * @return 如果任务已取消返回true，否则返回false
     */
    public boolean isCancel() {
        return cancel;
    }

    /**
     * 重新调度任务到定时器中
     * <p>
     * 该方法在重试失败后被调用，将任务重新添加到定时器中以进行下一次重试。
     *
     * @param timeout 当前的Timeout对象
     * @param tick 下次执行的延迟时间（毫秒）
     * @throws IllegalArgumentException 当timeout为null时抛出
     */
    protected void reput(Timeout timeout, long tick) {
        if (timeout == null) {
            throw new IllegalArgumentException();
        }

        // 获取定时器实例
        Timer timer = timeout.timer();

        // 如果定时器已停止、任务已取消或任务被标记为取消，则不再重调度
        if (timer.isStop() || timeout.isCancelled() || isCancel()) {
            return;
        }

        // 增加重试次数
        times++;

        // 将任务重新添加到定时器中，延迟tick毫秒后执行
        timer.newTimeout(timeout.task(), tick, TimeUnit.MILLISECONDS);
    }

    /**
     * 执行重试任务的核心逻辑
     * <p>
     * 该方法由定时器调用，主要流程：
     * 1. 检查任务是否已取消或定时器是否已停止
     * 2. 检查是否超过最大重试次数
     * 3. 检查注册中心是否可用
     * 4. 调用子类的doRetry方法执行具体重试逻辑
     * 5. 如果发生异常，捕获并重新调度任务
     *
     * @param timeout 定时器超时对象
     * @throws Exception 当执行过程中发生异常时抛出
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        // 检查任务是否已取消或定时器是否已停止
        if (timeout.isCancelled() || timeout.timer().isStop() || isCancel()) {
            // other thread cancel this timeout or stop the timer.
            return;
        }

        // 检查是否超过最大重试次数
        if (retryTimes > 0 && times > retryTimes) {
            // 1-13 - failed to execute the retrying task.

            logger.warn(
                    REGISTRY_EXECUTE_RETRYING_TASK,
                    "registry center offline",
                    "Check the registry server.",
                    "Final failed to execute task " + taskName + ", url: " + url + ", retry " + retryTimes + " times.");

            return;
        }

        // 记录重试日志
        if (logger.isInfoEnabled()) {
            logger.info(taskName + " : " + url);
        }

        try {
            // 检查注册中心是否可用
            if (!registry.isAvailable()) {
                throw new IllegalStateException("Registry is not available.");
            }

            // 调用子类实现的具体重试逻辑
            doRetry(url, registry, timeout);
        } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry

            // 1-13 - failed to execute the retrying task.

            logger.warn(
                    REGISTRY_EXECUTE_RETRYING_TASK,
                    "registry center offline",
                    "Check the registry server.",
                    "Failed to execute task " + taskName + ", url: " + url + ", waiting for again, cause:"
                            + t.getMessage(),
                    t);

            // reput this task when catch exception.
            // 捕获异常后重新调度任务，等待下一次重试
            reput(timeout, retryPeriod);
        }
    }

    /**
     * 执行具体的重试逻辑
     * <p>
     * 子类需要实现该方法来定义具体的重试行为，如重新注册、重新订阅等。
     *
     * @param url 注册中心的URL
     * @param registry FailbackRegistry实例
     * @param timeout 定时器超时对象
     */
    protected abstract void doRetry(URL url, FailbackRegistry registry, Timeout timeout);
}
