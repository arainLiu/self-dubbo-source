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
package org.apache.dubbo.common.threadpool.support.fixed;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.MemorySafeLinkedBlockingQueue;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_QUEUES;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_THREADS;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_THREAD_NAME;
import static org.apache.dubbo.common.constants.CommonConstants.QUEUES_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREAD_NAME_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;

/**
 * Creates a thread pool that reuses a fixed number of threads
 *
 * @see java.util.concurrent.Executors#newFixedThreadPool(int)
 */
public class FixedThreadPool implements ThreadPool {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(FixedThreadPool.class);

    /**
     * 创建固定大小的线程池执行器
     * <p>
     * 该方法根据URL配置参数创建ThreadPoolExecutor，主要配置项包括：
     * 1. 线程名称：从THREAD_NAME_KEY参数或属性中获取，默认为DEFAULT_THREAD_NAME
     * 2. 线程数量：从THREADS_KEY参数中获取，默认为DEFAULT_THREADS
     * 3. 队列容量：从QUEUES_KEY参数中获取，默认为DEFAULT_QUEUES
     * <p>
     * 阻塞队列的选择策略：
     * - queues == 0：使用SynchronousQueue（不存储元素的同步队列）
     * - queues < 0：使用MemorySafeLinkedBlockingQueue（无界队列，会记录警告日志）
     * - queues > 0：使用指定容量的LinkedBlockingQueue
     * <p>
     * 线程池配置特点：
     * - 核心线程数与最大线程数相同，保持固定大小
     * - 线程空闲时间为0，立即回收多余线程（但在此配置下不会有多余线程）
     * - 使用NamedInternalThreadFactory创建命名线程，便于问题排查
     * - 使用AbortPolicyWithReport拒绝策略，在任务被拒绝时生成报告
     *
     * @param URL 包含线程池配置参数的URL对象
     * @return 配置好的ThreadPoolExecutor执行器
     */
    @Override
    public Executor getExecutor(URL url) {
        // 获取线程名称，优先从参数中获取，其次从属性中获取，最后使用默认值
        String name =
                url.getParameter(THREAD_NAME_KEY, (String) url.getAttribute(THREAD_NAME_KEY, DEFAULT_THREAD_NAME));

        // 获取线程数量和队列容量配置
        int threads = url.getParameter(THREADS_KEY, DEFAULT_THREADS);
        int queues = url.getParameter(QUEUES_KEY, DEFAULT_QUEUES);

        BlockingQueue<Runnable> blockingQueue;

        // 根据队列容量配置选择合适的阻塞队列
        if (queues == 0) {
            // 使用同步队列，不存储元素，直接传递任务
            blockingQueue = new SynchronousQueue<>();
        } else if (queues < 0) {
            // 使用无界队列，可能导致OOM风险，记录警告日志
            blockingQueue = new MemorySafeLinkedBlockingQueue<>();
            logger.warn(
                    COMMON_UNEXPECTED_EXCEPTION,
                    "",
                    "",
                    "FixedThreadPool created with an unbounded queue (queues < 0). "
                            + "This may lead to OutOfMemoryError under high load. "
                            + "Consider configuring a positive integer for 'queues'.");
        } else {
            // 使用指定容量的有界队列
            blockingQueue = new LinkedBlockingQueue<>(queues);
        }

        // 创建固定大小的ThreadPoolExecutor
        return new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                blockingQueue,
                new NamedInternalThreadFactory(name, true),
                new AbortPolicyWithReport(name, url));
    }

}
