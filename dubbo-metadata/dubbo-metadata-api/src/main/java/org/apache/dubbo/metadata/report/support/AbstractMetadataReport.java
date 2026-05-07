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
package org.apache.dubbo.metadata.report.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.SystemPropertyConfigUtils;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.metadata.event.MetadataEvent;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.CYCLE_REPORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_DEFINITION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_PERIOD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_TIMES_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SYNC_REPORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SystemProperty.USER_HOME;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED_EXPORT_SERVICE;
import static org.apache.dubbo.common.utils.StringUtils.replace;
import static org.apache.dubbo.metadata.report.support.Constants.CACHE;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_CYCLE_REPORT;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_PERIOD;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_TIMES;
import static org.apache.dubbo.metadata.report.support.Constants.DUBBO_METADATA;

public abstract class AbstractMetadataReport implements MetadataReport {

    protected static final String DEFAULT_ROOT = "dubbo";

    protected static final int ONE_DAY_IN_MILLISECONDS = 60 * 24 * 60 * 1000;
    private static final int FOUR_HOURS_IN_MILLISECONDS = 60 * 4 * 60 * 1000;
    // Log output
    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    // Local disk cache, where the special key value.registries records the list of metadata centers, and the others are
    // the list of notified service providers
    final Properties properties = new Properties();
    private final ExecutorService reportCacheExecutor =
            Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveMetadataReport", true));
    final Map<MetadataIdentifier, Object> allMetadataReports = new ConcurrentHashMap<>(4);

    private final AtomicLong lastCacheChanged = new AtomicLong();
    final Map<MetadataIdentifier, Object> failedReports = new ConcurrentHashMap<>(4);
    private URL reportURL;
    boolean syncReport;
    // Local disk cache file
    File file;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    public MetadataReportRetry metadataReportRetry;
    private ScheduledExecutorService reportTimerScheduler;

    private final boolean reportMetadata;
    private final boolean reportDefinition;
    protected ApplicationModel applicationModel;

    public AbstractMetadataReport(URL reportServerURL) {
        setUrl(reportServerURL);
        applicationModel = reportServerURL.getOrDefaultApplicationModel();

        boolean localCacheEnabled = reportServerURL.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        // Start file save timer
        String defaultFilename = SystemPropertyConfigUtils.getSystemProperty(USER_HOME) + DUBBO_METADATA
                + reportServerURL.getApplication()
                + "-" + replace(reportServerURL.getAddress(), ":", "-")
                + CACHE;
        String filename = reportServerURL.getParameter(FILE_KEY, defaultFilename);
        File file = null;
        if (localCacheEnabled && ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists()
                    && file.getParentFile() != null
                    && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid service store file " + file
                            + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
            // if this file exists, firstly delete it.
            if (!initialized.getAndSet(true) && file.exists()) {
                file.delete();
            }
        }
        this.file = file;
        loadProperties();
        syncReport = reportServerURL.getParameter(SYNC_REPORT_KEY, false);
        metadataReportRetry = new MetadataReportRetry(
                reportServerURL.getParameter(RETRY_TIMES_KEY, DEFAULT_METADATA_REPORT_RETRY_TIMES),
                reportServerURL.getParameter(RETRY_PERIOD_KEY, DEFAULT_METADATA_REPORT_RETRY_PERIOD));
        // cycle report the data switch
        if (reportServerURL.getParameter(CYCLE_REPORT_KEY, DEFAULT_METADATA_REPORT_CYCLE_REPORT)) {
            reportTimerScheduler = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("DubboMetadataReportTimer", true));
            reportTimerScheduler.scheduleAtFixedRate(
                    this::publishAll, calculateStartTime(), ONE_DAY_IN_MILLISECONDS, TimeUnit.MILLISECONDS);
        }

        this.reportMetadata = reportServerURL.getParameter(REPORT_METADATA_KEY, false);
        this.reportDefinition = reportServerURL.getParameter(REPORT_DEFINITION_KEY, true);
    }

    public URL getUrl() {
        return reportURL;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("metadataReport url == null");
        }
        this.reportURL = url;
    }

    private void doSaveProperties(long version) {
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                    FileChannel channel = raf.getChannel()) {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    throw new IOException(
                            "Can not lock the metadataReport cache file " + file.getAbsolutePath()
                                    + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.metadata.file=xxx.properties");
                }
                // Save
                try {
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    Properties tmpProperties;
                    if (!syncReport) {
                        // When syncReport = false, properties.setProperty and properties.store are called from the same
                        // thread(reportCacheExecutor), so deep copy is not required
                        tmpProperties = properties;
                    } else {
                        // Using store method and setProperty method of the this.properties will cause lock contention
                        // under multi-threading, so deep copy a new container
                        tmpProperties = new Properties();
                        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
                        for (Map.Entry<Object, Object> entry : entries) {
                            tmpProperties.setProperty((String) entry.getKey(), (String) entry.getValue());
                        }
                    }

                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        tmpProperties.store(outputFile, "Dubbo metadataReport Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                reportCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn(
                    COMMON_UNEXPECTED_EXCEPTION,
                    "",
                    "",
                    "Failed to save service store file, cause: " + e.getMessage(),
                    e);
        }
    }

    void loadProperties() {
        if (file != null && file.exists()) {
            try (InputStream in = new FileInputStream(file)) {
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load service store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", "Failed to load service store file" + file, e);
            }
        }
    }

    /**
     * 保存元数据到本地属性文件缓存，支持同步和异步两种写入模式。
     * <p>
     * 该方法负责将元数据更新到内存中的Properties对象，并根据sync参数决定是立即同步写入磁盘还是异步批量写入。
     * 采用版本号机制（lastCacheChanged）追踪缓存变更，确保SaveProperties任务能够检测到数据变化并执行持久化操作。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>检查是否配置了本地缓存文件（file字段），如果未配置则直接返回，不执行任何操作</li>
     *   <li>根据add参数决定操作类型：
     *     <ul>
     *       <li>add=true：将元数据键值对添加到Properties中，键为metadataIdentifier的唯一标识，值为序列化后的元数据</li>
     *       <li>add=false：从Properties中移除该元数据键值对，用于清理过期数据</li>
     *     </ul>
     *   </li>
     *   <li>递增lastCacheChanged版本号，生成新的版本标识</li>
     *   <li>根据sync参数选择执行方式：
     *     <ul>
     *       <li>sync=true：在当前线程直接执行SaveProperties任务，同步写入磁盘</li>
     *       <li>sync=false：将SaveProperties任务提交到reportCacheExecutor线程池异步执行</li>
     *     </ul>
     *   </li>
     *   <li>捕获所有Throwable异常并记录警告日志，确保单个元数据的保存失败不影响其他操作</li>
     * </ol>
     * </p>
     *
     * @param metadataIdentifier 元数据的唯一标识符，包含服务接口、版本、分组等信息
     * @param value 序列化后的元数据内容（通常为JSON字符串）
     * @param add 标识是添加还是删除操作，true表示添加/更新，false表示删除
     * @param sync 标识是否同步执行，true表示立即写入磁盘，false表示异步批量写入
     */
    private void saveProperties(MetadataIdentifier metadataIdentifier, String value, boolean add, boolean sync) {
        if (file == null) {
            return;
        }

        try {
            if (add) {
                properties.setProperty(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), value);
            } else {
                properties.remove(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            }
            /*
             * 递增缓存变更版本号，触发持久化任务
             */
            long version = lastCacheChanged.incrementAndGet();
            if (sync) {
                /*
                 * 同步模式：在当前线程立即执行保存操作
                 */
                new SaveProperties(version).run();
            } else {
                /*
                 * 异步模式：提交到线程池批量执行保存操作
                 */
                reportCacheExecutor.execute(new SaveProperties(version));
            }

        } catch (Throwable t) {
            logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", t.getMessage(), t);
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

    @Override
    public void storeProviderMetadata(
            MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        if (syncReport) {
            storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition);
        } else {
            reportCacheExecutor.execute(() -> storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition));
        }
    }

    private void storeProviderMetadataTask(
            MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {

        MetadataEvent metadataEvent = MetadataEvent.toServiceSubscribeEvent(
                applicationModel, providerMetadataIdentifier.getUniqueServiceName());
        MetricsEventBus.post(
                metadataEvent,
                () -> {
                    boolean result = true;
                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("[METADATA_REGISTER] store provider metadata. Identifier : "
                                    + providerMetadataIdentifier + "; definition: " + serviceDefinition);
                        }
                        allMetadataReports.put(providerMetadataIdentifier, serviceDefinition);
                        failedReports.remove(providerMetadataIdentifier);
                        String data = JsonUtils.toJson(serviceDefinition);
                        doStoreProviderMetadata(providerMetadataIdentifier, data);
                        saveProperties(providerMetadataIdentifier, data, true, !syncReport);
                    } catch (Exception e) {
                        // retry again. If failed again, throw exception.
                        failedReports.put(providerMetadataIdentifier, serviceDefinition);
                        metadataReportRetry.startRetryTask();
                        logger.error(
                                PROXY_FAILED_EXPORT_SERVICE,
                                "",
                                "",
                                "Failed to put provider metadata " + providerMetadataIdentifier + " in  "
                                        + serviceDefinition + ", cause: " + e.getMessage(),
                                e);
                        result = false;
                    }
                    return result;
                },
                aBoolean -> aBoolean);
    }

    @Override
    public void storeConsumerMetadata(
            MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        if (syncReport) {
            storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap);
        } else {
            reportCacheExecutor.execute(
                    () -> storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap));
        }
    }

    /**
     * 执行消费者元数据的存储任务，支持失败重试机制。
     * <p>
     * 该方法负责将消费者的元数据信息（如服务接口、版本、分组、配置参数等）持久化到元数据存储中心。
     * 采用"先成功后清理"的策略：先将元数据存入allMetadataReports缓存，然后尝试持久化，如果成功则从failedReports中移除，
     * 如果失败则加入failedReports并触发重试任务。
     * </p>
     * <p>
     * 处理流程：
     * <ol>
     *   <li>记录INFO级别的日志，输出即将存储的消费者元数据标识和定义内容</li>
     *   <li>将元数据放入allMetadataReports全量缓存中，用于后续查询和管理</li>
     *   <li>从failedReports失败缓存中移除该标识，假设本次操作可能成功</li>
     *   <li>将serviceParameterMap序列化为JSON字符串</li>
     *   <li>调用doStoreConsumerMetadata()模板方法执行具体的持久化操作（由子类实现，如Zookeeper、Nacos等）</li>
     *   <li>调用saveProperties()保存元数据到本地属性文件，根据syncReport参数决定是同步还是异步执行</li>
     *   <li>如果任何步骤发生异常：
     *     <ul>
     *       <li>将元数据重新放入failedReports失败缓存</li>
     *       <li>调用metadataReportRetry.startRetryTask()启动或唤醒重试任务，定期重试失败的存储操作</li>
     *       <li>记录ERROR级别的日志，包含完整的错误堆栈信息</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     *
     * @param consumerMetadataIdentifier 消费者元数据的唯一标识符，包含服务接口、版本、分组、应用名称等信息
     * @param serviceParameterMap 消费者的配置参数映射表，包含超时时间、重试次数、负载均衡策略等配置项
     */
    protected void storeConsumerMetadataTask(
            MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("[METADATA_REGISTER] store consumer metadata. Identifier : " + consumerMetadataIdentifier
                        + "; definition: " + serviceParameterMap);
            }
            /*
             * 将元数据存入全量缓存，并从失败缓存中移除（假设本次可能成功）
             */
            allMetadataReports.put(consumerMetadataIdentifier, serviceParameterMap);
            failedReports.remove(consumerMetadataIdentifier);

            /*
             * 序列化参数Map为JSON格式
             */
            String data = JsonUtils.toJson(serviceParameterMap);
            /*
             * 调用子类实现的模板方法执行具体的持久化操作
             */
            doStoreConsumerMetadata(consumerMetadataIdentifier, data);
            /*
             * 保存元数据到本地属性文件，根据syncReport参数决定同步或异步执行
             */
            saveProperties(consumerMetadataIdentifier, data, true, !syncReport);
        } catch (Exception e) {
            // retry again. If failed again, throw exception.
            /*
             * 存储失败时，将元数据加入失败缓存并触发重试任务
             */
            failedReports.put(consumerMetadataIdentifier, serviceParameterMap);
            metadataReportRetry.startRetryTask();
            logger.error(
                    PROXY_FAILED_EXPORT_SERVICE,
                    "",
                    "",
                    "Failed to put consumer metadata " + consumerMetadataIdentifier + ";  " + serviceParameterMap
                            + ", cause: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void destroy() {
        if (reportCacheExecutor != null) {
            reportCacheExecutor.shutdown();
        }
        if (reportTimerScheduler != null) {
            reportTimerScheduler.shutdown();
        }
        if (metadataReportRetry != null) {
            metadataReportRetry.destroy();
            metadataReportRetry = null;
        }
    }

    @Override
    public void saveServiceMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        if (syncReport) {
            doSaveMetadata(metadataIdentifier, url);
        } else {
            reportCacheExecutor.execute(() -> doSaveMetadata(metadataIdentifier, url));
        }
    }

    @Override
    public void removeServiceMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        if (syncReport) {
            doRemoveMetadata(metadataIdentifier);
        } else {
            reportCacheExecutor.execute(() -> doRemoveMetadata(metadataIdentifier));
        }
    }

    @Override
    public List<String> getExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        // TODO, fallback to local cache
        return doGetExportedURLs(metadataIdentifier);
    }

    @Override
    public void saveSubscribedData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, Set<String> urls) {
        if (syncReport) {
            doSaveSubscriberData(subscriberMetadataIdentifier, JsonUtils.toJson(urls));
        } else {
            reportCacheExecutor.execute(
                    () -> doSaveSubscriberData(subscriberMetadataIdentifier, JsonUtils.toJson(urls)));
        }
    }

    @Override
    public List<String> getSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        String content = doGetSubscribedURLs(subscriberMetadataIdentifier);
        return JsonUtils.toJavaList(content, String.class);
    }

    String getProtocol(URL url) {
        String protocol = url.getSide();
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    /**
     * @return if need to continue
     */
    public boolean retry() {
        return doHandleMetadataCollection(failedReports);
    }

    @Override
    public boolean shouldReportDefinition() {
        return reportDefinition;
    }

    @Override
    public boolean shouldReportMetadata() {
        return reportMetadata;
    }

    private boolean doHandleMetadataCollection(Map<MetadataIdentifier, Object> metadataMap) {
        if (metadataMap.isEmpty()) {
            return true;
        }
        Iterator<Map.Entry<MetadataIdentifier, Object>> iterable =
                metadataMap.entrySet().iterator();
        while (iterable.hasNext()) {
            Map.Entry<MetadataIdentifier, Object> item = iterable.next();
            if (PROVIDER_SIDE.equals(item.getKey().getSide())) {
                this.storeProviderMetadata(item.getKey(), (FullServiceDefinition) item.getValue());
            } else if (CONSUMER_SIDE.equals(item.getKey().getSide())) {
                this.storeConsumerMetadata(item.getKey(), (Map) item.getValue());
            }
        }
        return false;
    }

    /**
     * not private. just for unittest.
     */
    void publishAll() {
        logger.info("start to publish all metadata.");
        this.doHandleMetadataCollection(allMetadataReports);
    }

    /**
     * between 2:00 am to 6:00 am, the time is random.
     *
     * @return
     */
    long calculateStartTime() {
        Calendar calendar = Calendar.getInstance();
        long nowMill = calendar.getTimeInMillis();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long subtract = calendar.getTimeInMillis() + ONE_DAY_IN_MILLISECONDS - nowMill;
        return subtract
                + (FOUR_HOURS_IN_MILLISECONDS / 2)
                + ThreadLocalRandom.current().nextInt(FOUR_HOURS_IN_MILLISECONDS);
    }

    class MetadataReportRetry {
        protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

        final ScheduledExecutorService retryExecutor =
                Executors.newScheduledThreadPool(0, new NamedThreadFactory("DubboMetadataReportRetryTimer", true));
        volatile ScheduledFuture retryScheduledFuture;
        final AtomicInteger retryCounter = new AtomicInteger(0);
        // retry task schedule period
        long retryPeriod;
        // if no failed report, wait how many times to run retry task.
        int retryTimesIfNonFail = 600;

        int retryLimit;

        public MetadataReportRetry(int retryTimes, int retryPeriod) {
            this.retryPeriod = retryPeriod;
            this.retryLimit = retryTimes;
        }

        /**
         * 启动元数据报告的定时重试任务，采用双重检查锁定保证任务只被调度一次。
         * <p>
         * 该方法在元数据存储失败时被调用，用于启动后台重试线程定期尝试重新提交失败的元数据报告。
         * 使用scheduleWithFixedDelay()创建固定延迟的周期性任务，无论前一次执行耗时多久，
         * 两次执行之间都会间隔固定的retryPeriod时间。
         * </p>
         * <p>
         * 重试任务的终止条件（满足任一即停止）：
         * <ul>
         *   <li><b>成功且超过最小重试次数</b>：retry()返回true（所有失败报告重传成功）且当前重试次数大于retryTimesIfNonFail</li>
         *   <li><b>超过最大重试次数限制</b>：当前重试次数大于retryLimit，防止无限重试消耗资源</li>
         * </ul>
         * </p>
         * <p>
         * 线程安全：通过synchronized(retryCounter)和双重检查锁定模式，确保在并发场景下只会创建一个重试任务，
         * 避免重复调度导致的资源浪费。
         * </p>
         */
        void startRetryTask() {
            if (retryScheduledFuture == null) {
                synchronized (retryCounter) {
                    if (retryScheduledFuture == null) {
                        retryScheduledFuture = retryExecutor.scheduleWithFixedDelay(
                                () -> {
                                    // Check and connect to the metadata
                                    try {
                                        /*
                                         * 递增重试计数器并记录日志
                                         */
                                        int times = retryCounter.incrementAndGet();
                                        logger.info("start to retry task for metadata report. retry times:" + times);
                                        /*
                                         * 如果重试成功且超过最小重试次数，则取消任务
                                         */
                                        if (retry() && times > retryTimesIfNonFail) {
                                            cancelRetryTask();
                                        }
                                        /*
                                         * 如果超过最大重试次数限制，强制取消任务
                                         */
                                        if (times > retryLimit) {
                                            cancelRetryTask();
                                        }
                                    } catch (Throwable t) { // Defensive fault tolerance
                                        logger.error(
                                                COMMON_UNEXPECTED_EXCEPTION,
                                                "",
                                                "",
                                                "Unexpected error occur at failed retry, cause: " + t.getMessage(),
                                                t);
                                    }
                                },
                                500,
                                retryPeriod,
                                TimeUnit.MILLISECONDS);
                    }
                }
            }
        }

        void cancelRetryTask() {
            if (retryScheduledFuture != null) {
                retryScheduledFuture.cancel(false);
            }
            retryExecutor.shutdown();
        }

        void destroy() {
            cancelRetryTask();
        }

        /**
         * @deprecated only for test
         */
        @Deprecated
        ScheduledExecutorService getRetryExecutor() {
            return retryExecutor;
        }
    }

    private void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, List<String> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }
        List<String> encodedUrlList = new ArrayList<>(urls.size());
        for (String url : urls) {
            encodedUrlList.add(URL.encode(url));
        }
        doSaveSubscriberData(subscriberMetadataIdentifier, encodedUrlList);
    }

    protected abstract void doStoreProviderMetadata(
            MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions);

    protected abstract void doStoreConsumerMetadata(
            MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString);

    protected abstract void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url);

    protected abstract void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract void doSaveSubscriberData(
            SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr);

    protected abstract String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier);

    /**
     * @deprecated only for unit test
     */
    @Deprecated
    protected ExecutorService getReportCacheExecutor() {
        return reportCacheExecutor;
    }

    /**
     * @deprecated only for unit test
     */
    @Deprecated
    protected MetadataReportRetry getMetadataReportRetry() {
        return metadataReportRetry;
    }
}
