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
package com.alibaba.dubbo.registry.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ExecutorUtil;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.registry.NotifyListener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * FailbackRegistry. (SPI, Prototype, ThreadSafe)
 *
 * 实现 AbstractRegistry 抽象类，支持失败重试的 Registry 抽象类。
 * AbstractRegistry 进行的注册、订阅等操作，更多的是修改状态，而无和注册中心实际的操作。FailbackRegistry 在 AbstractRegistry 的基础上，实现了和注册中心实际的操作，并且支持失败重试的特性。
 *
 */
public abstract class FailbackRegistry extends AbstractRegistry {

    /**
     * 定时任务执行器
     */
    // Scheduled executor service
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("DubboRegistryFailedRetryTimer", true));

    /**
     * 失败重试定时器，定时检查是否有请求失败，如有，无限次重试
     */
    // Timer for failure retry, regular check if there is a request for failure, and if there is, an unlimited retry
    private final ScheduledFuture<?> retryFuture;

    /**
     * 发起注册失败的 URL 集合
     */
    private final Set<URL> failedRegistered = new ConcurrentHashSet<URL>();

    /**
     * 取消注册失败的 URL 集合
     */
    private final Set<URL> failedUnregistered = new ConcurrentHashSet<URL>();

    /**
     * 发起订阅失败的监听器集合
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> failedSubscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();

    /**
     * 取消订阅失败的监听器集合
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> failedUnsubscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();

    /**
     * 失败通知的 URL 集合
     */
    private final ConcurrentMap<URL, Map<NotifyListener, List<URL>>> failedNotified = new ConcurrentHashMap<URL, Map<NotifyListener, List<URL>>>();

    /**
     * retryExecutor重试执行器将等待的时间（毫秒）
     * The time in milliseconds the retryExecutor will wait
     */
    private final int retryPeriod;

    public FailbackRegistry(URL url) {
        // 设置注册中心 URL
        super(url);
        // 重试频率，默认5 * 1000，单位：毫秒
        this.retryPeriod = url.getParameter(Constants.REGISTRY_RETRY_PERIOD_KEY, Constants.DEFAULT_REGISTRY_RETRY_PERIOD);
        // 创建失败重试定时器
        this.retryFuture = retryExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                // Check and connect to the registry
                try {
                    retry();
                } catch (Throwable t) { // Defensive fault tolerance
                    logger.error("Unexpected error occur at failed retry, cause: " + t.getMessage(), t);
                }
            }
        }, retryPeriod, retryPeriod, TimeUnit.MILLISECONDS);
    }

    public Future<?> getRetryFuture() {
        return retryFuture;
    }

    public Set<URL> getFailedRegistered() {
        return failedRegistered;
    }

    public Set<URL> getFailedUnregistered() {
        return failedUnregistered;
    }

    public Map<URL, Set<NotifyListener>> getFailedSubscribed() {
        return failedSubscribed;
    }

    public Map<URL, Set<NotifyListener>> getFailedUnsubscribed() {
        return failedUnsubscribed;
    }

    public Map<URL, Map<NotifyListener, List<URL>>> getFailedNotified() {
        return failedNotified;
    }

    private void addFailedSubscribed(URL url, NotifyListener listener) {
        Set<NotifyListener> listeners = failedSubscribed.get(url);
        if (listeners == null) {
            failedSubscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
            listeners = failedSubscribed.get(url);
        }
        listeners.add(listener);
    }

    // 移除出 `failedSubscribed` `failedUnsubscribed` `failedNotified`
    private void removeFailedSubscribed(URL url, NotifyListener listener) {
        Set<NotifyListener> listeners = failedSubscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
        listeners = failedUnsubscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
        Map<NotifyListener, List<URL>> notified = failedNotified.get(url);
        if (notified != null) {
            notified.remove(listener);
        }
    }

    @Override
    public void register(URL url) {
        // 添加到 `registered` 变量
        super.register(url);
        // 移除出 `failedRegistered` `failedUnregistered` 变量
        failedRegistered.remove(url);
        failedUnregistered.remove(url);
        try {
            // 向注册中心发送注册请求
            // Sending a registration request to the server side
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;
            // 如果开启了启动时检测，则直接抛出异常
            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && !Constants.CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to register " + url + " to registry " + getUrl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的注册请求记录到 `failedRegistered`，定时重试
            // Record a failed registration request to a failed list, retry regularly
            failedRegistered.add(url);
        }
    }

    @Override
    public void unregister(URL url) {
        // 从 registered 集合移除 注册中心 URL
        super.unregister(url);
        // 移除出 `failedRegistered` `failedUnregistered` 变量
        failedRegistered.remove(url);
        failedUnregistered.remove(url);
        try {
            // 向注册中心发送取消注册请求
            // Sending a cancellation request to the server side
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // 如果开启了启动时检测，则直接抛出异常
            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true)
                    && !Constants.CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unregister " + url + " to registry " + getUrl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to uregister " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的取消注册请求记录到 `failedUnregistered`，定时重试
            // Record a failed registration request to a failed list, retry regularly
            failedUnregistered.add(url);
        }
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        // 添加到 `subscribed` 变量
        super.subscribe(url, listener);
        // 移除出 `failedSubscribed` `failedUnsubscribed` `failedNotified`
        removeFailedSubscribed(url, listener);
        try {
            // 向注册中心发送订阅请求
            // Sending a subscription request to the server side
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;
            // 如果有缓存的 URL 集合，进行通知。后续订阅成功后，会使用最新的 URL 集合，进行通知。
            List<URL> urls = getCacheUrls(url);
            if (urls != null && !urls.isEmpty()) {
                // 通知
                notify(url, listener, urls);
                logger.error("Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: " + getUrl().getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/dubbo-registry-" + url.getHost() + ".cache") + ", cause: " + t.getMessage(), t);
            } else {

                // 如果开启了启动时检测，则直接抛出异常
                // If the startup detection is opened, the Exception is thrown directly.
                boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                        && url.getParameter(Constants.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackWrapperException;
                if (check || skipFailback) {
                    if (skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    logger.error("Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
                }
            }

            // 将失败的订阅请求记录到 `failedSubscribed`，定时重试
            // Record a failed registration request to a failed list, retry regularly
            addFailedSubscribed(url, listener);
        }
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        // 移除出 `subscribed` 变量
        super.unsubscribe(url, listener);
        // 移除出 `failedSubscribed` `failedUnsubscribed` `failedNotified`
        removeFailedSubscribed(url, listener);
        try {
            // 向注册中心发送取消订阅请求
            // Sending a canceling subscription request to the server side
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // 如果开启了启动时检测，则直接抛出异常
            // If the startup detection is opened, the Exception is thrown directly.
            boolean check = getUrl().getParameter(Constants.CHECK_KEY, true)
                    && url.getParameter(Constants.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException("Failed to unsubscribe " + url + " to registry " + getUrl().getAddress() + ", cause: " + t.getMessage(), t);
            } else {
                logger.error("Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // 将失败的取消订阅请求记录到 `failedUnsubscribed`，定时重试
            // Record a failed registration request to a failed list, retry regularly
            Set<NotifyListener> listeners = failedUnsubscribed.get(url);
            if (listeners == null) {
                failedUnsubscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
                listeners = failedUnsubscribed.get(url);
            }
            listeners.add(listener);
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
            // 通知监听器
            doNotify(url, listener, urls);
        } catch (Exception t) {
            // 将失败的通知记录到 `failedNotified`，定时重试
            // Record a failed registration request to a failed list, retry regularly
            Map<NotifyListener, List<URL>> listeners = failedNotified.get(url);
            if (listeners == null) {
                failedNotified.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, List<URL>>());
                listeners = failedNotified.get(url);
            }
            listeners.put(listener, urls);
            logger.error("Failed to notify for subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
        }
    }

    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {
        super.notify(url, listener, urls);
    }

    /**
     * 完全覆盖父类方法( 即不像前面几个方法，会调用父类的方法 )，将需要注册和订阅的 URL 添加到 failedRegistered failedSubscribed 属性中。这样，在 #retry() 方法中，会重试进行连接。
     * @throws Exception
     */
    @Override
    protected void recover() throws Exception {
        // register 恢复注册，添加到 `failedRegistered` ，定时重试
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                failedRegistered.add(url);
            }
        }
        // subscribe 恢复订阅，添加到 `failedSubscribed` ，定时重试
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    /**
     * 重试，遍历五个 failedXXX 属性，重试对应的操作。
     */
    // Retry the failed actions
    protected void retry() {
        // 重试执行注册
        if (!failedRegistered.isEmpty()) {
            // 避免并发冲突
            Set<URL> failed = new HashSet<URL>(failedRegistered);
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry register " + failed);
                }
                try {
                    for (URL url : failed) {
                        try {
                            doRegister(url);
                            failedRegistered.remove(url);
                        } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                            logger.warn("Failed to retry register " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                        }
                    }
                } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                    logger.warn("Failed to retry register " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        // 重试执行取消注册
        if (!failedUnregistered.isEmpty()) {
            // 避免并发冲突
            Set<URL> failed = new HashSet<URL>(failedUnregistered);
            if (!failed.isEmpty()) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry unregister " + failed);
                }
                try {
                    for (URL url : failed) {
                        try {
                            doUnregister(url);
                            failedUnregistered.remove(url);
                        } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                            logger.warn("Failed to retry unregister  " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                        }
                    }
                } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                    logger.warn("Failed to retry unregister  " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        // 重试执行订阅
        if (!failedSubscribed.isEmpty()) {
            // 避免并发冲突
            Map<URL, Set<NotifyListener>> failed = new HashMap<URL, Set<NotifyListener>>(failedSubscribed);
            for (Map.Entry<URL, Set<NotifyListener>> entry : new HashMap<URL, Set<NotifyListener>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry subscribe " + failed);
                }
                try {
                    for (Map.Entry<URL, Set<NotifyListener>> entry : failed.entrySet()) {
                        URL url = entry.getKey();
                        Set<NotifyListener> listeners = entry.getValue();
                        for (NotifyListener listener : listeners) {
                            try {
                                // 执行订阅
                                doSubscribe(url, listener);
                                // 移除出监听器
                                listeners.remove(listener);
                            } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                                logger.warn("Failed to retry subscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                    logger.warn("Failed to retry subscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        // 重试执行取消订阅
        if (!failedUnsubscribed.isEmpty()) {
            // 避免并发冲突
            Map<URL, Set<NotifyListener>> failed = new HashMap<URL, Set<NotifyListener>>(failedUnsubscribed);
            for (Map.Entry<URL, Set<NotifyListener>> entry : new HashMap<URL, Set<NotifyListener>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry unsubscribe " + failed);
                }
                try {
                    for (Map.Entry<URL, Set<NotifyListener>> entry : failed.entrySet()) {
                        URL url = entry.getKey();
                        Set<NotifyListener> listeners = entry.getValue();
                        for (NotifyListener listener : listeners) {
                            try {
                                // 执行取消订阅
                                doUnsubscribe(url, listener);
                                // 移除出监听器
                                listeners.remove(listener);
                            } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                                logger.warn("Failed to retry unsubscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                    logger.warn("Failed to retry unsubscribe " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
        // 重试执行通知监听器
        if (!failedNotified.isEmpty()) {
            Map<URL, Map<NotifyListener, List<URL>>> failed = new HashMap<URL, Map<NotifyListener, List<URL>>>(failedNotified);
            for (Map.Entry<URL, Map<NotifyListener, List<URL>>> entry : new HashMap<URL, Map<NotifyListener, List<URL>>>(failed).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    failed.remove(entry.getKey());
                }
            }
            if (failed.size() > 0) {
                if (logger.isInfoEnabled()) {
                    logger.info("Retry notify " + failed);
                }
                try {
                    for (Map<NotifyListener, List<URL>> values : failed.values()) {
                        for (Map.Entry<NotifyListener, List<URL>> entry : values.entrySet()) {
                            try {
                                NotifyListener listener = entry.getKey();
                                List<URL> urls = entry.getValue();
                                // 通知监听器
                                listener.notify(urls);
                                // 移除出监听器
                                values.remove(listener);
                            } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                                logger.warn("Failed to retry notify " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                            }
                        }
                    }
                } catch (Throwable t) { // Ignore all the exceptions and wait for the next retry
                    logger.warn("Failed to retry notify " + failed + ", waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
    }

    /**
     * 取消注册和订阅，并关闭定时器。
     */
    @Override
    public void destroy() {
        // 取消注册和订阅
        super.destroy();
        try {
            // 关闭定时器。
            retryFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        // 关闭定时任务执行器
        ExecutorUtil.gracefulShutdown(retryExecutor, retryPeriod);
    }

    // ==== Template method ====

    protected abstract void doRegister(URL url);

    protected abstract void doUnregister(URL url);

    protected abstract void doSubscribe(URL url, NotifyListener listener);

    protected abstract void doUnsubscribe(URL url, NotifyListener listener);

}
