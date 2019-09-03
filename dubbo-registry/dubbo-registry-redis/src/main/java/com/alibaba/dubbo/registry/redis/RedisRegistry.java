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
package com.alibaba.dubbo.registry.redis;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ExecutorUtil;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.rpc.RpcException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RedisRegistry
 *
 * 实现 FailbackRegistry 抽象类，基于 Redis 实现的注册中心实现类。
 *
 */
public class RedisRegistry extends FailbackRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RedisRegistry.class);

    /**
     * 默认端口
     */
    private static final int DEFAULT_REDIS_PORT = 6379;

    /**
     * 默认 Redis 根节点
     */
    private final static String DEFAULT_ROOT = "dubbo";

    /**
     * Redis Key 过期机制执行器
     * 1、该任务主要有两个逻辑：1）延长未过期的 Key ；2）删除过期的 Key 。
     * 2、任务间隔为 expirePeriod 的一半，避免过于频繁，对 Redis 的压力过大；同时，避免过于不频繁，每次执行时，都过期了。
     */
    private final ScheduledExecutorService expireExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("DubboRegistryExpireTimer", true));

    /**
     *  表示ScheduledExecutorService中提交了任务的返回结果。我们通过Delayed的接口getDelay()方法知道该任务还有好久才被执行
     *  Redis Key 过期机制 Future
     */
    private final ScheduledFuture<?> expireFuture;

    /**
     * Redis 根节点
     */
    private final String root;

    /**
     * JedisPool 集合
     * key：ip:port
     */
    private final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<String, JedisPool>();

    /**
     * 通知器集合
     * key：Root + Service ，例如 `/dubbo/com.alibaba.dubbo.demo.DemoService`
     * Notifier 用于 Redis Publish/Subscribe 机制中的订阅，实时监听数据的变化。
     */
    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<String, Notifier>();

    /**
     * 重连周期，单位：毫秒。用于订阅发生 Redis 连接异常时，Notifier sleep ，等待重连上。
     */
    private final int reconnectPeriod;

    /**
     * 过期周期，单位：毫秒
     */
    private final int expirePeriod;

    /**
     * 是否监控中心
     * 用于判断脏数据，脏数据由监控中心删除 {@link #clean(Jedis)}
     */
    private volatile boolean admin = false;

    /**
     * 是否复制模式
     */
    private boolean replicate;

    public RedisRegistry(URL url) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setTestOnBorrow(url.getParameter("test.on.borrow", true));
        config.setTestOnReturn(url.getParameter("test.on.return", false));
        config.setTestWhileIdle(url.getParameter("test.while.idle", false));
        if (url.getParameter("max.idle", 0) > 0)
            config.setMaxIdle(url.getParameter("max.idle", 0));
        if (url.getParameter("min.idle", 0) > 0)
            config.setMinIdle(url.getParameter("min.idle", 0));
        if (url.getParameter("max.active", 0) > 0)
            config.setMaxTotal(url.getParameter("max.active", 0));
        if (url.getParameter("max.total", 0) > 0)
            config.setMaxTotal(url.getParameter("max.total", 0));
        if (url.getParameter("max.wait", url.getParameter("timeout", 0)) > 0)
            config.setMaxWaitMillis(url.getParameter("max.wait", url.getParameter("timeout", 0)));
        if (url.getParameter("num.tests.per.eviction.run", 0) > 0)
            config.setNumTestsPerEvictionRun(url.getParameter("num.tests.per.eviction.run", 0));
        if (url.getParameter("time.between.eviction.runs.millis", 0) > 0)
            config.setTimeBetweenEvictionRunsMillis(url.getParameter("time.between.eviction.runs.millis", 0));
        if (url.getParameter("min.evictable.idle.time.millis", 0) > 0)
            config.setMinEvictableIdleTimeMillis(url.getParameter("min.evictable.idle.time.millis", 0));

        // 是否复制模式
        String cluster = url.getParameter("cluster", "failover");
        if (!"failover".equals(cluster) && !"replicate".equals(cluster)) {
            throw new IllegalArgumentException("Unsupported redis cluster: " + cluster + ". The redis cluster only supported failover or replicate.");
        }
        replicate = "replicate".equals(cluster);

        // 解析注册中心地址
        List<String> addresses = new ArrayList<String>();
        addresses.add(url.getAddress());
        String[] backups = url.getParameter(Constants.BACKUP_KEY, new String[0]);
        if (backups != null && backups.length > 0) {
            addresses.addAll(Arrays.asList(backups));
        }

        // 创建 JedisPool 对象
        for (String address : addresses) {
            int i = address.indexOf(':');
            String host;
            int port;
            if (i > 0) {
                host = address.substring(0, i);
                port = Integer.parseInt(address.substring(i + 1));
            } else {
                host = address;
                port = DEFAULT_REDIS_PORT;
            }
            // 加入JedisPool 集合
            this.jedisPools.put(address, new JedisPool(config, host, port,
                    url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT), StringUtils.isEmpty(url.getPassword()) ? null : url.getPassword(),
                    url.getParameter("db.index", 0)));
        }

        this.reconnectPeriod = url.getParameter(Constants.REGISTRY_RECONNECT_PERIOD_KEY, Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD);
        // 获得 Redis 根节点
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        // 头 `/`
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        // 尾 `/`
        if (!group.endsWith(Constants.PATH_SEPARATOR)) {
            group = group + Constants.PATH_SEPARATOR;
        }
        this.root = group;

        // 创建实现 Redis Key 过期机制的任务
        this.expirePeriod = url.getParameter(Constants.SESSION_TIMEOUT_KEY, Constants.DEFAULT_SESSION_TIMEOUT);
        this.expireFuture = expireExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    deferExpired(); // Extend the expiration time
                } catch (Throwable t) { // Defensive fault tolerance
                    logger.error("Unexpected exception occur at defer expire time, cause: " + t.getMessage(), t);
                }
            }
        }, expirePeriod / 2, expirePeriod / 2, TimeUnit.MILLISECONDS);
    }

    private void deferExpired() {
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    for (URL url : new HashSet<URL>(getRegistered())) {
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                            String key = toCategoryPath(url);
                            if (jedis.hset(key, url.toFullString(), String.valueOf(System.currentTimeMillis() + expirePeriod)) == 1) {
                                jedis.publish(key, Constants.REGISTER);
                            }
                        }
                    }
                    if (admin) {
                        clean(jedis);
                    }
                    if (!replicate) {
                        break;//  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                logger.warn("Failed to write provider heartbeat to redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }

    // The monitoring center is responsible for deleting outdated dirty data
    private void clean(Jedis jedis) {
        Set<String> keys = jedis.keys(root + Constants.ANY_VALUE);
        if (keys != null && !keys.isEmpty()) {
            for (String key : keys) {
                Map<String, String> values = jedis.hgetAll(key);
                if (values != null && values.size() > 0) {
                    boolean delete = false;
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        URL url = URL.valueOf(entry.getKey());
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                            long expire = Long.parseLong(entry.getValue());
                            if (expire < now) {
                                jedis.hdel(key, entry.getKey());
                                delete = true;
                                if (logger.isWarnEnabled()) {
                                    logger.warn("Delete expired key: " + key + " -> value: " + entry.getKey() + ", expire: " + new Date(expire) + ", now: " + new Date(now));
                                }
                            }
                        }
                    }
                    if (delete) {
                        jedis.publish(key, Constants.UNREGISTER);
                    }
                }
            }
        }
    }

    @Override
    public boolean isAvailable() {
        for (JedisPool jedisPool : jedisPools.values()) {
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    if (jedis.isConnected()) {
                        return true; // At least one single machine is available.
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            expireFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            for (Notifier notifier : notifiers.values()) {
                notifier.shutdown();
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                jedisPool.destroy();
            } catch (Throwable t) {
                logger.warn("Failed to destroy the redis registry client. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
        ExecutorUtil.gracefulShutdown(expireExecutor, expirePeriod);
    }

    @Override
    public void doRegister(URL url) {
        // 获得分类路径作为 Key，root + service + category
        String key = toCategoryPath(url);
        // 获得 URL 字符串作为 Value
        String value = url.toFullString();
        // 计算过期时间，当前时间 + expirePeriod
        String expire = String.valueOf(System.currentTimeMillis() + expirePeriod);
        boolean success = false;
        // 向 Redis 注册
        RpcException exception = null;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 写入 Redis Map 中。注意，过期时间，作为 Map 的值。
                    jedis.hset(key, value, expire);
                    // 发布 Redis 注册事件。这样订阅该 Key 的服务消费者和监控中心，就会实时从 Redis 读取该服务的最新数据。
                    jedis.publish(key, Constants.REGISTER);
                    success = true;
                    // 如果非 replicate ，意味着 Redis 服务器端已同步数据，只需写入单台机器。因此，结束循环。否则，满足 replicate ，向所有 Redis 写入。
                    if (!replicate) {
                        break; //  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to register service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        // 处理异常
        if (exception != null) {
            if (success) {
                // 虽然发生异常，但是结果成功
                logger.warn(exception.getMessage(), exception);
            } else {
                // 最终未成功
                throw exception;
            }
        }
    }

    /**
     * 当服务消费者或服务提供者，关闭时，会调用 #doUnregister(url) 方法，取消注册。
     * 在该方法中，会删除对应 Map 中的键 + 发布 unregister 事件，从而实时通知订阅者们。因此，正常情况下，就无需监控中心，做脏数据删除的工作。
     * @param url
     */
    @Override
    public void doUnregister(URL url) {
        // 获得分类路径作为 Key，root + service + category
        String key = toCategoryPath(url);
        // 获得 URL 字符串作为 Value
        String value = url.toFullString();
        RpcException exception = null;
        boolean success = false;
        // 向 Redis 取消注册
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 删除 Redis Map 键
                    jedis.hdel(key, value);
                    // 发布 Redis 取消注册事件
                    jedis.publish(key, Constants.UNREGISTER);
                    success = true;
                    //  如果服务器端已同步数据，只需写入单台机器
                    if (!replicate) {
                        break; //  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to unregister service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        // 处理异常
        if (exception != null) {
            if (success) {
                // 虽然发生异常，但是结果成功
                logger.warn(exception.getMessage(), exception);
            } else {
                // 最终未成功
                throw exception;
            }
        }
    }

    /**
     * 订阅动作，一定要在获取初始化数据之前。如果反过来，可能获取数据完后，处理的过程中，有数据的变更，我们就无法收到 register unregister 的事件
     * @param url
     * @param listener
     */
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        // 获得服务路径，例如：`/dubbo/com.alibaba.dubbo.demo.DemoService`
        String service = toServicePath(url);
        // 获得通知器 Notifier 对象
        Notifier notifier = notifiers.get(service);
        if (notifier == null) {
            // 不存在，则创建 Notifier 对象
            Notifier newNotifier = new Notifier(service);
            notifiers.putIfAbsent(service, newNotifier);
            notifier = notifiers.get(service);
            if (notifier == newNotifier) {
                // 保证并发的情况下，有且仅有一个启动
                notifier.start();
            }
        }
        boolean success = false;
        RpcException exception = null;
        // 循环 `jedisPools` ，仅向一个 Redis 发起订阅，直到一个成功。
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 处理所有 Service 层的发起订阅，例如监控中心的订阅
                    if (service.endsWith(Constants.ANY_VALUE)) {
                        // 只有监控中心，才清理脏数据。
                        admin = true;
                        // 获得分类层集合，例如：`/dubbo/com.alibaba.dubbo.demo.DemoService/providers`
                        Set<String> keys = jedis.keys(service);
                        if (keys != null && !keys.isEmpty()) {
                            // 按照服务聚合 URL 集合
                            Map<String, Set<String>> serviceKeys = new HashMap<String, Set<String>>();
                            for (String key : keys) {
                                String serviceKey = toServicePath(key);
                                Set<String> sk = serviceKeys.get(serviceKey);
                                if (sk == null) {
                                    sk = new HashSet<String>();
                                    serviceKeys.put(serviceKey, sk);
                                }
                                sk.add(key);
                            }
                            // 循环 serviceKeys ，按照每个 Service 层的发起通知
                            for (Set<String> sk : serviceKeys.values()) {
                                doNotify(jedis, sk, url, Arrays.asList(listener));
                            }
                        }
                    // 适用服务提供者和服务消费者，处理指定 Service 层的初始化数据
                    } else {
                        // 调用 Jedis#keys(pattern) 方法，获得指定 Service 层下的所有 URL 们，发起通知
                        doNotify(jedis, jedis.keys(service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE), url, Arrays.asList(listener));
                    }
                    // 标记成功
                    success = true;
                    // 结束，仅仅从一台服务器读取数据
                    break; // Just read one server's data
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) { // Try the next server
                exception = new RpcException("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        // 处理异常
        if (exception != null) {
            // 标记成功
            if (success) {
                // 虽然发生异常，但是结果成功
                logger.warn(exception.getMessage(), exception);
            } else {
                // 最终未成功
                throw exception;
            }
        }
    }

    /**
     * 此处应该增加取消向 Redis 的订阅( Subscribe ) 。在 ZookeeperRegistry 的该方法中，是移除了对应的监听器。
     * @param url
     * @param listener
     */
    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
    }

    /**
     *
     * @param jedis
     * @param key 分类数组，例如：`/dubbo/com.alibaba.dubbo.demo.DemoService/providers`
     */
    private void doNotify(Jedis jedis, String key) {
        // 遍历订阅 URL 的监听器集合
        for (Map.Entry<URL, Set<NotifyListener>> entry : new HashMap<URL, Set<NotifyListener>>(getSubscribed()).entrySet()) {
            doNotify(jedis, Arrays.asList(key), entry.getKey(), new HashSet<NotifyListener>(entry.getValue()));
        }
    }

    private void doNotify(Jedis jedis, Collection<String> keys, URL url, Collection<NotifyListener> listeners) {
        if (keys == null || keys.isEmpty()
                || listeners == null || listeners.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        List<URL> result = new ArrayList<URL>();
        List<String> categories = Arrays.asList(url.getParameter(Constants.CATEGORY_KEY, new String[0]));
        String consumerService = url.getServiceInterface();
        for (String key : keys) {
            if (!Constants.ANY_VALUE.equals(consumerService)) {
                String prvoiderService = toServiceName(key);
                if (!prvoiderService.equals(consumerService)) {
                    continue;
                }
            }
            String category = toCategoryName(key);
            if (!categories.contains(Constants.ANY_VALUE) && !categories.contains(category)) {
                continue;
            }
            List<URL> urls = new ArrayList<URL>();
            Map<String, String> values = jedis.hgetAll(key);
            if (values != null && values.size() > 0) {
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    URL u = URL.valueOf(entry.getKey());
                    if (!u.getParameter(Constants.DYNAMIC_KEY, true)
                            || Long.parseLong(entry.getValue()) >= now) {
                        if (UrlUtils.isMatch(url, u)) {
                            urls.add(u);
                        }
                    }
                }
            }
            if (urls.isEmpty()) {
                urls.add(url.setProtocol(Constants.EMPTY_PROTOCOL)
                        .setAddress(Constants.ANYHOST_VALUE)
                        .setPath(toServiceName(key))
                        .addParameter(Constants.CATEGORY_KEY, category));
            }
            result.addAll(urls);
            if (logger.isInfoEnabled()) {
                logger.info("redis notify: " + key + " = " + urls);
            }
        }
        if (result == null || result.isEmpty()) {
            return;
        }
        for (NotifyListener listener : listeners) {
            notify(url, listener, result);
        }
    }

    private String toServiceName(String categoryPath) {
        String servicePath = toServicePath(categoryPath);
        return servicePath.startsWith(root) ? servicePath.substring(root.length()) : servicePath;
    }

    private String toCategoryName(String categoryPath) {
        int i = categoryPath.lastIndexOf(Constants.PATH_SEPARATOR);
        return i > 0 ? categoryPath.substring(i + 1) : categoryPath;
    }

    /**
     * 获得服务路径，主要截掉多余的部分
     *
     * Root + Service
     *
     * @param categoryPath 分类路径
     * @return 服务路径
     */
    private String toServicePath(String categoryPath) {
        int i;
        if (categoryPath.startsWith(root)) {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR, root.length());
        } else {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR);
        }
        return i > 0 ? categoryPath.substring(0, i) : categoryPath;
    }

    /**
     * 获得服务路径
     *
     * Root + Service
     *
     * @param url URL
     * @return 服务路径
     */
    private String toServicePath(URL url) {
        return root + url.getServiceInterface();
    }

    /**
     * 获得分类路径
     *
     * Root + Service + Type
     *
     * @param url URL
     * @return 分类路径
     */
    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    private class NotifySub extends JedisPubSub {

        private final JedisPool jedisPool;

        public NotifySub(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if (msg.equals(Constants.REGISTER)
                    || msg.equals(Constants.UNREGISTER)) {
                try {
                    Jedis jedis = jedisPool.getResource();
                    try {
                        doNotify(jedis, key);
                    } finally {
                        jedis.close();
                    }
                } catch (Throwable t) { // TODO Notification failure does not restore mechanism guarantee
                    logger.error(t.getMessage(), t);
                }
            }
        }

        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }

        @Override
        public void onSubscribe(String key, int num) {
        }

        @Override
        public void onPSubscribe(String pattern, int num) {
        }

        @Override
        public void onUnsubscribe(String key, int num) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int num) {
        }

    }

    private class Notifier extends Thread {

        private final String service;
        private final AtomicInteger connectSkip = new AtomicInteger();
        private final AtomicInteger connectSkiped = new AtomicInteger();
        private final Random random = new Random();
        private volatile Jedis jedis;
        private volatile boolean first = true;
        private volatile boolean running = true;
        private volatile int connectRandom;

        public Notifier(String service) {
            super.setDaemon(true);
            super.setName("DubboRedisSubscribe");
            this.service = service;
        }

        private void resetSkip() {
            connectSkip.set(0);
            connectSkiped.set(0);
            connectRandom = 0;
        }

        private boolean isSkip() {
            int skip = connectSkip.get(); // Growth of skipping times
            if (skip >= 10) { // If the number of skipping times increases by more than 10, take the random number
                if (connectRandom == 0) {
                    connectRandom = random.nextInt(10);
                }
                skip = 10 + connectRandom;
            }
            if (connectSkiped.getAndIncrement() < skip) { // Check the number of skipping times
                return true;
            }
            connectSkip.incrementAndGet();
            connectSkiped.set(0);
            connectRandom = 0;
            return false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (!isSkip()) {
                        try {
                            for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
                                JedisPool jedisPool = entry.getValue();
                                try {
                                    jedis = jedisPool.getResource();
                                    try {
                                        if (service.endsWith(Constants.ANY_VALUE)) {
                                            if (!first) {
                                                first = false;
                                                Set<String> keys = jedis.keys(service);
                                                if (keys != null && !keys.isEmpty()) {
                                                    for (String s : keys) {
                                                        doNotify(jedis, s);
                                                    }
                                                }
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service); // blocking
                                        } else {
                                            if (!first) {
                                                first = false;
                                                doNotify(jedis, service);
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE); // blocking
                                        }
                                        break;
                                    } finally {
                                        jedis.close();
                                    }
                                } catch (Throwable t) { // Retry another server
                                    logger.warn("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
                                    // If you only have a single redis, you need to take a rest to avoid overtaking a lot of CPU resources
                                    sleep(reconnectPeriod);
                                }
                            }
                        } catch (Throwable t) {
                            logger.error(t.getMessage(), t);
                            sleep(reconnectPeriod);
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t.getMessage(), t);
                }
            }
        }

        public void shutdown() {
            try {
                running = false;
                jedis.disconnect();
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }

    }

}
