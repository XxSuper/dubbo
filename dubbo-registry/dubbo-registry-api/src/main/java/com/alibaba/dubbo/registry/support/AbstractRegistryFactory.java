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
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractRegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * 实现 RegistryFactory 接口，RegistryFactory 抽象类，实现了 Registry 的容器管理。单例，线程安全。
 *
 * @see com.alibaba.dubbo.registry.RegistryFactory
 */
public abstract class AbstractRegistryFactory implements RegistryFactory {

    // Log output
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRegistryFactory.class);

    // 静态属性，锁，用于 #destroyAll() 和 #getRegistry(url) 方法，对 REGISTRIES 访问的竞争。
    // The lock for the acquisition process of the registry
    private static final ReentrantLock LOCK = new ReentrantLock();

    // 静态属性，Registry 集合。
    // Registry Collection Map<RegistryAddress, Registry>
    private static final Map<String, Registry> REGISTRIES = new ConcurrentHashMap<String, Registry>();

    /**
     * Get all registries
     *
     * @return all registries
     */
    public static Collection<Registry> getRegistries() {
        return Collections.unmodifiableCollection(REGISTRIES.values());
    }

    /**
     * Close all created registries
     * 销毁所有 Registry 对象。
     */
    // TODO: 2017/8/30 to move somewhere else better
    public static void destroyAll() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Close all registries " + getRegistries());
        }
        // Lock up the registry shutdown process
        // 获得锁
        LOCK.lock();
        try {
            // 销毁
            for (Registry registry : getRegistries()) {
                try {
                    registry.destroy();
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            // 清空缓存
            REGISTRIES.clear();
        } finally {
            // 释放锁
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * 获得注册中心 Registry 对象。优先从缓存中获取，否则进行创建。
     * @param url Registry address, is not allowed to be empty 注册中心地址，不允许为空
     * @return Registry 对象
     */
    @Override
    public Registry getRegistry(URL url) {
        // 修改 URL
        url = url.setPath(RegistryService.class.getName())
                .addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
                .removeParameters(Constants.EXPORT_KEY, Constants.REFER_KEY);
        // 计算 key
        String key = url.toServiceString();
        // Lock the registry access process to ensure a single instance of the registry
        // 获得锁
        LOCK.lock();
        try {
            // 从缓存中获得 Registry 对象
            Registry registry = REGISTRIES.get(key);
            if (registry != null) {
                return registry;
            }
            // 缓存不存在，进行创建 Registry 对象
            registry = createRegistry(url);
            if (registry == null) {
                throw new IllegalStateException("Can not create registry " + url);
            }
            // 添加到缓存
            REGISTRIES.put(key, registry);
            return registry;
        } finally {
            // 释放锁
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * 抽象方法，创建 Registry 对象。子类实现该方法，创建其对应的 Registry 实现类。例如，ZookeeperRegistryFactory 的该方法，创建 ZookeeperRegistry 对象。
     * @param url 注册中心地址
     * @return Registry 对象
     */
    protected abstract Registry createRegistry(URL url);

}
