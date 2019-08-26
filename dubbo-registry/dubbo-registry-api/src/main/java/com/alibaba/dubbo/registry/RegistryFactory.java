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
package com.alibaba.dubbo.registry;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;

/**
 * RegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * RegistryFactory 是一个 Dubbo SPI 拓展接口。注册中心工厂接口，单例，线程安全
 *
 * @see com.alibaba.dubbo.registry.support.AbstractRegistryFactory
 */
@SPI("dubbo")
public interface RegistryFactory {

    /**
     * @Adaptive({"protocol"}) 注解，Dubbo SPI 会自动实现 RegistryFactory$Adaptive 类，根据 url.protocol 获得对应的 RegistryFactory 实现类。例如，url.protocol = zookeeper 时，获得 ZookeeperRegistryFactory 实现类。
     *
     * 获得注册中心 Registry 对象。
     * Connect to the registry 连接注册中心.
     * <p>
     * Connecting the registry needs to support the contract: 连接注册中心需处理契约：<br>
     * 1. When the check=false is set, the connection is not checked, otherwise the exception is thrown when disconnection 1. 当设置check=false时表示不检查连接，否则在连接不上时抛出异常。<br>
     * 2. Support username:password authority authentication on URL. 2. 支持URL上的username:password权限认证。<br>
     * 3. Support the backup=10.20.153.10 candidate registry cluster address. 3.支持backup=10.20.153.10备选注册中心集群地址。<br>
     * 4. Support file=registry.cache local disk file cache. 4. 支持file=registry.cache本地磁盘文件缓存。<br>
     * 5. Support the timeout=1000 request timeout setting. 5.支持timeout=1000请求超时设置。<br>
     * 6. Support session=60000 session timeout or expiration settings. 6. 支持session=60000会话超时或过期设置。<br>
     *
     * @param url Registry address, is not allowed to be empty 注册中心地址，不允许为空
     * @return Registry reference, never return empty value 注册中心引用，总不返回空
     */
    @Adaptive({"protocol"})
    Registry getRegistry(URL url);

}