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
package com.alibaba.dubbo.rpc.cluster.directory;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.RouterFactory;
import com.alibaba.dubbo.rpc.cluster.router.MockInvokersSelector;
import com.alibaba.dubbo.rpc.cluster.router.tag.TagRouter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract implementation of Directory: Invoker list returned from this Directory's list method have been filtered by Routers
 * 实现 Directory 接口，Directory 抽象实现类，实现了公用的路由规则( Router )的逻辑。
 */
public abstract class AbstractDirectory<T> implements Directory<T> {

    // logger
    private static final Logger logger = LoggerFactory.getLogger(AbstractDirectory.class);

    /**
     * 注册中心 URL
     */
    private final URL url;

    /**
     * 是否已经销毁
     */
    private volatile boolean destroyed = false;

    /**
     * 消费者 URL
     *
     * 若未显式调用 {@link #AbstractDirectory(URL, URL, List)} 构造方法，consumerUrl 等于 {@link #url}
     */
    private volatile URL consumerUrl;

    /**
     * Router 数组
     */
    private volatile List<Router> routers;

    public AbstractDirectory(URL url) {
        this(url, null);
    }

    public AbstractDirectory(URL url, List<Router> routers) {
        this(url, url, routers);
    }

    public AbstractDirectory(URL url, URL consumerUrl, List<Router> routers) {
        if (url == null)
            throw new IllegalArgumentException("url == null");
        this.url = url;
        this.consumerUrl = consumerUrl;
        // 初始化并设置 Router 数组
        setRouters(routers);
    }

    /**
     * 获得所有服务 Invoker 集合。
     * @param invocation
     * @return
     * @throws RpcException
     */
    @Override
    public List<Invoker<T>> list(Invocation invocation) throws RpcException {
        // 如果已销毁，抛出异常
        if (destroyed) {
            throw new RpcException("Directory already destroyed .url: " + getUrl());
        }
        // 获得所有 Invoker 集合
        List<Invoker<T>> invokers = doList(invocation);
        // 根据路由规则，筛选 Invoker 集合，本地引用，避免并发问题
        List<Router> localRouters = this.routers; // local reference
        if (localRouters != null && !localRouters.isEmpty()) {
            for (Router router : localRouters) {
                try {
                    if (router.getUrl() == null || router.getUrl().getParameter(Constants.RUNTIME_KEY, false)) {
                        // 根据路由规则( Router )，进一步筛选合适的 Invoker 集合。
                        invokers = router.route(invokers, getConsumerUrl(), invocation);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
                }
            }
        }
        return invokers;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public List<Router> getRouters() {
        return routers;
    }

    /**
     * 初始化并设置 Router 数组（设置路由规则）。
     * @param routers
     */
    protected void setRouters(List<Router> routers) {
        // copy list 复制重新创建 routers 数组，因为下面会进行修改。
        routers = routers == null ? new ArrayList<Router>() : new ArrayList<Router>(routers);
        // append url router  拼接 `url` 中，配置的路由规则
        String routerkey = url.getParameter(Constants.ROUTER_KEY);
        if (routerkey != null && routerkey.length() > 0) {
            // 添加 url 中配置的路由规则到 routers 中
            RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getExtension(routerkey);
            routers.add(routerFactory.getRouter(url));
        }
        // append mock invoker selector 添加 MockInvokersSelector 到 routers 中
        routers.add(new MockInvokersSelector());
        routers.add(new TagRouter());
        // 排序
        Collections.sort(routers);
        // 赋值属性给 AbstractDirectory
        this.routers = routers;
    }

    public URL getConsumerUrl() {
        return consumerUrl;
    }

    public void setConsumerUrl(URL consumerUrl) {
        this.consumerUrl = consumerUrl;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public void destroy() {
        destroyed = true;
    }

    protected abstract List<Invoker<T>> doList(Invocation invocation) throws RpcException;

}
