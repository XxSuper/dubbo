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
package com.alibaba.dubbo.remoting.zookeeper.support;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.zookeeper.ChildListener;
import com.alibaba.dubbo.remoting.zookeeper.StateListener;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 实现 ZookeeperClient 接口，Zookeeper 客户端抽象类，实现通用的逻辑。
 * @param <TargetChildListener>
 */
public abstract class AbstractZookeeperClient<TargetChildListener> implements ZookeeperClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractZookeeperClient.class);

    /**
     * 注册中心 URL
     */
    private final URL url;

    /**
     * StateListener 集合
     */
    private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<StateListener>();

    /**
     * ChildListener 集合
     *
     * key1：节点路径
     * key2：ChildListener 对象
     * value ：监听器具体对象。不同 Zookeeper 客户端，实现会不同。
     */
    private final ConcurrentMap<String, ConcurrentMap<ChildListener, TargetChildListener>> childListeners = new ConcurrentHashMap<String, ConcurrentMap<ChildListener, TargetChildListener>>();

    /**
     * 是否关闭
     */
    private volatile boolean closed = false;

    public AbstractZookeeperClient(URL url) {
        this.url = url;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public void create(String path, boolean ephemeral) {
        if (!ephemeral) {
            // 抽象方法，节点是否存在
            if (checkExists(path)) {
                return;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            // 循环创建父路径
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            // 抽象方法，创建临时节点
            createEphemeral(path);
        } else {
            // 抽象方法，创建持久节点
            createPersistent(path);
        }
    }

    /**
     * 添加 StateListener
     * @param listener 监听器
     */
    @Override
    public void addStateListener(StateListener listener) {
        stateListeners.add(listener);
    }

    /**
     * 移除 StateListener
     * @param listener 监听器
     */
    @Override
    public void removeStateListener(StateListener listener) {
        stateListeners.remove(listener);
    }

    /**
     * 获取 StateListener 集合
     */
    public Set<StateListener> getSessionListeners() {
        return stateListeners;
    }

    @Override
    public List<String> addChildListener(String path, final ChildListener listener) {
        // 获得路径下的监听器数组
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners == null) {
            // 监听器数组不存在创建，继续获取
            childListeners.putIfAbsent(path, new ConcurrentHashMap<ChildListener, TargetChildListener>());
            listeners = childListeners.get(path);
        }
        // 获得是否已经有该监听器
        TargetChildListener targetListener = listeners.get(listener);
        // 监听器不存在，进行创建，再次获取
        if (targetListener == null) {
            // createTargetChildListener(path, listener) 抽象方法，创建真正的 ChildListener 对象。因为，每个 Zookeeper 的库，实现不同。
            listeners.putIfAbsent(listener, createTargetChildListener(path, listener));
            targetListener = listeners.get(listener);
        }
        // addTargetChildListener(path, targetListener) 抽象方法，向 Zookeeper ，真正发起订阅
        return addTargetChildListener(path, targetListener);
    }

    @Override
    public void removeChildListener(String path, ChildListener listener) {
        // 获得路径下的监听器数组
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners != null) {
            // 从集合中移除监听器
            TargetChildListener targetListener = listeners.remove(listener);
            if (targetListener != null) {
                // removeTargetChildListener(path, targetListener) 抽象方法，向 Zookeeper ，真正发起取消订阅
                removeTargetChildListener(path, targetListener);
            }
        }
    }

    /**
     * StateListener 数组，回调
     *
     * @param state 状态
     */
    protected void stateChanged(int state) {
        for (StateListener sessionListener : getSessionListeners()) {
            sessionListener.stateChanged(state);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            // 抽象方法，关闭 Zookeeper 连接
            doClose();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    protected abstract void doClose();

    protected abstract void createPersistent(String path);

    protected abstract void createEphemeral(String path);

    protected abstract boolean checkExists(String path);

    protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);

    protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);

    protected abstract void removeTargetChildListener(String path, TargetChildListener listener);

}
