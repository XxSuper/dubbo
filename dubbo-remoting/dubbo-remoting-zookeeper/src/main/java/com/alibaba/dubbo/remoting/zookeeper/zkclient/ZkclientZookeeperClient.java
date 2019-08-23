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
package com.alibaba.dubbo.remoting.zookeeper.zkclient;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.zookeeper.ChildListener;
import com.alibaba.dubbo.remoting.zookeeper.StateListener;
import com.alibaba.dubbo.remoting.zookeeper.support.AbstractZookeeperClient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.List;

/**
 * 实现 AbstractZookeeperClient 抽象类，基于 Zkclient 的 Zookeeper 客户端实现类。
 */
public class ZkclientZookeeperClient extends AbstractZookeeperClient<IZkChildListener> {

    private final ZkClientWrapper client;

    private volatile KeeperState state = KeeperState.SyncConnected;

    public ZkclientZookeeperClient(URL url) {
        super(url);
        // 创建 client 对象
        client = new ZkClientWrapper(url.getBackupAddress(), 30000);
        // 添加连接监听器
        client.addListener(new IZkStateListener() {
            @Override
            public void handleStateChanged(KeeperState state) throws Exception {
                ZkclientZookeeperClient.this.state = state;
                if (state == KeeperState.Disconnected) {
                    stateChanged(StateListener.DISCONNECTED);
                } else if (state == KeeperState.SyncConnected) {
                    stateChanged(StateListener.CONNECTED);
                }
            }

            @Override
            public void handleNewSession() throws Exception {
                stateChanged(StateListener.RECONNECTED);
            }
        });
        // 启动 client
        client.start();
    }


    @Override
    public void createPersistent(String path) {
        try {
            // 创建一个节点
            client.createPersistent(path);
        } catch (ZkNodeExistsException e) {
        }
    }

    @Override
    public void createEphemeral(String path) {
        try {
            // 创建一个临时节点
            client.createEphemeral(path);
        } catch (ZkNodeExistsException e) {
        }
    }

    @Override
    public void delete(String path) {
        try {
            // 删除一个节点
            client.delete(path);
        } catch (ZkNoNodeException e) {
        }
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            // 获取某个节点的所有子节点路径
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    @Override
    public boolean checkExists(String path) {
        try {
            // 检查节点是否存在
            return client.exists(path);
        } catch (Throwable t) {
        }
        return false;
    }

    /**
     * 客户端是否连接
     * @return
     */
    @Override
    public boolean isConnected() {
        return state == KeeperState.SyncConnected;
    }

    @Override
    public void doClose() {
        // 关闭客户端连接
        client.close();
    }

    @Override
    public IZkChildListener createTargetChildListener(String path, final ChildListener listener) {
        // 创建监听
        return new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds)
                    throws Exception {
                listener.childChanged(parentPath, currentChilds);
            }
        };
    }

    @Override
    public List<String> addTargetChildListener(String path, final IZkChildListener listener) {
        // 添加监听
        return client.subscribeChildChanges(path, listener);
    }

    @Override
    public void removeTargetChildListener(String path, IZkChildListener listener) {
        // 移除监听
        client.unsubscribeChildChanges(path, listener);
    }

}
