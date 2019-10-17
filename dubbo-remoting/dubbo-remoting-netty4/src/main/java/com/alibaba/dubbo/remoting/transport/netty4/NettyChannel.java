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
package com.alibaba.dubbo.remoting.transport.netty4;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractChannel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * NettyChannel.
 * 实现 AbstractChannel 抽象类，封装 Netty Channel 的通道实现类。
 */
final class NettyChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class);

    /**
     * 静态属性，通道集合。在实际 Netty Handler 里，每个方法参数里，传递的是 io.netty.channel.Channel 对象。通过 NettyChannel.channelMap 中，获得对应的 NettyChannel 对象。
     */
    private static final ConcurrentMap<Channel, NettyChannel> channelMap = new ConcurrentHashMap<Channel, NettyChannel>();

    /**
     * 通道。NettyChannel 是传入 channel 属性的装饰器，每个实现的方法，都会调用 channel。
     */
    private final Channel channel;

    /**
     * 属性集合。注意，setAttribute(...) 等方法，使用的是该属性，而不是 io.netty.channel.Channel 的。
     */
    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    private NettyChannel(Channel channel, URL url, ChannelHandler handler) {
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
    }

    /**
     * 创建 NettyChannel 对象
     * @param ch
     * @param url
     * @param handler
     * @return
     */
    static NettyChannel getOrAddChannel(Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }
        NettyChannel ret = channelMap.get(ch);
        if (ret == null) {
            NettyChannel nettyChannel = new NettyChannel(ch, url, handler);
            // 连接中
            if (ch.isActive()) {
                // 添加到 channelMap
                ret = channelMap.putIfAbsent(ch, nettyChannel);
            }
            if (ret == null) {
                ret = nettyChannel;
            }
        }
        return ret;
    }

    /**
     * 如果 channel 断开连接，从 channelMap 中移除
     * @param ch
     */
    static void removeChannelIfDisconnected(Channel ch) {
        // 未连接
        if (ch != null && !ch.isActive()) {
            // 移除出channelMap
            channelMap.remove(ch);
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public boolean isConnected() {
        return !isClosed() && channel.isActive();
    }

    /**
     * 发送消息
     * @param message
     * @param sent    already sent to socket?
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        // 检查连接状态
        super.send(message, sent);

        // success ，是否执行成功。如果没有等待发送成功，默认成功。
        boolean success = true;
        int timeout = 0;
        try {
            // 调用真正的 io.netty.channel.Channel#writeAndFlush(message) 方法，发送消息。
            ChannelFuture future = channel.writeAndFlush(message);
            // 若需要等待发送成功( sent = true )，等待直到成功或超时。
            if (sent) {
                timeout = getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
                success = future.await(timeout);
            }
            // 若发生异常，抛出异常
            Throwable cause = future.cause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + ", cause: " + e.getMessage(), e);
        }

        // 发送失败，抛出异常
        if (!success) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress()
                    + "in timeout(" + timeout + "ms) limit");
        }
    }

    /**
     * 关闭通道，最后一步才关闭真正的通道，避免中间状态。
     */
    @Override
    public void close() {
        try {
            // 标记关闭
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 移除连接
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 清空属性 attributes
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            // 关闭真正的通道 channel
            channel.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        if (value == null) { // The null value unallowed in the ConcurrentHashMap.
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NettyChannel other = (NettyChannel) obj;
        if (channel == null) {
            if (other.channel != null) return false;
        } else if (!channel.equals(other.channel)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

}
