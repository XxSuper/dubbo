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
import com.alibaba.dubbo.common.utils.ExecutorUtil;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Server;
import com.alibaba.dubbo.remoting.transport.AbstractServer;
import com.alibaba.dubbo.remoting.transport.dispatcher.ChannelHandlers;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * NettyServer
 * 实现 Server 接口，继承 AbstractServer 抽象类，Netty 服务器实现类。
 */
public class NettyServer extends AbstractServer implements Server {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * 通道集合.连接到服务器的客户端通道集合。
     */
    private Map<String, Channel> channels; // <ip:port, channel>

    /**
     * ServerBootstrap
     */
    private ServerBootstrap bootstrap;

    /**
     * channel
     */
    private io.netty.channel.Channel channel;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyServer(URL url, ChannelHandler handler) throws RemotingException {
        // ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME) 设置线程名到 URL 上
        // ChannelHandlers.wrap(handler, ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME) 代码段，包装 ChannelHandler ，实现 Dubbo 线程模型的功能。
        super(url, ChannelHandlers.wrap(handler, ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME)));
    }

    /**
     * 启动服务器
     * @throws Throwable
     */
    @Override
    protected void doOpen() throws Throwable {
        // 实例化 ServerBootstrap
        bootstrap = new ServerBootstrap();

        // 创建线程组
        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("NettyServerBoss", true));
        workerGroup = new NioEventLoopGroup(getUrl().getPositiveParameter(Constants.IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS),
                new DefaultThreadFactory("NettyServerWorker", true));

        // 创建 NettyServerHandler 对象
        final NettyServerHandler nettyServerHandler = new NettyServerHandler(getUrl(), this);
        // 设置 `channels` 属性，指向 NettyServerHandler.channels 属性。
        channels = nettyServerHandler.getChannels();

        // 设置线程组
        bootstrap.group(bossGroup, workerGroup)
                // 设置 Channel类型
                .channel(NioServerSocketChannel.class)
                // 设置可选项
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                // PooledByteBufAllocator.DEFAULT ，对象池，重用缓冲区。
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                // 设置责任链路
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        // 创建 NettyCodecAdapter 对象
                        NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyServer.this);
                        ch.pipeline()//.addLast("logging",new LoggingHandler(LogLevel.INFO))//for debug
                                // 解码
                                .addLast("decoder", adapter.getDecoder())
                                // 编码
                                .addLast("encoder", adapter.getEncoder())
                                // 处理器
                                .addLast("handler", nettyServerHandler);
                    }
                });
        // 服务器绑定端口监听
        // bind
        ChannelFuture channelFuture = bootstrap.bind(getBindAddress());
        channelFuture.syncUninterruptibly();
        channel = channelFuture.channel();

    }

    /**
     * 关闭服务器
     * @throws Throwable
     */
    @Override
    protected void doClose() throws Throwable {
        // 关闭服务器通道
        try {
            if (channel != null) {
                // unbind.
                channel.close();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 关闭连接到服务器的客户端通道
            Collection<com.alibaba.dubbo.remoting.Channel> channels = getChannels();
            if (channels != null && channels.size() > 0) {
                for (com.alibaba.dubbo.remoting.Channel channel : channels) {
                    try {
                        channel.close();
                    } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 优雅关闭工作组
            if (bootstrap != null) {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        // 清空连接到服务器的客户端通道
        try {
            if (channels != null) {
                channels.clear();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 获得所有通道
     * @return
     */
    @Override
    public Collection<Channel> getChannels() {
        Collection<Channel> chs = new HashSet<Channel>();
        for (Channel channel : this.channels.values()) {
            // 已连接，添加
            if (channel.isConnected()) {
                chs.add(channel);
            } else {
                // 未连接，移除
                channels.remove(NetUtils.toAddressString(channel.getRemoteAddress()));
            }
        }
        return chs;
    }

    /**
     * 获得指定地址的 Channel 对象。
     * @param remoteAddress
     * @return
     */
    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return channels.get(NetUtils.toAddressString(remoteAddress));
    }

    @Override
    public boolean isBound() {
        return channel.isActive();
    }

}
