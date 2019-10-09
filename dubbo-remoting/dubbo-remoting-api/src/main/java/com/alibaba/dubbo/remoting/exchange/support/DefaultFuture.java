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
package com.alibaba.dubbo.remoting.exchange.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DefaultFuture.
 * 实现 ResponseFuture 接口，默认响应 Future 实现类。同时，它也是所有 DefaultFuture 的管理容器。
 */
public class DefaultFuture implements ResponseFuture {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFuture.class);

    /**
     * 通道集合
     *
     * key：请求编号
     */
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<Long, Channel>();

    /**
     * Future 集合
     *
     * key：请求编号
     */
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<Long, DefaultFuture>();

    static {
        // 后台扫描调用超时任务
        Thread th = new Thread(new RemotingInvocationTimeoutScan(), "DubboResponseTimeoutScanTimer");
        th.setDaemon(true);
        th.start();
    }

    /**
     * 请求编号
     */
    // invoke id.
    private final long id;

    /**
     * 通道
     */
    private final Channel channel;

    /**
     * 请求
     */
    private final Request request;

    /**
     * 超时
     */
    private final int timeout;

    /**
     * 锁
     */
    private final Lock lock = new ReentrantLock();

    /**
     * 完成 Condition
     */
    private final Condition done = lock.newCondition();

    /**
     * 创建开始时间
     */
    private final long start = System.currentTimeMillis();

    /**
     * 发送请求时间
     */
    private volatile long sent;

    /**
     * 响应
     */
    private volatile Response response;

    /**
     * 回调，适用于异步请求。通过 #setCallback(callback) 方法设置。
     */
    private volatile ResponseCallback callback;

    public DefaultFuture(Channel channel, Request request, int timeout) {
        this.channel = channel;
        this.request = request;
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        // put into waiting map.
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    /**
     * 判断通道是否有未结束的请求。
     * @param channel
     * @return
     */
    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    /**
     * close a channel when a channel is inactive
     * directly return the unfinished requests.
     *
     * @param channel channel to close
     */
    public static void closeChannel(Channel channel) {
        for (long id : CHANNELS.keySet()) {
            if (channel.equals(CHANNELS.get(id))) {
                DefaultFuture future = getFuture(id);
                if (future != null && !future.isDone()) {
                    Response disconnectResponse = new Response(future.getId());
                    disconnectResponse.setStatus(Response.CHANNEL_INACTIVE);
                    disconnectResponse.setErrorMessage("Channel " +
                            channel +
                            " is inactive. Directly return the unFinished request : " +
                            future.getRequest());
                    DefaultFuture.received(channel, disconnectResponse);
                }
            }
        }
    }

    /**
     * 该方法有两处被调用
     * 1、后台扫描调用超时任务 DefaultFuture.RemotingInvocationTimeoutScan.run()
     * 2、Handler：处理响应结果 HeaderExchangeHandler.handleResponse(Channel, Response)
     * @param channel
     * @param response
     */
    public static void received(Channel channel, Response response) {
        try {
            // 移除 FUTURES
            DefaultFuture future = FUTURES.remove(response.getId());
            if (future != null) {
                // 接收结果
                future.doReceived(response);
            } else {
                // 超时情况，打印告警日志。
                logger.warn("The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response " + response
                        + (channel == null ? "" : ", channel: " + channel.getLocalAddress()
                        + " -> " + channel.getRemoteAddress()));
            }
        } finally {
            // 移除 CHANNELS
            CHANNELS.remove(response.getId());
        }
    }

    @Override
    public Object get() throws RemotingException {
        return get(timeout);
    }

    /**
     * 调用 #isDone() 方法，判断是否完成。若未完成，基于 Lock + Condition 的方式，实现等待。
     * 而等待的唤醒，通过 ChannelHandler#received(channel, message) 方法，接收到请求时执行 DefaultFuture#received(channel, response) 方法。
     * @param timeout
     * @return
     * @throws RemotingException
     */
    @Override
    public Object get(int timeout) throws RemotingException {
        if (timeout <= 0) {
            // 设置默认超时时间
            timeout = Constants.DEFAULT_TIMEOUT;
        }
        // 若未完成，等待
        if (!isDone()) {
            // 获得开始时间。注意，此处使用的不是 start 属性。后面我们会看到，#get(...) 方法中，使用的是重新获取开始时间；
            // 后台扫描调用超时任务，使用的是 start 属性。也就是说，#get(timeout) 方法的 timeout 参数，指的是从当前时刻开始的等待超时时间。
            // 当然，这不影响最终的结果，最终 Response 是什么，由是 ChannelHandler#received(channel, message) 还是后台扫描调用超时任务，
            // 谁先调用 DefaultFuture#received(channel, response) 方法决定。
            long start = System.currentTimeMillis();
            // 上锁
            lock.lock();
            try {
                // 等待完成或超时
                while (!isDone()) {
                    done.await(timeout, TimeUnit.MILLISECONDS);
                    if (isDone() || System.currentTimeMillis() - start > timeout) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                // 解锁
                lock.unlock();
            }
            // 未完成，抛出超时异常 TimeoutException
            if (!isDone()) {
                throw new TimeoutException(sent > 0, channel, getTimeoutMessage(false));
            }
        }
        // 返回响应
        return returnFromResponse();
    }

    public void cancel() {
        Response errorResult = new Response(id);
        errorResult.setErrorMessage("request future has been canceled.");
        response = errorResult;
        FUTURES.remove(id);
        CHANNELS.remove(id);
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    @Override
    public void setCallback(ResponseCallback callback) {
        // 若已完成，调用 #invokeCallback(callback) 方法，执行回调方法。
        if (isDone()) {
            invokeCallback(callback);
        } else {
            boolean isdone = false;
            // 获得锁
            lock.lock();
            try {
                // 未完成，设置回调 callback 属性，等在 #doReceived(response) 方法中再回调。
                if (!isDone()) {
                    this.callback = callback;
                } else {
                    // 标记已完成。
                    isdone = true;
                }
            } finally {
                // 释放锁
                lock.unlock();
            }
            // 已完成，调用 #invokeCallback(callback) 方法，执行回调方法。
            if (isdone) {
                invokeCallback(callback);
            }
        }
    }

    private void invokeCallback(ResponseCallback c) {
        ResponseCallback callbackCopy = c;
        if (callbackCopy == null) {
            throw new NullPointerException("callback cannot be null.");
        }
        c = null;
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null. url:" + channel.getUrl());
        }

        // 正常，处理结果
        if (res.getStatus() == Response.OK) {
            try {
                callbackCopy.done(res.getResult());
            } catch (Exception e) {
                logger.error("callback invoke error .reasult:" + res.getResult() + ",url:" + channel.getUrl(), e);
            }
        // 超时，处理 TimeoutException 异常
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            try {
                TimeoutException te = new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
                callbackCopy.caught(te);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        // 其他，处理 RemotingException 异常
        } else {
            try {
                RuntimeException re = new RuntimeException(res.getErrorMessage());
                callbackCopy.caught(re);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        }
    }

    private Object returnFromResponse() throws RemotingException {
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }
        // 正常，返回结果
        if (res.getStatus() == Response.OK) {
            return res.getResult();
        }
        // 超时，抛出 TimeoutException 异常
        if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            throw new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
        }
        // 其他，抛出 RemotingException 异常
        throw new RemotingException(channel, res.getErrorMessage());
    }

    private long getId() {
        return id;
    }

    private Channel getChannel() {
        return channel;
    }

    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private long getStartTimestamp() {
        return start;
    }

    private void doSent() {
        sent = System.currentTimeMillis();
    }

    private void doReceived(Response res) {
        // 锁定
        lock.lock();
        try {
            // 设置结果
            response = res;
            // 通知，唤醒等待
            if (done != null) {
                done.signal();
            }
        } finally {
            // 释放锁定
            lock.unlock();
        }
        // 执行回调方法。
        if (callback != null) {
            invokeCallback(callback);
        }
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                + (scan ? " by scan timer" : "") + ". start time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + ","
                + (sent > 0 ? " client elapsed: " + (sent - start)
                + " ms, server elapsed: " + (nowTimestamp - sent)
                : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                + timeout + " ms, request: " + request + ", channel: " + channel.getLocalAddress()
                + " -> " + channel.getRemoteAddress();
    }

    /**
     * 后台扫描调用超时任务
     */
    private static class RemotingInvocationTimeoutScan implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    for (DefaultFuture future : FUTURES.values()) {
                        // 已完成，跳过
                        if (future == null || future.isDone()) {
                            continue;
                        }
                        // 超时
                        if (System.currentTimeMillis() - future.getStartTimestamp() > future.getTimeout()) {
                            // 创建超时 Response
                            // create exception response.
                            Response timeoutResponse = new Response(future.getId());
                            // 设置超时状态
                            // set timeout status.
                            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
                            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
                            // 响应结果
                            // handle response.
                            DefaultFuture.received(future.getChannel(), timeoutResponse);
                        }
                    }
                    // 30 ms
                    Thread.sleep(30);
                } catch (Throwable e) {
                    logger.error("Exception when scan the timeout invocation of remoting.", e);
                }
            }
        }
    }

}
