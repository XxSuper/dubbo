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
package com.alibaba.dubbo.common.utils;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtil {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorUtil.class);
    private static final ThreadPoolExecutor shutdownExecutor = new ThreadPoolExecutor(0, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(100),
            new NamedThreadFactory("Close-ExecutorService-Timer", true));

    public static boolean isTerminated(Executor executor) {
        if (executor instanceof ExecutorService) {
            if (((ExecutorService) executor).isTerminated()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Use the shutdown pattern from:
     *  https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html
     * @param executor the Executor to shutdown
     * @param timeout the timeout in milliseconds before termination
     */
    public static void gracefulShutdown(Executor executor, int timeout) {
        if (!(executor instanceof ExecutorService) || isTerminated(executor)) {
            return;
        }
        final ExecutorService es = (ExecutorService) executor;
        try {
            // 禁止提交新任务
            // 当线程池调用该方法时,线程池的状态则立刻变成SHUTDOWN状态。
            // 此时，则不能再往线程池中添加任何任务，否则将会抛出RejectedExecutionException异常。
            // 但是，此时线程池不会立刻退出，直到添加到线程池中的任务都已经处理完成，才会退出。
            // Disable new tasks from being submitted
            es.shutdown();
        } catch (SecurityException ex2) {
            return;
        } catch (NullPointerException ex2) {
            return;
        }
        try {
            // 等待现有任务终止
            // Wait a while for existing tasks to terminate
            if (!es.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
                // 执行该方法，线程池的状态立刻变成STOP状态，并试图停止所有正在执行的线程，不再处理还在池队列中等待的任务，当然，它会返回那些未执行的任务。
                es.shutdownNow();
            }
        } catch (InterruptedException ex) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // 如果还没有停止，开启线程池执行关闭
        if (!isTerminated(es)) {
            newThreadToCloseExecutor(es);
        }
    }

    public static void shutdownNow(Executor executor, final int timeout) {
        if (!(executor instanceof ExecutorService) || isTerminated(executor)) {
            return;
        }
        final ExecutorService es = (ExecutorService) executor;
        try {
            es.shutdownNow();
        } catch (SecurityException ex2) {
            return;
        } catch (NullPointerException ex2) {
            return;
        }
        try {
            es.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        if (!isTerminated(es)) {
            newThreadToCloseExecutor(es);
        }
    }

    private static void newThreadToCloseExecutor(final ExecutorService es) {
        if (!isTerminated(es)) {
            shutdownExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 1000; i++) {
                            es.shutdownNow();
                            if (es.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                                break;
                            }
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            });
        }
    }

    /**
     * append thread name with url address
     *
     * @return new url with updated thread name
     */
    public static URL setThreadName(URL url, String defaultName) {
        String name = url.getParameter(Constants.THREAD_NAME_KEY, defaultName);
        name = name + "-" + url.getAddress();
        url = url.addParameter(Constants.THREAD_NAME_KEY, name);
        return url;
    }
}
