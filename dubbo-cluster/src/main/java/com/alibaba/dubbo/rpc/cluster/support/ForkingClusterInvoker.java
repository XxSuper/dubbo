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
package com.alibaba.dubbo.rpc.cluster.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.threadlocal.NamedInternalThreadFactory;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Invoke a specific number of invokers concurrently, usually used for demanding real-time operations, but need to waste more service resources.
 *
 * <a href="http://en.wikipedia.org/wiki/Fork_(topology)">Fork</a>
 * 实现 AbstractClusterInvoker 抽象类，ForkingCluster Invoker 实现类。
 *
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link com.alibaba.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link com.alibaba.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     * ExecutorService 对象，并且为 CachedThreadPool 。
     * CachedThreadPool：创建一个可缓存线程池。
     * 可根据需要创建新线程的线程池，如果现有线程没有可用的，则创建一个新线程并添加到池中，如果有被使用完但是还没销毁的线程，就复用该线程。
     * 终止并从缓存中移除那些已有 60 秒钟未被使用的线程。因此，长时间保持空闲的线程池不会使用任何资源。
     */
    private final ExecutorService executor = Executors.newCachedThreadPool(
            new NamedInternalThreadFactory("forking-cluster-timer", true));

    public ForkingClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    /**
     * ForkingClusterInvoker 实现了并行调用，且只要一个成功即返回。当然，还有一个隐性的，所有都失败才返回。
     * @param invocation
     * @param invokers
     * @param loadbalance
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {
            // 检查 invokers 即可用 Invoker 集合是否为空，如果为空，那么抛出异常
            checkInvokers(invokers, invocation);
            // 保存选择的 Invoker 集合
            final List<Invoker<T>> selected;
            // 得到最大并行数，默认为 Constants.DEFAULT_FORKS = 2
            final int forks = getUrl().getParameter(Constants.FORKS_KEY, Constants.DEFAULT_FORKS);
            // 获得调用超时时间，默认为 DEFAULT_TIMEOUT = 1000 毫秒
            final int timeout = getUrl().getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
            // 若最大并行书小于等于 0，或者大于 invokers 的数量，直接使用 invokers
            if (forks <= 0 || forks >= invokers.size()) {
                selected = invokers;
            } else {
                // 循环，根据负载均衡机制从 invokers，中选择一个个Invoker ，从而组成 Invoker 集合。
                // 注意，因为增加了排重逻辑，所以不能保证获得的 Invoker 集合的大小，小于最大并行数
                selected = new ArrayList<Invoker<T>>();
                for (int i = 0; i < forks; i++) {
                    // 在 invoker 列表(排除 selected)后,如果没有选够,则存在重复循环问题.见 select 实现.
                    // TODO. Add some comment here, refer chinese version for more details.
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);
                    // 防止重复添加
                    if (!selected.contains(invoker)) {//Avoid add the same invoker several times.
                        selected.add(invoker);
                    }
                }
            }
            // 设置已经调用的 Invoker 集合，到 Context 中
            RpcContext.getContext().setInvokers((List) selected);
            // 异常计数器
            final AtomicInteger count = new AtomicInteger();
            // 创建阻塞队列。通过它，实现线程池异步执行任务的结果通知，非常亮眼。
            final BlockingQueue<Object> ref = new LinkedBlockingQueue<Object>();
            // 循环 selected 集合，提交线程池，发起 RPC 调用
            for (final Invoker<T> invoker : selected) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // RPC 调用，获得 Result 结果
                            Result result = invoker.invoke(invocation);
                            // 添加 Result 到 `ref` 阻塞队列
                            ref.offer(result);
                        } catch (Throwable e) {
                            // 异常计数器 + 1
                            int value = count.incrementAndGet();
                            // 若 RPC 调用结果都是异常，则添加异常到 `ref` 阻塞队列
                            if (value >= selected.size()) {
                                ref.offer(e);
                            }
                        }
                    }
                });
            }
            try {
                // 从 `ref` 队列中，阻塞等待，直到获得到结果或者超时。
                Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);
                // 若是异常结果，抛出 RpcException 异常
                if (ret instanceof Throwable) {
                    Throwable e = (Throwable) ret;
                    throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : 0, "Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e.getCause() != null ? e.getCause() : e);
                }
                // 若是正常结果，直接返回
                return (Result) ret;
            } catch (InterruptedException e) {
                throw new RpcException("Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
            }
        } finally {
            // clear attachments which is binding to current thread.
            RpcContext.getContext().clearAttachments();
        }
    }
}
