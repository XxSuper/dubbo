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
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n times, which means at most n different invokers will be invoked)
 * Note that retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 * 实现 AbstractClusterInvoker 抽象类，FailoverCluster Invoker 实现类。
 * 失败自动切换，当出现失败，重试其它服务器。通常用于读操作，但重试会带来更长延迟。可通过 retries="2" 来设置重试次数(不含第一次)。
 *
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    /**
     * 循环，查找一个 Invoker 对象，进行调用，直到成功。
     * @param invocation
     * @param invokers
     * @param loadbalance
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        // copyinvokers 变量，候选的 Invoker 集合。
        List<Invoker<T>> copyinvokers = invokers;
        // 检查 copyinvokers 即可用 Invoker 集合是否为空，如果为空，那么抛出异常
        checkInvokers(copyinvokers, invocation);
        // 得到最大可调用次数：最大可重试次数 + 1，默认最大可重试次数Constants.DEFAULT_RETRIES = 2
        int len = getUrl().getMethodParameter(invocation.getMethodName(), Constants.RETRIES_KEY, Constants.DEFAULT_RETRIES) + 1;
        if (len <= 0) {
            len = 1;
        }
        // retry loop.
        // 保存最后一次调用的异常
        RpcException le = null; // last exception.
        // 保存已经调用过的 Invoker
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyinvokers.size()); // invoked invokers.
        // 保存已经调用的网络地址集合。
        Set<String> providers = new HashSet<String>(len);
        // failover机制核心实现：如果出现调用失败，那么重试其他服务器
        for (int i = 0; i < len; i++) {
            //Reselect before retry to avoid a change of candidate `invokers`.
            //NOTE: if `invokers` changed, then `invoked` also lose accuracy.
            // 重试时( i > 0 )，进行重新选择，避免重试时，候选 invoker 集合已发生变化.
            // 注意：如果列表发生了变化，那么 invoked 判断会失效，因为 invoker 示例已经改变
            if (i > 0) {
                // 是否已经销毁。
                checkWhetherDestroyed();
                // 根据 Invocation 调用信息从 Directory 中获取所有可用 Invoker
                copyinvokers = list(invocation);
                // check again
                // 重新检查一下
                checkInvokers(copyinvokers, invocation);
            }
            // 根据负载均衡机制从 copyinvokers 中选择一个 Invoker
            Invoker<T> invoker = select(loadbalance, invocation, copyinvokers, invoked);
            // 保存每次调用的 Invoker
            invoked.add(invoker);
            // 设置已经调用的 Invoker 集合，到 Context 中
            RpcContext.getContext().setInvokers((List) invoked);
            try {
                // RPC 调用得到 Result
                Result result = invoker.invoke(invocation);
                // 若 le 非空，说明此时是重试调用成功，将最后一次调用的异常信息以 warn 级别日志输出，方便未来追溯。
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + invocation.getMethodName()
                            + " in the service " + getInterface().getName()
                            + " was successful by the provider " + invoker.getUrl().getAddress()
                            + ", but there have been failed providers " + providers
                            + " (" + providers.size() + "/" + copyinvokers.size()
                            + ") from the registry " + directory.getUrl().getAddress()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                            + le.getMessage(), le);
                }
                return result;
            } catch (RpcException e) {
                // 如果是业务性质的异常，不再重试，直接抛出
                if (e.isBiz()) { // biz exception.
                    throw e;
                }
                // 保存异常到 le 。其他性质的异常统一封装成RpcException
                le = e;
            } catch (Throwable e) {
                // 非 RpcException 异常，封装成 RpcException 异常。
                le = new RpcException(e.getMessage(), e);
            } finally {
                // 保存每次调用的网络地址，到 providers 中。
                providers.add(invoker.getUrl().getAddress());
            }
        }
        // 超过最大调用次数，抛出 RpcException 异常。该异常中，带有最后一次调用异常的信息。
        throw new RpcException(le != null ? le.getCode() : 0, "Failed to invoke the method "
                + invocation.getMethodName() + " in the service " + getInterface().getName()
                + ". Tried " + len + " times of the providers " + providers
                + " (" + providers.size() + "/" + copyinvokers.size()
                + ") from the registry " + directory.getUrl().getAddress()
                + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                + Version.getVersion() + ". Last error is: "
                + (le != null ? le.getMessage() : ""), le != null && le.getCause() != null ? le.getCause() : le);
    }

}
