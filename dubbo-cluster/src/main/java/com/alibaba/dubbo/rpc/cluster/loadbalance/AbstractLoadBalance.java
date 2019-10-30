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
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * AbstractLoadBalance
 * 实现 LoadBalance 接口，LoadBalance 抽象类，提供了权重计算的功能。
 */
public abstract class AbstractLoadBalance implements LoadBalance {

    /**
     * 计算权重的代码这么写看起来比较“绕”，我们来修改成 (uptime / warmup) * weight ，是否就好理解多了，相当于进度百分比 * 权重。
     *
     * 根据calculateWarmupWeight()方法实现可知，随着provider的启动时间越来越长，慢慢提升权重直到weight，且权重最小值为1，所以：
     *
     * 如果 provider 运行了 1 分钟，那么 weight 为 10，即只有最终需要承担的 10% 流量；
     * 如果 provider 运行了 2 分钟，那么 weight 为 20，即只有最终需要承担的 20% 流量；
     * 如果 provider 运行了 5 分钟，那么 weight 为 50，即只有最终需要承担的 50% 流量；
     *  … …
     * 如果 provider 运行了 10 分钟，那么 weight 为 100，即只有最终需要承担的 100% 流量；
     * "weight" 配置项，默认为 100 。"warmup" 配置项，默认为 10 * 60 * 1000 = 10 分钟。
     *
     * @param uptime
     * @param warmup
     * @param weight
     * @return
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        // 计算权重
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        // 权重范围为 [0, weight] 之间
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

    /**
     * #select(List<Invoker<T>>, URL, Invocation) 实现方法，默认只有一个 Invoker 时，直接选择返回。
     * @param invokers   invokers.
     * @param url        refer url
     * @param invocation invocation.
     * @param <T>
     * @return
     */
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (invokers == null || invokers.isEmpty())
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        return doSelect(invokers, url, invocation);
    }

    /**
     * 子类实现 #doSelect(List<Invoker<T>>, URL, Invocation) 抽象方法，提供自定义的负载均衡策略。
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        // 获得 weight 配置，即服务权重。默认为 100
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
        if (weight > 0) {
            long timestamp = invoker.getUrl().getParameter(Constants.REMOTE_TIMESTAMP_KEY, 0L);
            if (timestamp > 0L) {
                // 获得启动总时长
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                // 获得预热需要总时长。默认为 10 * 60 * 1000 = 10 分钟
                int warmup = invoker.getUrl().getParameter(Constants.WARMUP_KEY, Constants.DEFAULT_WARMUP);
                // 处于预热中，计算当前的权重
                if (uptime > 0 && uptime < warmup) {
                    // 考虑到 JVM 自身会有预热的过程，所以服务提供者一启动就直接承担 100% 的流量，可能会出现很吃力的情况。因此权重的计算，默认自带了预热的过程。
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight;
    }

}
