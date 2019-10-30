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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.Random;

/**
 * LeastActiveLoadBalance
 * 实现 AbstractLoadBalance 抽象类，最少活跃调用数，相同活跃数的随机，活跃数指调用前后计数差。
 * 使慢的提供者收到更少请求，因为越慢的提供者的调用前后计数差会越大。
 * 相比来说，LeastActiveLoadBalance 是 RandomLoadBalance 的加强版，基于最少活跃调用数。
 *
 * 最小活跃数算法实现：
 * 假定有3台dubbo provider:
 *
 * 10.0.0.1:20884, weight=2，active=2
 * 10.0.0.1:20886, weight=3，active=4
 * 10.0.0.1:20888, weight=4，active=3
 * active=2最小，且只有一个2，所以选择10.0.0.1:20884
 *
 * 假定有3台dubbo provider:
 *
 * 10.0.0.1:20884, weight=2，active=2
 * 10.0.0.1:20886, weight=3，active=2
 * 10.0.0.1:20888, weight=4，active=3
 * active=2最小，且有2个，所以从[10.0.0.1:20884,10.0.0.1:20886 ]中选择；
 * 接下来的算法与随机算法类似：
 *
 * 假设offset=1（即random.nextInt(5)=1）
 * 1-2=-1<0？是，所以选中 10.0.0.1:20884, weight=2
 * 假设offset=4（即random.nextInt(5)=4）
 * 4-2=2<0？否，这时候offset=2， 2-3<0？是，所以选中 10.0.0.1:20886, weight=3
 *
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // invokers 集合总个数
        int length = invokers.size(); // Number of invokers
        // 最小的活跃数
        int leastActive = -1; // The least active value of all invokers
        // 相同最小活跃数的个数
        int leastCount = 0; // The number of invokers having the same least active value (leastActive)
        // 相同最小活跃数的下标
        int[] leastIndexs = new int[length]; // The index of invokers having the same least active value (leastActive)
        // 多个最小活跃数 invoker 的总权重
        int totalWeight = 0; // The sum of with warmup weights
        // 第一个权重，用于于计算是否相同
        int firstWeight = 0; // Initial value, used for comparision
        // 是否所有权重相同
        boolean sameWeight = true; // Every invoker has the same weight value?
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 活跃数，每个 Invoker 的活跃数计算，通过 RpcStatus，在 ActiveLimitFilter && ExecuteLimitFilter 中计算。
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive(); // Active number
            // 权重
            int afterWarmup = getWeight(invoker, invocation); // Weight
            // 发现更小的活跃数，重新开始
            if (leastActive == -1 || active < leastActive) { // Restart, when find a invoker having smaller least active value.
                // 记录最小活跃数
                leastActive = active; // Record the current least active value
                // 重新统计相同最小活跃数的个数
                leastCount = 1; // Reset leastCount, count again based on current leastCount
                // 重新记录最小活跃数下标
                leastIndexs[0] = i; // Reset
                // 重新累计总权重
                totalWeight = afterWarmup; // Reset
                // 记录第一个最小活跃数 invoker 的权重，第一个权重
                firstWeight = afterWarmup; // Record the weight the first invoker
                // 还原权重相同标识
                sameWeight = true; // Reset, every invoker has the same weight value?
            } else if (active == leastActive) { // If current invoker's active value equals with leaseActive, then accumulating.
                // 累计相同最小的活跃数
                leastIndexs[leastCount++] = i; // Record index number of this invoker
                // 累计总权重
                totalWeight += afterWarmup; // Add this invoker's weight to totalWeight.
                // If every invoker has the same weight?
                // 判断所有权重是否一样
                if (sameWeight && i > 0
                        && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // assert(leastCount > 0)
        if (leastCount == 1) {
            // 如果只有一个最小活跃数则直接返回
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexs[0]);
        }
        // ========== 如下部分，和 RandomLoadBalance 类似 ==========
        // 存在多个最小活跃数的 invoker 并且各自的权重不相等
        if (!sameWeight && totalWeight > 0) {
            // 如果权重不相同且权重大于0则按总权重数随机
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            int offsetWeight = random.nextInt(totalWeight) + 1;
            // 并确定随机值落在哪个片断上
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexs[i];
                offsetWeight -= getWeight(invokers.get(leastIndex), invocation);
                if (offsetWeight <= 0)
                    return invokers.get(leastIndex);
            }
        }
        // 如果权重相同或权重为0则均等随机
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexs[random.nextInt(leastCount)]);
    }
}
