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

import java.util.List;
import java.util.Random;

/**
 * random load balance.
 * 实现 AbstractLoadBalance 抽象类，随机，按权重设置随机概率。
 * 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重。
 *
 * 假定有3台dubbo provider:
 *
 * 10.0.0.1:20884, weight=2
 * 10.0.0.1:20886, weight=3
 * 10.0.0.1:20888, weight=4
 * 随机算法的实现：
 * totalWeight = 9;
 *
 * 假设offset=1（即random.nextInt(9)=1）
 * 1-2=-1<0？是，所以选中 10.0.0.1:20884, weight=2
 * 假设offset=4（即random.nextInt(9)=4）
 * 4-2=2<0？否，这时候offset=2， 2-3<0？是，所以选中 10.0.0.1:20886, weight=3
 * 假设offset=7（即random.nextInt(9)=7）
 * 7-2=5<0？否，这时候offset=5， 5-3=2<0？否，这时候offset=2， 2-4<0？是，所以选中 10.0.0.1:20888, weight=4
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // invoker 集合数量
        int length = invokers.size(); // Number of invokers
        // 总的权重
        int totalWeight = 0; // The sum of weights
        boolean sameWeight = true; // Every invoker has the same weight?
        // 计算总权重，并判断所有 Invoker 是否相同权重。
        for (int i = 0; i < length; i++) {
            // 获得每个 invoker 的权重
            int weight = getWeight(invokers.get(i), invocation);
            totalWeight += weight; // Sum
            // 当前 invoker 的权重与上一个 invoker 的权重是否不等
            if (sameWeight && i > 0
                    && weight != getWeight(invokers.get(i - 1), invocation)) {
                sameWeight = false;
            }
        }
        // 权重不相等，随机权重后，判断在哪个 Invoker 的权重区间中
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            // 随机
            int offset = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            // 区间判断
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        // 权重相等，平均随机
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(random.nextInt(length));
    }

}
