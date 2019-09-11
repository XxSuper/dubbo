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
package com.alibaba.dubbo.rpc.proxy;

import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcInvocation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * InvokerHandler
 * 实现 java.lang.reflect.InvocationHandler 接口，可以实现 Proxy 和真正的逻辑解耦
 *
 * 每一个代理都要实现接口InvocationHandler，通过invoke进行调用方法
 */
public class InvokerInvocationHandler implements InvocationHandler {

    /**
     * Invoker 对象，用于在 #invoke() 方法调用
     */
    private final Invoker<?> invoker;

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
    }

    /**
     * @param proxy 所代理的那个真实对象
     * @param method 所要调用真实对象的某个方法的Method对象
     * @param args 调用真实对象某个方法时接收的参数
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 获取方法名
        String methodName = method.getName();
        // 获取方法参数类型
        Class<?>[] parameterTypes = method.getParameterTypes();
        // method.getDeclaringClass() 返回表示声明由此Method对象表示的方法的类的Class对象。
        if (method.getDeclaringClass() == Object.class) {
            // 处理 #wait() #notify() 等方法，进行反射调用。
            return method.invoke(invoker, args);
        }
        // 基础方法，使用 Invoker 对象的方法，不进行 RPC 调用
        if ("toString".equals(methodName) && parameterTypes.length == 0) {
            return invoker.toString();
        }
        if ("hashCode".equals(methodName) && parameterTypes.length == 0) {
            return invoker.hashCode();
        }
        if ("equals".equals(methodName) && parameterTypes.length == 1) {
            return invoker.equals(args[0]);
        }
        // RPC 调用，调用 Result#recreate() 方法，回放调用结果。
        return invoker.invoke(new RpcInvocation(method, args)).recreate();
    }

}
