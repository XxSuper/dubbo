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
package com.alibaba.dubbo.rpc.protocol.dubbo.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.StaticContext;
import com.alibaba.dubbo.rpc.protocol.dubbo.FutureAdapter;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Future;

/**
 * EventFilter
 * 实现 Filter 接口，事件通知过滤器
 */
// @Activate(group = Constants.CONSUMER) 注解，基于 Dubbo SPI Activate 机制，只有服务消费者才生效该过滤器。
@Activate(group = Constants.CONSUMER)
public class FutureFilter implements Filter {

    protected static final Logger logger = LoggerFactory.getLogger(FutureFilter.class);

    @Override
    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
        // 获得是否异步调用
        final boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);

        // 触发前置方法
        fireInvokeCallback(invoker, invocation);

        // need to configure if there's return value before the invocation in order to help invoker to judge if it's
        // necessary to return future.
        // 调用方法，调用服务提供者，即 Dubbo RPC 。
        Result result = invoker.invoke(invocation);

        // 触发回调方法
        if (isAsync) {
            // 异步回调
            asyncCallback(invoker, invocation);
        } else {
            // 同步回调
            syncCallback(invoker, invocation, result);
        }
        // 返回结果。如果是异步调用或单向调用，所以返回结果是空的。
        return result;
    }

    /**
     * 同步回调
     *
     * @param invoker Invoker 对象
     * @param invocation Invocation 对象
     * @param result RPC 结果
     */
    private void syncCallback(final Invoker<?> invoker, final Invocation invocation, final Result result) {
        // 异常，触发异常回调
        if (result.hasException()) {
            fireThrowCallback(invoker, invocation, result.getException());
        } else {
            // 正常，触发正常回调
            fireReturnCallback(invoker, invocation, result.getValue());
        }
    }

    /**
     * 异步回调
     *
     * @param invoker Invoker 对象
     * @param invocation Invocation 对象
     */
    private void asyncCallback(final Invoker<?> invoker, final Invocation invocation) {
        // 从ThreadLocal 中获得 Future 对象，在 DubboInvoker 中进行设置
        Future<?> f = RpcContext.getContext().getFuture();
        if (f instanceof FutureAdapter) {
            ResponseFuture future = ((FutureAdapter<?>) f).getFuture();
            // 触发回调
            future.setCallback(new ResponseCallback() {

                /**
                 * 触发正常回调方法
                 *
                 * @param rpcResult RPC 结果
                 */
                @Override
                public void done(Object rpcResult) {
                    if (rpcResult == null) {
                        logger.error(new IllegalStateException("invalid result value : null, expected " + Result.class.getName()));
                        return;
                    }
                    ///must be rpcResult
                    if (!(rpcResult instanceof Result)) {
                        logger.error(new IllegalStateException("invalid result type :" + rpcResult.getClass() + ", expected " + Result.class.getName()));
                        return;
                    }
                    Result result = (Result) rpcResult;
                    if (result.hasException()) {
                        // 触发异常回调方法
                        fireThrowCallback(invoker, invocation, result.getException());
                    } else {
                        // 触发正常回调方法
                        fireReturnCallback(invoker, invocation, result.getValue());
                    }
                }

                /**
                 * 触发异常回调方法
                 *
                 * @param exception 异常
                 */
                @Override
                public void caught(Throwable exception) {
                    fireThrowCallback(invoker, invocation, exception);
                }
            });
        }
    }

    /**
     * 触发前置方法
     * @param invoker Invoker 对象
     * @param invocation Invocation 对象
     */
    private void fireInvokeCallback(final Invoker<?> invoker, final Invocation invocation) {
        // 获得前置方法和对象 - ReferenceConfig init() 方法中调用 appendAttributes() checkAndConvertImplicitConfig() 进行赋值
        final Method onInvokeMethod = (Method) StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_INVOKE_METHOD_KEY));
        final Object onInvokeInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_INVOKE_INSTANCE_KEY));

        if (onInvokeMethod == null && onInvokeInst == null) {
            return;
        }
        if (onInvokeMethod == null || onInvokeInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a onreturn callback config , but no such " + (onInvokeMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }
        if (!onInvokeMethod.isAccessible()) {
            onInvokeMethod.setAccessible(true);
        }

        // 反射调用前置方法。
        Object[] params = invocation.getArguments();
        try {
            onInvokeMethod.invoke(onInvokeInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    private void fireReturnCallback(final Invoker<?> invoker, final Invocation invocation, final Object result) {
        // 获得 `onreturn` 方法和对象
        final Method onReturnMethod = (Method) StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_RETURN_METHOD_KEY));
        final Object onReturnInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_RETURN_INSTANCE_KEY));

        //not set onreturn callback
        if (onReturnMethod == null && onReturnInst == null) {
            return;
        }

        if (onReturnMethod == null || onReturnInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a onreturn callback config , but no such " + (onReturnMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }

        // 设置为可访问
        if (!onReturnMethod.isAccessible()) {
            onReturnMethod.setAccessible(true);
        }

        // 参数数组
        Object[] args = invocation.getArguments();
        Object[] params;

        // 获取 onReturnMethod 方法参数类型
        Class<?>[] rParaTypes = onReturnMethod.getParameterTypes();
        if (rParaTypes.length > 1) {
            // onReturnMethod 参数为2个，并且第二个参数为 Object[]
            if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                params = new Object[2];
                // 第一个参数设置为服务提供者调用方法的执行结果
                params[0] = result;
                // 第二个参数设置为服务提供者调用方法的调用参数
                params[1] = args;
            } else {
                // onReturnMethod 参数大于2个
                params = new Object[args.length + 1];
                // 第一个参数设置为服务提供者调用方法的执行结果
                params[0] = result;
                // 其他参数为服务提供者调用方法的调用参数
                System.arraycopy(args, 0, params, 1, args.length);
            }
        } else {
            // onReturnMethod 只有一个参数，设置为服务提供者调用方法的执行结果
            params = new Object[]{result};
        }
        try {
            // 执行回调方法
            onReturnMethod.invoke(onReturnInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    private void fireThrowCallback(final Invoker<?> invoker, final Invocation invocation, final Throwable exception) {
        // 获得 `onthrow` 方法和对象
        final Method onthrowMethod = (Method) StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_THROW_METHOD_KEY));
        final Object onthrowInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_THROW_INSTANCE_KEY));

        //onthrow callback not configured
        if (onthrowMethod == null && onthrowInst == null) {
            return;
        }
        if (onthrowMethod == null || onthrowInst == null) {
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() + " has a onthrow callback config , but no such " + (onthrowMethod == null ? "method" : "instance") + " found. url:" + invoker.getUrl());
        }

        // 设置为可访问
        if (!onthrowMethod.isAccessible()) {
            onthrowMethod.setAccessible(true);
        }

        // 获取 onthrowMethod 方法参数类型
        Class<?>[] rParaTypes = onthrowMethod.getParameterTypes();

        // onthrowMethod 方法参数类型第一个参数为 exception
        if (rParaTypes[0].isAssignableFrom(exception.getClass())) {
            try {
                // 参数数组
                Object[] args = invocation.getArguments();
                Object[] params;

                if (rParaTypes.length > 1) {
                    // onthrowMethod 方法参数为2个，并且第二个参数为 Object[]
                    if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)) {
                        params = new Object[2];
                        // 第一个参数设置为 exception
                        params[0] = exception;
                        // 第二个参数设置为服务提供者调用方法的调用参数
                        params[1] = args;
                    } else {
                        // onthrowMethod 方法参数大于2个
                        params = new Object[args.length + 1];
                        // 第一个参数设置为 exception
                        params[0] = exception;
                        // 其他参数为服务提供者调用方法的调用参数
                        System.arraycopy(args, 0, params, 1, args.length);
                    }
                } else {
                    // onthrowMethod 只有一个参数，设置为 exception
                    params = new Object[]{exception};
                }
                // 调用 onthrowMethod 方法
                onthrowMethod.invoke(onthrowInst, params);
            } catch (Throwable e) {
                logger.error(invocation.getMethodName() + ".call back method invoke error . callback method :" + onthrowMethod + ", url:" + invoker.getUrl(), e);
            }
        } else {
            // 不符合异常，打印错误日志
            logger.error(invocation.getMethodName() + ".call back method invoke error . callback method :" + onthrowMethod + ", url:" + invoker.getUrl(), exception);
        }
    }
}
