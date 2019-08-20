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
package com.alibaba.dubbo.rpc.protocol.injvm;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProtocol;
import com.alibaba.dubbo.rpc.support.ProtocolUtils;

import java.util.Map;

/**
 * InjvmProtocol
 *
 * 实现 AbstractProtocol 抽象类，Injvm 协议实现类
 */
public class InjvmProtocol extends AbstractProtocol implements Protocol {

    /**
     * injvm 协议名
     */
    public static final String NAME = Constants.LOCAL_PROTOCOL;

    /**
     * 默认端口
     */
    public static final int DEFAULT_PORT = 0;

    /**
     * 单例。在 Dubbo SPI 中，被初始化，有且仅有一次。
     */
    private static InjvmProtocol INSTANCE;

    public InjvmProtocol() {
        INSTANCE = this;
    }

    /**
     * 获得单例
     * @return INSTANCE
     */
    public static InjvmProtocol getInjvmProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(InjvmProtocol.NAME); // load
        }
        return INSTANCE;
    }

    /**
     * 获得 Exporter 对象
     *
     * @param map Exporter 集合
     * @param key URL
     * @return Exporter
     */
    static Exporter<?> getExporter(Map<String, Exporter<?>> map, URL key) {
        Exporter<?> result = null;
        // 全匹配
        if (!key.getServiceKey().contains("*")) {
            result = map.get(key.getServiceKey());
        // 带 * 时，循环匹配，依然匹配的是 `group` `version` `interface` 属性。带 * 的原因是，version = * ，所有版本
        } else {
            if (map != null && !map.isEmpty()) {
                for (Exporter<?> exporter : map.values()) {
                    if (UrlUtils.isServiceKeyMatch(key, exporter.getInvoker().getUrl())) {
                        result = exporter;
                        break;
                    }
                }
            }
        }

        if (result == null) {
            return null;
        // 泛化调用
        } else if (ProtocolUtils.isGeneric(
                result.getInvoker().getUrl().getParameter(Constants.GENERIC_KEY))) {
            return null;
        } else {
            return result;
        }
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * 创建 InjvmExporter 对象xs
     * @param invoker Service invoker 服务的执行体
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        return new InjvmExporter<T>(invoker, invoker.getUrl().getServiceKey(), exporterMap);
    }

    @Override
    public <T> Invoker<T> refer(Class<T> serviceType, URL url) throws RpcException {
        // 创建 InjvmInvoker 对象。注意，传入的 exporterMap 参数，包含所有的 InjvmExporter 对象
        return new InjvmInvoker<T>(serviceType, url, url.getServiceKey(), exporterMap);
    }

    /**
     * 是否本地引用
     *
     * @param url URL
     * @return 是否
     */
    public boolean isInjvmRefer(URL url) {
        final boolean isJvmRefer;
        // 获取 scope 属性
        String scope = url.getParameter(Constants.SCOPE_KEY);
        // Since injvm protocol is configured explicitly, we don't need to set any extra flag, use normal refer process.
        // 当 `protocol = injvm` 时，本身已经是 jvm 协议了，走正常流程就是了。
        // 因为 #isInjvmRefer(url) 方法，仅有在 #createProxy(map) 方法中调用，因此实际也不会触发该逻辑
        if (Constants.LOCAL_PROTOCOL.toString().equals(url.getProtocol())) {
            isJvmRefer = false;
        // 当 `scope = local` 或者 `injvm = true` 时，本地引用
        } else if (Constants.SCOPE_LOCAL.equals(scope) || (url.getParameter("injvm", false))) {
            // if it's declared as local reference
            // 'scope=local' is equivalent to 'injvm=true', injvm will be deprecated in the future release
            isJvmRefer = true;
        // 当 `scope = remote` 时，远程引用
        } else if (Constants.SCOPE_REMOTE.equals(scope)) {
            // it's declared as remote reference
            isJvmRefer = false;
        // 当 `generic = true` 时，即使用泛化调用，远程引用。
        } else if (url.getParameter(Constants.GENERIC_KEY, false)) {
            // generic invocation is not local reference
            isJvmRefer = false;
        // 判断当本地已经有 url 对应的 InjvmExporter 时，直接引用。本地已有的服务，不必要使用远程服务，减少网络开销，提升性能。
        } else if (getExporter(exporterMap, url) != null) {
            // by default, go through local reference if there's the service exposed locally
            isJvmRefer = true;
        // 默认，远程引用
        } else {
            isJvmRefer = false;
        }
        return isJvmRefer;
    }
}
