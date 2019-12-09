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
package com.alibaba.dubbo.registry.integration;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.registry.support.ProviderConsumerRegTable;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.dubbo.common.Constants.ACCEPT_FOREIGN_IP;
import static com.alibaba.dubbo.common.Constants.QOS_ENABLE;
import static com.alibaba.dubbo.common.Constants.QOS_PORT;
import static com.alibaba.dubbo.common.Constants.VALIDATION_KEY;
import static com.alibaba.dubbo.common.Constants.CATEGORY_KEY;
import static com.alibaba.dubbo.common.Constants.CONSUMERS_CATEGORY;
import static com.alibaba.dubbo.common.Constants.CHECK_KEY;

/**
 * RegistryProtocol
 * 实现 Protocol 接口，注册中心协议实现类
 */
public class RegistryProtocol implements Protocol {

    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);

    /**
     * 静态属性，单例。在 Dubbo SPI 中，被初始化，有且仅有一次。
     */
    private static RegistryProtocol INSTANCE;

    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();

    /**
     * 绑定关系集合。
     *
     * key：服务提供者 Dubbo URL
     */
    //To solve the problem of RMI repeated exposure port conflicts, the services that have been exposed are no longer exposed.
    //providerurl <--> exporter
    // 用于解决rmi重复暴露端口冲突的问题，已经暴露过的服务不再重新暴露
    // providerurl <--> exporter
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();

    /**
     * Cluster 自适应拓展实现类对象，Cluster$Adaptive 对象
     */
    private Cluster cluster;

    /**
     * Protocol 自适应拓展实现类，通过 Dubbo SPI 自动注入。
     */
    private Protocol protocol;

    /**
     * RegistryFactory 自适应拓展实现类，通过 Dubbo SPI 自动注入。用于创建注册中心 Registry 对象。
     */
    private RegistryFactory registryFactory;

    private ProxyFactory proxyFactory;

    public RegistryProtocol() {
        INSTANCE = this;
    }

    /**
     * 静态方法，获得单例
     * @return
     */
    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }

    //Filter the parameters that do not need to be output in url(Starting with .)
    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[]{};
        }
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return 9090;
    }

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    public void register(URL registryUrl, URL registedProviderUrl) {
        Registry registry = registryFactory.getRegistry(registryUrl);
        registry.register(registedProviderUrl);
    }

    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        //export invoker
        // 暴露服务
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        // 获得注册中心 URL
        URL registryUrl = getRegistryUrl(originInvoker);

        //registry provider
        // 获得注册中心对象
        final Registry registry = getRegistry(originInvoker);

        // 获得服务提供者 URL
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);

        //to judge to delay publish whether or not
        // 配置项 register ，服务提供者是否注册到配置中心
        boolean register = registeredProviderUrl.getParameter("register", true);

        // 向本地注册表，注册服务提供者
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);

        // 向注册中心注册服务提供者（自己）
        if (register) {
            register(registryUrl, registeredProviderUrl);
            // 标记，向本地注册表注册的服务提供者，已经注册
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }

        // 使用 OverrideListener 对象，订阅配置规则
        // Subscribe the override data
        // FIXME When the provider subscribes, it will affect the scene : a certain JVM exposes the service and call the same service. Because the subscribed is cached key with the name of the service, it causes the subscription information to cover.
        // 创建订阅配置规则的 URL
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);
        // 创建 OverrideListener 对象，并添加到 `overrideListeners` 中
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);
        // 向注册中心，发起订阅
        // 向注册中心注册 OverrideListener 监听器，订阅配置规则的变化。
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
        //Ensure that a new exporter instance is returned every time export
        return new DestroyableExporter<T>(exporter, originInvoker, overrideSubscribeUrl, registeredProviderUrl);
    }

    /**
     * 暴露服务
     *
     * 此处的 Local 指的是，本地启动服务，但是不包括向注册中心注册服务的意思
     *
     * @param originInvoker 原始 Invoker
     * @param <T> 泛型
     * @return Exporter 对象
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {
        // 获得在 `bounds` 中的缓存 Key
        String key = getCacheKey(originInvoker);
        // 从 `bounds` 获得，是不是已经暴露过服务
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                // 未暴露过，进行暴露服务
                if (exporter == null) {
                    // 获得服务提供者的 URL，创建 Invoker Delegate 对象
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));
                    // 调用 DubboProtocol#export(invoker) 方法，暴露服务，返回 Exporter 对象
                    // 使用 创建的Exporter对象 + originInvoker ，创建 ExporterChangeableWrapper 对象。这样，originInvoker 就和 Exporter 对象，形成了绑定的关系。
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);

                    // 添加到 `bounds`
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    /**
     * Reexport the invoker of the modified url
     *
     * 原来的 Exporter 不进行销毁么?实际上不需要，原因有两点：
     * 1、每个协议初始化的 Server 有缓存 ，所以重新初始化，可以重用缓存中的 Server 。
     * 2、如果销毁原有 Exporter ，会导致缓存的 Server 也一起销毁。而且，即使不销毁，原有 Exporter 也就是一个对象，可以被回收掉。
     *
     * @param originInvoker
     * @param newInvokerUrl
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        // 校验对应的 Exporter 是否存在。若不存在，打印告警日志。
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
        } else {
            // 创建 InvokerDelegete 对象
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            // 重新暴露 Invoker
            // 设置到 ExporterChangeableWrapper 中
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * Get an instance of registry based on the address of invoker
     * 获得注册中心 URL
     *
     * @param originInvoker originInvoker 原始 Invoker
     * @return URL
     */
    private Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        return registryFactory.getRegistry(registryUrl);
    }

    private URL getRegistryUrl(Invoker<?> originInvoker) {
        URL registryUrl = originInvoker.getUrl();
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        return registryUrl;
    }


    /**
     * Return the url that is registered to the registry and filter the url parameter once
     *
     * @param originInvoker
     * @return
     */
    private URL getRegisteredProviderUrl(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);

        //The address you see at the registry
        // 移除多余的参数。因为，这些参数注册到注册中心没有实际的用途
        // 移除 .hide 为前缀的参数
        return providerUrl.removeParameters(getFilteredKeys(providerUrl))
                // 移除 .hide 为前缀的参数
                .removeParameter(Constants.MONITOR_KEY)
                // bind.ip
                .removeParameter(Constants.BIND_IP_KEY)
                // bind.port
                .removeParameter(Constants.BIND_PORT_KEY)
                // qos.enable
                .removeParameter(QOS_ENABLE)
                // qos.port
                .removeParameter(QOS_PORT)
                // qos.accept.foreign.ip
                .removeParameter(ACCEPT_FOREIGN_IP)
                // validation
                .removeParameter(VALIDATION_KEY);
    }

    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL)
                .addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY,
                        // 订阅失败，不校验
                        Constants.CHECK_KEY, String.valueOf(false));
    }

    /**
     * Get the address of the providerUrl through the url of the invoker
     *
     * @param origininvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> origininvoker) {
        // 从注册中心的 URL 中获得 export 参数对应的值，即服务提供者的 URL
        String export = origininvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }

        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * Get the key cached in bounds by invoker
     * 获取 invoker 在 bounds 中 缓存的key
     *
     * @param originInvoker 原始 Invoker
     * @return url 字符串
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        // 获得真实的注册中心的 URL
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        // 获得注册中心 Registry 对象 TODO
        Registry registry = registryFactory.getRegistry(url);
        // TODO
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // 获得服务引用配置参数集合
        // group="a,b" or group="*"
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        String group = qs.get(Constants.GROUP_KEY);
        // 分组聚合，参见文档 http://dubbo.apache.org/zh-cn/docs/user/demos/group-merger.html
        if (group != null && group.length() > 0) {
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                // 执行服务引用，获得可合并的 Cluster 对象 TODO
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }
        return doRefer(cluster, registry, type, url);
    }

    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    /**
     * 执行服务引用，返回 Invoker 对象
     * @param cluster Cluster 对象
     * @param registry 注册中心对象
     * @param type 服务接口类型
     * @param url 注册中心 URL
     * @param <T> 泛型
     * @return Invoker 对象
     */
    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // 创建 RegistryDirectory 对象，并设置注册中心、协议。通过它，可以注册到一个注册中心的所有服务提供者
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        // 创建订阅 URL
        // all attributes of REFER_KEY
        // 服务引用配置集合。注意，url 传入 RegistryDirectory 后，经过处理并重新创建，所以 url != directory.url ，所以获得的是服务引用配置集合。
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());
        // 创建订阅 URL 对象
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);
        // 向注册中心注册自己（服务消费者）
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            // 服务消费者注册 URL
            URL registeredConsumerUrl = getRegisteredConsumerUrl(subscribeUrl, url);
            // 向注册中心注册服务消费者 TODO
            registry.register(registeredConsumerUrl);
            directory.setRegisteredConsumerUrl(registeredConsumerUrl);
        }
        // 向注册中心订阅服务提供者+ 路由规则 + 配置规则 TODO
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY,
                Constants.PROVIDERS_CATEGORY
                        + "," + Constants.CONFIGURATORS_CATEGORY
                        + "," + Constants.ROUTERS_CATEGORY));

        // 创建 Invoker 对象。因为 cluster 是 Dubbo SPI Adaptive 类，所以可以自动获取到对应的 Cluster 实现类。
        Invoker invoker = cluster.join(directory);
        // 向本地注册表，注册消费者 TODO
        ProviderConsumerRegTable.registerConsumer(invoker, url, subscribeUrl, directory);
        return invoker;
    }

    public URL getRegisteredConsumerUrl(final URL consumerUrl, URL registryUrl) {
        return consumerUrl.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY,
                CHECK_KEY, String.valueOf(false));
    }

    @Override
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    /**
     * InvokerDelegete 实现 com.alibaba.dubbo.rpc.protocol.InvokerWrapper 类
     *
     * 主要增加了 #getInvoker() 方法，获得真实的，非 InvokerDelegete 的 Invoker 对象。因为，可能会存在 InvokerDelegete.invoker 也是 InvokerDelegete 类型的情况。
     * @param <T>
     */
    public static class InvokerDelegete<T> extends InvokerWrapper<T> {
        private final Invoker<T> invoker;

        /**
         * @param invoker
         * @param url     invoker.getUrl return this value
         */
        public InvokerDelegete(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegete) {
                return ((InvokerDelegete<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    /**
     * Reexport: the exporter destroy problem in protocol
     * 1.Ensure that the exporter returned by registryprotocol can be normal destroyed
     * 2.No need to re-register to the registry after notify
     * 3.The invoker passed by the export method , would better to be the invoker of exporter
     *
     * OverrideListener 是 RegistryProtocol 内部类，实现 NotifyListener 接口
     * 重新 export ：protocol 中的 exporter destroy 问题
     *
     * 1. 要求 registry protocol 返回的 exporter 可以正常 destroy
     * 2. notify 后不需要重新向注册中心注册
     * 3. export 方法传入的 invoker 最好能一直作为 exporter 的 invoker.
     *
     *
     */
    private class OverrideListener implements NotifyListener {

        /**
         * 订阅 URL 对象
         */
        private final URL subscribeUrl;

        /**
         * 原始 Invoker 对象
         */
        private final Invoker originInvoker;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * @param urls The list of registered information , is always not empty, The meaning is the same as the return value of {@link com.alibaba.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            // 获得匹配的规则配置 URL 集合
            logger.debug("original override urls: " + urls);
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            // 将配置规则 URL 集合，**转换**成对应的 Configurator 集合
            List<Configurator> configurators = RegistryDirectory.toConfigurators(matchedUrls);

            // 获得真实的 Invoker 对象
            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }

            // 获得真实的 Invoker 的 URL 对象
            //The origin invoker
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);

            // 若对应的 Exporter 对象不存在
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }

            // 获得 Invoker 当前的 URL 对象，可能已经被之前的配置规则合并过
            //The current, may have been merged many times
            URL currentUrl = exporter.getInvoker().getUrl();

            // 基于 originUrl 对象，合并配置规则，生成新的 newUrl 对象
            //Merged with this configuration
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            // 判断新老 Url 不匹配，重新暴露 Invoker
            if (!currentUrl.equals(newUrl)) {
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getParameter(Constants.CATEGORY_KEY) == null
                        && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                // 进行判断是否匹配。
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        //Merge the urls of configurators
        private URL getConfigedInvokerUrl(List<Configurator> configurators, URL url) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
            return url;
        }
    }

    /**
     * exporter proxy, establish the corresponding relationship between the returned exporter and the exporter exported by the protocol, and can modify the relationship at the time of override.
     * 实现 Exporter 接口，Exporter 可变的包装器
     *
     * 建立【返回的 Exporter】与【Protocol export 出的 Exporter】的对应关系
     * Exporter 代理，建立原始的 Invoker 与 Protocol#export(Invoker<T> invoker) 方法返回的 Exporter 的对应关系。
     *
     * 在 override 时可以进行关系修改，在配置规则发生变化时，可调用 #setExporter(Exporter<T>) 方法，修改对应关系
     *
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        /**
         * 原 Invoker 对象
         */
        private final Invoker<T> originInvoker;

        /**
         * 暴露的 Exporter 对象
         */
        private Exporter<T> exporter;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        // 可以重新设置 Exporter 对象
        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            // 移除出 `bounds`
            bounds.remove(key);
            // 取消暴露
            exporter.unexport();
        }
    }

    /**
     * 实现 Exporter 接口，可销毁的 Exporter 实现类
     * @param <T>
     */
    static private class DestroyableExporter<T> implements Exporter<T> {

        public static final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        /**
         * 暴露的 Exporter 对象
         */
        private Exporter<T> exporter;

        /**
         * 原 Invoker 对象
         */
        private Invoker<T> originInvoker;
        private URL subscribeUrl;
        private URL registerUrl;

        public DestroyableExporter(Exporter<T> exporter, Invoker<T> originInvoker, URL subscribeUrl, URL registerUrl) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
            this.subscribeUrl = subscribeUrl;
            this.registerUrl = registerUrl;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            Registry registry = RegistryProtocol.INSTANCE.getRegistry(originInvoker);
            try {
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            try {
                NotifyListener listener = RegistryProtocol.INSTANCE.overrideListeners.remove(subscribeUrl);
                registry.unsubscribe(subscribeUrl, listener);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int timeout = ConfigUtils.getServerShutdownTimeout();
                        if (timeout > 0) {
                            logger.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. Usually, this is called when you use dubbo API");
                            Thread.sleep(timeout);
                        }
                        exporter.unexport();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            });
        }
    }
}
