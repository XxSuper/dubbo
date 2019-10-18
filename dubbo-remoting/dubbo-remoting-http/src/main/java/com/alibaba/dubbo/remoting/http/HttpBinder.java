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
package com.alibaba.dubbo.remoting.http;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;

/**
 * HttpBinder
 * HTTP 绑定器接口，负责创建对应的 HttpServer 对象。
 */
// @SPI("jetty") 注解，Dubbo SPI 拓展点，默认为 "jetty" ，即未配置情况下，使用 Jetty Server。
@SPI("jetty")
public interface HttpBinder {

    /**
     * bind the server.
     *
     * @param url server url.
     * @return server.
     */
    // @Adaptive({Constants.SERVER_KEY}) 注解，基于 Dubbo SPI Adaptive 机制，加载对应的 Server 实现，使用 URL.server 属性。
    @Adaptive({Constants.SERVER_KEY})
    HttpServer bind(URL url, HttpHandler handler);

}