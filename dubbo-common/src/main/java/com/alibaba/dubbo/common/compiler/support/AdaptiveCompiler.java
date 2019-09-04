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
package com.alibaba.dubbo.common.compiler.support;


import com.alibaba.dubbo.common.compiler.Compiler;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.ExtensionLoader;

/**
 * AdaptiveCompiler. (SPI, Singleton, ThreadSafe)
 * 实现 Compiler 接口，自适应 Compiler 实现类
 */
@Adaptive
public class AdaptiveCompiler implements Compiler {

    /**
     * 默认编译器的拓展名
     */
    private static volatile String DEFAULT_COMPILER;

    /**
     * 静态方法，设置默认编译器的拓展名。该方法被 ApplicationConfig#setCompiler(compiler) 方法调用。在 <dubbo:application compiler="" /> 配置下，可触发该方法。
     * @param compiler
     */
    public static void setDefaultCompiler(String compiler) {
        DEFAULT_COMPILER = compiler;
    }

    @Override
    public Class<?> compile(String code, ClassLoader classLoader) {
        Compiler compiler;
        // 获得 Compiler 的 ExtensionLoader 对象。
        ExtensionLoader<Compiler> loader = ExtensionLoader.getExtensionLoader(Compiler.class);
        // 声明 name 变量，引用 DEFAULT_COMPILER 的值
        String name = DEFAULT_COMPILER; // copy reference
        if (name != null && name.length() > 0) {
            // 使用设置的拓展名，获得 Compiler 拓展对象
            compiler = loader.getExtension(name);
        } else {
            // 获得默认的 Compiler 拓展对象
            compiler = loader.getDefaultExtension();
        }
        // 调用真正的 Compiler 对象，动态编译代码
        return compiler.compile(code, classLoader);
    }

}
