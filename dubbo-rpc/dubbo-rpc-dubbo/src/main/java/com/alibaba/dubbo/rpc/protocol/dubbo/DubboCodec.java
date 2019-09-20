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
package com.alibaba.dubbo.rpc.protocol.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.io.Bytes;
import com.alibaba.dubbo.common.io.UnsafeByteArrayInputStream;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.codec.ExchangeCodec;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcInvocation;

import java.io.IOException;
import java.io.InputStream;

import static com.alibaba.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.encodeInvocationArgument;

/**
 * Dubbo codec.
 * 实现 Codec2 接口，继承 ExchangeCodec 类，Dubbo 编解码器实现类。
 *
 * dubbo 协议采用固定长度的消息头（16字节）和不定长度的消息体来进行数据传输，消息头定义了一些通讯框架netty在IO线程处理时需要的信息。具体dubbo协议的报文格式如下：
 *
 * 消息头报文格式：
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * |                                                                                                                                                                |
 * |                                                                              DUBBO Protocol                                                                    |
 * |                                                                                                                                                                |
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * |              |             |         |            |         |                     |              |                   |                                         |
 * |     0-7      |     8-15    |    16   |     17     |    18   |        19-23        |     24-31    |       32-95       |                   96-127                |
 * |              |             |         |            |         |                     |              |                   |                                         |
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * |              |             |                                                      |              |                   |                                         |
 * |    1byte     |    1byte    |                        1byte                         |     1byte    |       8byte       |                     4byte               |
 * |              |             |                                                      |              |                   |                                         |
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * |              |             |         |            |         |                     |              |                   |                                         |
 * |  MAGIC-HIGH  |  MAGIC-LOW  | REQUEST |  TWOWAY    |   EVENT |   Serialization Id  |  status      | 请求序号Invoke ID  |           消息总长度 Body Length         |
 * |              |             |         |            |         |                     |              |                   |                                         |
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 * |                            |                                                      |              |                   |                                         |
 * |           MAGIC            |                            FLAG                      |    Status    |     Invoke ID     |              Body Length                |
 * |                            |                                                      |              |                   |                                         |
 * +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
 *
 * magic：2字节的魔数。类似 java 字节码文件里的魔数，用来判断是不是 dubbo 协议的数据包。魔数是常量0xdabb，用于判断报文的开始。
 * flag：标志位，一共8个地址位。低四位用来表示消息体数据用的序列化工具的类型（默认hessian），高四位中，第一位为1表示是request请求，第二位为1表示双向传输（即有返回response），第三位为1表示是心跳ping事件。
 * status：状态位, 设置请求响应状态，dubbo定义了一些响应的类型。
 * invoke id：消息id, long 类型。每一个请求的唯一识别id（由于采用异步通讯的方式，用来把请求request和返回的response对应上）
 * body length：消息体 body 长度, int 类型，即记录Body Content有多少个字节。
 */
public class DubboCodec extends ExchangeCodec implements Codec2 {

    /**
     * 协议名
     */
    public static final String NAME = "dubbo";

    /**
     * 协议版本
     */
    public static final String DUBBO_VERSION = Version.getProtocolVersion();

    /**
     * 响应 - 异常
     */
    public static final byte RESPONSE_WITH_EXCEPTION = 0;

    /**
     * 响应 - 正常（有返回）
     */
    public static final byte RESPONSE_VALUE = 1;

    /**
     * 响应 - 正常（空返回）
     */
    public static final byte RESPONSE_NULL_VALUE = 2;

    /**
     * 响应 - 异常带隐式属性
     */
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;

    /**
     * 响应 - 正常（有返回）带隐式属性
     */
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;

    /**
     * 响应 - 正常（空返回）带隐式属性
     */
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;

    /**
     * 方法参数 - 空（参数）
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * 方法参数 - 空（类型）
     */
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);

    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 读取消息标记位和协议类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id. 读取请求id
        long id = Bytes.bytes2long(header, 4);
        // 解析响应
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response. 客户端对响应进行解码
            Response res = new Response(id);
            // // 若是心跳事件，进行设置
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // get status. 设置状态
            byte status = header[3];
            res.setStatus(status);
            try {
                // 获取序列化协议，对输入数据进行解码，这里会根据序列化协议 id 和名称来校验序列化协议是否合法
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                // 正常响应状态
                if (status == Response.OK) {
                    Object data;
                    if (res.isHeartbeat()) {
                        // 解码心跳事件
                        data = decodeHeartbeatData(channel, in);
                    } else if (res.isEvent()) {
                        // 解码其它事件
                        data = decodeEventData(channel, in);
                    } else {
                        // 解码普通响应
                        DecodeableRpcResult result;
                        // 根据decode.in.io的配置决定何时进行解码
                        // 在通信框架（例如，Netty）的 IO 线程，解码
                        if (channel.getUrl().getParameter(
                                Constants.DECODE_IN_IO_THREAD_KEY,
                                Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                            result = new DecodeableRpcResult(channel, res, is,
                                    (Invocation) getRequestData(id), proto);
                            result.decode();
                        } else {
                            // 在 Dubbo ThreadPool 线程，解码，使用 DecodeHandler
                            result = new DecodeableRpcResult(channel, res,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    // 设置结果
                    res.setResult(data);
                } else {
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode response failed: " + t.getMessage(), t);
                }
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request. 服务端对请求进行解码
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            // 是否需要响应
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            // 若是心跳事件，进行设置
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                Object data;
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                // 解码心跳事件
                if (req.isHeartbeat()) {
                    data = decodeHeartbeatData(channel, in);
                } else if (req.isEvent()) {
                    // 解码其它事件
                    data = decodeEventData(channel, in);
                } else {
                    DecodeableRpcInvocation inv;
                    // 在通信框架（例如，Netty）的 IO 线程，解码
                    if (channel.getUrl().getParameter(
                            Constants.DECODE_IN_IO_THREAD_KEY,
                            Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                        inv = new DecodeableRpcInvocation(channel, req, is, proto);
                        inv.decode();
                    } else {
                        // 在 Dubbo ThreadPool 线程，解码，使用 DecodeHandler
                        inv = new DecodeableRpcInvocation(channel, req,
                                new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                req.setData(data);
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // bad request
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 编码 RpcInvocation 对象，写入需要编码的字段。对应的解码，在 DecodeableRpcInvocation 中。
     * 在对请求进行编码时，会把版本号、服务路径、方法名、方法参数等信息都进行编码，其中参数类型是用JVM中的类型表示方法来编码的。
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        // 写入 `dubbo` `path` `version`
        out.writeUTF(version);
        out.writeUTF(inv.getAttachment(Constants.PATH_KEY));
        out.writeUTF(inv.getAttachment(Constants.VERSION_KEY));

        // 写入方法、方法签名、方法参数集合
        out.writeUTF(inv.getMethodName());
        out.writeUTF(ReflectUtils.getDesc(inv.getParameterTypes()));
        Object[] args = inv.getArguments();
        if (args != null)
            for (int i = 0; i < args.length; i++) {
                // 调用 CallbackServiceCodec#encodeInvocationArgument(...) 方法，编码参数。主要用于 参数回调 功能。
                out.writeObject(encodeInvocationArgument(channel, inv, i));
            }
        // 写入隐式传参集合
        out.writeObject(inv.getAttachments());
    }

    /**
     * 编码 Result 对象，写入需要编码的字段。对应的解码，在 DecodeableRpcResult 中。
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        // 2.0.2之后版本的响应中，才会存在附加属性，这里会根据版本号来判断是否需要将附加属性进行编码
        boolean attach = Version.isSupportResponseAttatchment(version);
        Throwable th = result.getException();
        // 正常
        if (th == null) {
            Object ret = result.getValue();
            // 空返回
            if (ret == null) {
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                // 有返回
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            // 异常
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeObject(th);
        }

        if (attach) {
            // returns current version of Response to consumer side.
            // 将响应的当前版本返回给 consumer 端。
            result.getAttachments().put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeObject(result.getAttachments());
        }
    }
}
