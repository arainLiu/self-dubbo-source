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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.Resetable;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.transport.codec.CodecAdapter;
import org.apache.dubbo.rpc.model.FrameworkModel;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.rpc.model.ScopeModelUtil.getFrameworkModel;

public abstract class AbstractEndpoint extends AbstractPeer implements Resetable {

    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private Codec2 codec;

    private int connectTimeout;

    public AbstractEndpoint(URL url, ChannelHandler handler) {
        super(url, handler);
        this.codec = getChannelCodec(url);
        this.connectTimeout =
                url.getPositiveParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT);
    }

    protected AbstractEndpoint() {}

    /**
     * 根据URL配置获取对应的编解码器实现，支持Codec2和Codec两种扩展类型。
     * <p>
     * 该方法的查找逻辑：
     * 1. 从URL中获取codec参数，如果未配置则使用protocol名称作为编解码器名称；
     * 2. 优先查找Codec2类型的扩展实现（新版编解码器接口）；
     * 3. 如果Codec2不存在，则查找Codec类型的扩展实现并通过CodecAdapter适配为Codec2；
     * 4. 如果都不存在，则返回默认的default编解码器。
     * </p>
     *
     * @param url 配置URL，包含codec参数和protocol信息，用于确定使用哪种编解码器
     * @return Codec2 编解码器实例，可能是直接的Codec2实现或经过适配器包装的Codec实现
     */
    protected static Codec2 getChannelCodec(URL url) {
        // 从URL中获取编解码器名称，如果未配置则使用协议名称（保证编解码器扩展名与协议名一致）
        String codecName = url.getParameter(Constants.CODEC_KEY);
        if (StringUtils.isEmpty(codecName)) {
            // codec extension name must stay the same with protocol name
            codecName = url.getProtocol();
        }

        // 获取框架模型，依次尝试查找Codec2扩展、Codec扩展（需适配）、默认编解码器
        FrameworkModel frameworkModel = getFrameworkModel(url.getScopeModel());
        if (frameworkModel.getExtensionLoader(Codec2.class).hasExtension(codecName)) {
            // 优先使用Codec2类型的扩展实现（新版编解码器接口）
            return frameworkModel.getExtensionLoader(Codec2.class).getExtension(codecName);
        } else if (frameworkModel.getExtensionLoader(Codec.class).hasExtension(codecName)) {
            // 如果Codec2不存在，使用Codec扩展并通过CodecAdapter适配为Codec2接口
            return new CodecAdapter(
                    frameworkModel.getExtensionLoader(Codec.class).getExtension(codecName));
        } else {
            // 兜底策略：返回默认的default编解码器实现
            return frameworkModel.getExtensionLoader(Codec2.class).getExtension("default");
        }
    }

    @Override
    public void reset(URL url) {
        if (isClosed()) {
            throw new IllegalStateException(
                    "Failed to reset parameters " + url + ", cause: Channel closed. channel: " + getLocalAddress());
        }

        try {
            if (url.hasParameter(Constants.CONNECT_TIMEOUT_KEY)) {
                int t = url.getParameter(Constants.CONNECT_TIMEOUT_KEY, 0);
                if (t > 0) {
                    this.connectTimeout = t;
                }
            }
        } catch (Throwable t) {
            logger.error(INTERNAL_ERROR, "", "", t.getMessage(), t);
        }

        try {
            if (url.hasParameter(Constants.CODEC_KEY)) {
                this.codec = getChannelCodec(url);
            }
        } catch (Throwable t) {
            logger.error(INTERNAL_ERROR, "unknown error in remoting module", "", t.getMessage(), t);
        }
    }

    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    protected Codec2 getCodec() {
        return codec;
    }

    protected int getConnectTimeout() {
        return connectTimeout;
    }
}
