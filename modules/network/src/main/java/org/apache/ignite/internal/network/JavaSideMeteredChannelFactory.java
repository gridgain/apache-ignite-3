/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * {@link ChannelFactory} producing {@link NioSocketChannel}s wrapping {@link SocketChannel}s with attached metrics.
 */
class JavaSideMeteredChannelFactory implements ChannelFactory<Channel> {
    private final SelectorProvider selectorProvider;

    public JavaSideMeteredChannelFactory(NetworkMetricSource metricSource) {
        selectorProvider = new MeteredSelectorProvider(metricSource);
    }

    @Override
    public Channel newChannel() {
        return new NioSocketChannel(selectorProvider);
    }
}
