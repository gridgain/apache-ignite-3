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

import io.netty.channel.ChannelFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * {@link ChannelFactory} producing {@link NioServerSocketChannel}s wrapping {@link ServerSocketChannel}s with attached metrics.
 */
class JavaSideMeteredServerChannelFactory implements ChannelFactory<ServerChannel> {
    private final SelectorProvider selectorProvider;

    public JavaSideMeteredServerChannelFactory(NetworkMetricSource metricSource) {
        selectorProvider = new MeteredSelectorProvider(metricSource);
    }

    @Override
    public ServerChannel newChannel() {
        return new NioServerSocketChannel(selectorProvider);
    }
}
