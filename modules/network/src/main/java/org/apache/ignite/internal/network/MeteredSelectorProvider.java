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

import java.io.IOException;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

/**
 * Wrapper around {@link SelectorProvider} that adds metrics to {@link SocketChannel}s produced by the wrapped provider.
 */
class MeteredSelectorProvider extends SelectorProvider {
    private final SelectorProvider selectorProvider = SelectorProvider.provider();

    private final NetworkMetricSource metricSource;

    public MeteredSelectorProvider(NetworkMetricSource metricSource) {
        this.metricSource = metricSource;
    }

    @Override
    public DatagramChannel openDatagramChannel() throws IOException {
        return selectorProvider.openDatagramChannel();
    }

    @Override
    public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
        return selectorProvider.openDatagramChannel(family);
    }

    @Override
    public Pipe openPipe() throws IOException {
        return selectorProvider.openPipe();
    }

    @Override
    public AbstractSelector openSelector() throws IOException {
        return selectorProvider.openSelector();
    }

    @Override
    public ServerSocketChannel openServerSocketChannel() throws IOException {
        return new MeteredServerSocketChannel(selectorProvider, selectorProvider.openServerSocketChannel(), metricSource);
    }

    @Override
    public SocketChannel openSocketChannel() throws IOException {
        return new MeteredSocketChannel(selectorProvider, selectorProvider.openSocketChannel(), metricSource);
    }
}
