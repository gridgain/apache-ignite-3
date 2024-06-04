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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;
import sun.nio.ch.SelChImpl;
import sun.nio.ch.SelectionKeyImpl;

/**
 * Wrapper around a {@link SocketChannel} which collects some metrics about it.
 */
class MeteredServerSocketChannel extends ServerSocketChannel implements SelChImpl {
    private final ServerSocketChannel channel;
    private final SelChImpl selCh;

    private final NetworkMetricSource metricSource;

    private static final Method implCloseSelectableChannelMethod;
    private static final Method implConfigureBlockingMethod;

    static {
        try {
            implCloseSelectableChannelMethod = AbstractSelectableChannel.class.getDeclaredMethod("implCloseSelectableChannel");
            implCloseSelectableChannelMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        try {
            implConfigureBlockingMethod = AbstractSelectableChannel.class.getDeclaredMethod("implConfigureBlocking", boolean.class);
            implConfigureBlockingMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    MeteredServerSocketChannel(SelectorProvider provider, ServerSocketChannel channel, NetworkMetricSource metricSource) {
        super(provider);

        this.channel = channel;
        selCh = (SelChImpl) channel;

        this.metricSource = metricSource;
    }

    @Override
    public ServerSocketChannel bind(SocketAddress local, int backlog) throws IOException {
        return channel.bind(local, backlog);
    }

    @Override
    public SocketChannel accept() throws IOException {
        SocketChannel socketChannel = channel.accept();
        if (socketChannel == null) {
            return socketChannel;
        }

        return new MeteredSocketChannel(provider(), socketChannel, metricSource);
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return channel.getOption(name);
    }

    @Override
    public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return channel.setOption(name, value);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return channel.supportedOptions();
    }

    @Override
    public ServerSocket socket() {
        return channel.socket();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return channel.getLocalAddress();
    }

    @Override
    protected void implCloseSelectableChannel() {
        try {
            implCloseSelectableChannelMethod.invoke(channel);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void implConfigureBlocking(boolean block) {
        try {
            implConfigureBlockingMethod.invoke(channel, block);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileDescriptor getFD() {
        return selCh.getFD();
    }

    @Override
    public int getFDVal() {
        return selCh.getFDVal();
    }

    @Override
    public boolean translateAndUpdateReadyOps(int ops, SelectionKeyImpl ski) {
        return selCh.translateAndUpdateReadyOps(ops, ski);
    }

    @Override
    public boolean translateAndSetReadyOps(int ops, SelectionKeyImpl ski) {
        return selCh.translateAndSetReadyOps(ops, ski);
    }

    @Override
    public int translateInterestOps(int ops) {
        return selCh.translateInterestOps(ops);
    }

    @Override
    public void kill() throws IOException {
        selCh.kill();
    }
}
