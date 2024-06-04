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
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;
import sun.nio.ch.SelChImpl;
import sun.nio.ch.SelectionKeyImpl;

/**
 * Wrapper around a {@link SocketChannel} which collects some metrics about it.
 */
class MeteredSocketChannel extends SocketChannel implements SelChImpl {
    private final SocketChannel channel;
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

    MeteredSocketChannel(SelectorProvider provider, SocketChannel channel, NetworkMetricSource metricSource) {
        super(provider);

        this.channel = channel;
        selCh = (SelChImpl) channel;

        this.metricSource = metricSource;
    }

    @Override
    public SocketChannel bind(SocketAddress local) throws IOException {
        return channel.bind(local);
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return channel.setOption(name, value);
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return channel.getOption(name);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return channel.supportedOptions();
    }

    @Override
    public SocketChannel shutdownInput() throws IOException {
        return channel.shutdownInput();
    }

    @Override
    public SocketChannel shutdownOutput() throws IOException {
        return channel.shutdownOutput();
    }

    @Override
    public Socket socket() {
        return channel.socket();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public boolean isConnectionPending() {
        return channel.isConnectionPending();
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        return channel.connect(remote);
    }

    @Override
    public boolean finishConnect() throws IOException {
        return channel.finishConnect();
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return channel.getRemoteAddress();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return channel.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        metricSource.attemptedWriteSize().add(src.remaining());
        metricSource.writeBuffers().add(1);
        metricSource.attemptedWrites().increment();

        int written = channel.write(src);

        if (written >= 0) {
            metricSource.performedWriteSize().add(written);
        }

        return written;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long writeSize = 0;
        for (int i = 0; i < length; i++) {
            writeSize += srcs[offset + i].remaining();
        }

        metricSource.attemptedWriteSize().add(writeSize);
        metricSource.writeBuffers().add(length);
        metricSource.attemptedWrites().increment();

        long written = channel.write(srcs, offset, length);

        if (written >= 0) {
            metricSource.performedWriteSize().add(written);
        }

        return written;
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
