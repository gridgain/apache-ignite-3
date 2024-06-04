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

import static io.netty.channel.internal.ChannelUtils.MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

/**
 * {@link NioSocketChannel} that collects metrics about its operations.
 */
class MeteredNioSocketChannel extends NioSocketChannel {
    private final NetworkMetricSource metricSource;

    private static final Method getMaxBytesPerGatheringWriteMethod;
    private static final Method setMaxBytesPerGatheringWriteMethod;

    static {
        try {
            Class<?> configClass = Class.forName(NioSocketChannel.class.getName() + "$NioSocketChannelConfig");

            getMaxBytesPerGatheringWriteMethod = configClass.getDeclaredMethod("getMaxBytesPerGatheringWrite");
            getMaxBytesPerGatheringWriteMethod.setAccessible(true);

            setMaxBytesPerGatheringWriteMethod = configClass.getDeclaredMethod("setMaxBytesPerGatheringWrite", int.class);
            setMaxBytesPerGatheringWriteMethod.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    MeteredNioSocketChannel(SelectorProvider provider, NetworkMetricSource metricSource) {
        super(provider);

        this.metricSource = metricSource;
    }

    MeteredNioSocketChannel(Channel parent, SocketChannel socket, NetworkMetricSource metricSource) {
        super(parent, socket);

        this.metricSource = metricSource;
    }

    @Override
    protected int doWriteBytes(ByteBuf buf) throws Exception {
        metricSource.attemptedWrites().increment();
        metricSource.attemptedWriteSize().add(buf.readableBytes());
        metricSource.writeBuffers().add(1);

        int bytesWritten = super.doWriteBytes(buf);

        if (bytesWritten >= 0) {
            metricSource.performedWriteSize().add(bytesWritten);
        }

        return bytesWritten;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        SocketChannel ch = javaChannel();
        int writeSpinCount = config().getWriteSpinCount();
        do {
            if (in.isEmpty()) {
                // All written so clear OP_WRITE
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }

            // Ensure the pending writes are made of ByteBufs only.
            int maxBytesPerGatheringWrite = getMaxBytesPerGatheringWrite();
            ByteBuffer[] nioBuffers = in.nioBuffers(1024, maxBytesPerGatheringWrite);
            int nioBufferCnt = in.nioBufferCount();

            // Always use nioBuffers() to workaround data-corruption.
            // See https://github.com/netty/netty/issues/2761
            switch (nioBufferCnt) {
                case 0:
                    // We have something else beside ByteBuffers to write so fallback to normal writes.
                    writeSpinCount -= doWrite0(in);
                    break;
                case 1: {
                    // Only one ByteBuf so use non-gathering write
                    // Zero length buffers are not added to nioBuffers by ChannelOutboundBuffer, so there is no need
                    // to check if the total size of all the buffers is non-zero.
                    ByteBuffer buffer = nioBuffers[0];
                    int attemptedBytes = buffer.remaining();

                    metricSource.attemptedWriteSize().add(attemptedBytes);
                    metricSource.writeBuffers().add(1);
                    metricSource.attemptedWrites().increment();

                    final int localWrittenBytes = ch.write(buffer);

                    if (localWrittenBytes >= 0) {
                        metricSource.performedWriteSize().add(localWrittenBytes);
                    }

                    if (localWrittenBytes <= 0) {
                        incompleteWrite(true);
                        return;
                    }
                    adjustMaxBytesPerGatheringWrite(attemptedBytes, localWrittenBytes, maxBytesPerGatheringWrite);
                    in.removeBytes(localWrittenBytes);
                    --writeSpinCount;
                    break;
                }
                default: {
                    // Zero length buffers are not added to nioBuffers by ChannelOutboundBuffer, so there is no need
                    // to check if the total size of all the buffers is non-zero.
                    // We limit the max amount to int above so cast is safe
                    long attemptedBytes = in.nioBufferSize();

                    metricSource.attemptedWriteSize().add(attemptedBytes);
                    metricSource.writeBuffers().add(nioBufferCnt);
                    metricSource.attemptedWrites().increment();

                    final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);

                    if (localWrittenBytes >= 0) {
                        metricSource.performedWriteSize().add(localWrittenBytes);
                    }

                    if (localWrittenBytes <= 0) {
                        incompleteWrite(true);
                        return;
                    }
                    // Casting to int is safe because we limit the total amount of data in the nioBuffers to int above.
                    adjustMaxBytesPerGatheringWrite((int) attemptedBytes, (int) localWrittenBytes,
                            maxBytesPerGatheringWrite);
                    in.removeBytes(localWrittenBytes);
                    --writeSpinCount;
                    break;
                }
            }
        } while (writeSpinCount > 0);

        incompleteWrite(writeSpinCount < 0);
    }

    private void adjustMaxBytesPerGatheringWrite(int attempted, int written, int oldMaxBytesPerGatheringWrite) {
        // By default we track the SO_SNDBUF when ever it is explicitly set. However some OSes may dynamically change
        // SO_SNDBUF (and other characteristics that determine how much data can be written at once) so we should try
        // make a best effort to adjust as OS behavior changes.
        if (attempted == written) {
            if (attempted << 1 > oldMaxBytesPerGatheringWrite) {
                setMaxBytesPerGatheringWrite(attempted << 1);
            }
        } else if (attempted > MAX_BYTES_PER_GATHERING_WRITE_ATTEMPTED_LOW_THRESHOLD && written < attempted >>> 1) {
            setMaxBytesPerGatheringWrite(attempted >>> 1);
        }
    }

    private void setMaxBytesPerGatheringWrite(int maxBytesPerGatheringWrite) {
        try {
            setMaxBytesPerGatheringWriteMethod.invoke(config(), maxBytesPerGatheringWrite);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private int getMaxBytesPerGatheringWrite() {
        try {
            return (int) getMaxBytesPerGatheringWriteMethod.invoke(config());
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
