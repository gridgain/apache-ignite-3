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

package org.apache.ignite.internal.network.netty;

import static io.opentelemetry.api.GlobalOpenTelemetry.getPropagators;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedInput;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.message.ClassDescriptorListMessage;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.TraceableMessage;
import org.apache.ignite.internal.network.message.TraceableMessageImpl;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * An encoder for the outbound messages that uses {@link DirectMessageWriter}.
 */
public class OutboundEncoder extends MessageToMessageEncoder<OutNetworkObject> {
    /** Handler name. */
    public static final String NAME = "outbound-encoder";

    private static final NetworkMessagesFactory MSG_FACTORY = new NetworkMessagesFactory();

    /** Serialization registry. */
    private final PerSessionSerializationService serializationService;

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public OutboundEncoder(PerSessionSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    /** {@inheritDoc} */
    @Override
    protected void encode(ChannelHandlerContext ctx, OutNetworkObject msg, List<Object> out) throws Exception {
        if (!Span.current().getSpanContext().isValid()) {
            out.add(new NetworkMessageChunkedInput(msg, null, serializationService));
            return;
        }

        var propagator = getPropagators().getTextMapPropagator();
        Map<String, String> headers = new HashMap<>(capacity(propagator.fields().size()));

        propagator.inject(Context.current(), headers, (carrier, key, val) -> carrier.put(key, val));

        out.add(new NetworkMessageChunkedInput(msg, TraceableMessageImpl.builder().headers(headers).build(), serializationService));
    }

    /**
     * Chunked input for network message.
     */
    private static class NetworkMessageChunkedInput implements ChunkedInput<ByteBuf> {
        /** Network message. */
        private final NetworkMessage msg;

        /** Traceable message. */
        private final TraceableMessage traceableMsg;

        /** Message serializer. */
        private final MessageSerializer<NetworkMessage> serializer;

        private final MessageSerializer<ClassDescriptorListMessage> descriptorSerializer;

        /** Trace message serializer. */
        private final MessageSerializer<TraceableMessage> traceSerializer;

        /** Message writer. */
        private final DirectMessageWriter writer;

        private final ClassDescriptorListMessage descriptors;
        private final PerSessionSerializationService serializationService;

        /** Whether the message was fully written. */
        private boolean finished = false;
        private boolean descriptorsFinished = false;

        /** Whether the traceable message was fully written. */
        private boolean traceableFinished;

        /**
         * Constructor.
         *
         * @param outObject            Out network object.
         * @param traceableMsg         Traceable object.
         * @param serializationService Serialization service.
         */
        private NetworkMessageChunkedInput(
                OutNetworkObject outObject,
                @Nullable TraceableMessage traceableMsg,
                PerSessionSerializationService serializationService
        ) {
            this.msg = outObject.networkMessage();
            this.traceableMsg = traceableMsg;
            this.serializationService = serializationService;
            this.traceableFinished = traceableMsg == null;
            this.traceSerializer = traceableMsg == null
                    ? null : serializationService.createMessageSerializer(traceableMsg.groupType(), traceableMsg.messageType());

            List<ClassDescriptorMessage> outDescriptors = outObject.descriptors().stream()
                    .filter(classDescriptorMessage -> !serializationService.isDescriptorSent(classDescriptorMessage.descriptorId()))
                    .collect(Collectors.toList());

            if (!outDescriptors.isEmpty()) {
                this.descriptors = MSG_FACTORY.classDescriptorListMessage().messages(outDescriptors).build();
                short groupType = this.descriptors.groupType();
                short messageType = this.descriptors.messageType();
                descriptorSerializer = serializationService.createMessageSerializer(groupType, messageType);
            } else {
                descriptors = null;
                descriptorSerializer = null;
                descriptorsFinished = true;
            }

            this.serializer = serializationService.createMessageSerializer(msg.groupType(), msg.messageType());
            this.writer = new DirectMessageWriter(serializationService.serializationRegistry(), ConnectionManager.DIRECT_PROTOCOL_VERSION);
        }

        /** {@inheritDoc} */
        @Override
        public boolean isEndOfInput() throws Exception {
            return finished;
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {

        }

        /** {@inheritDoc} */
        @Deprecated
        @Override
        public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
            return readChunk(ctx.alloc());
        }

        /** {@inheritDoc} */
        @Override
        public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            ByteBuf buffer = allocator.ioBuffer();
            int capacity = buffer.capacity();

            ByteBuffer byteBuffer = buffer.internalNioBuffer(0, capacity);

            int initialPosition = byteBuffer.position();

            writer.setBuffer(byteBuffer);

            while (byteBuffer.hasRemaining()) {
                if (!traceableFinished) {
                    traceableFinished = traceSerializer.writeMessage(traceableMsg, writer);
                    if (traceableFinished) {
                        writer.reset();
                    } else {
                        break;
                    }
                } else if (!descriptorsFinished) {
                    descriptorsFinished = descriptorSerializer.writeMessage(descriptors, writer);
                    if (descriptorsFinished) {
                        for (ClassDescriptorMessage classDescriptorMessage : descriptors.messages()) {
                            serializationService.addSentDescriptor(classDescriptorMessage.descriptorId());
                        }
                        writer.reset();
                    } else {
                        break;
                    }
                } else {
                    finished = serializer.writeMessage(msg, writer);
                    break;
                }
            }

            buffer.writerIndex(byteBuffer.position() - initialPosition);

            return buffer;
        }

        /** {@inheritDoc} */
        @Override
        public long length() {
            // Return negative values, because object's size is unknown.
            return -1;
        }

        /** {@inheritDoc} */
        @Override
        public long progress() {
            // Not really needed, as there won't be listeners for the write operation's progress.
            return 0;
        }
    }
}
