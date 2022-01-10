/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 *
 */
class UosObjectInputStream extends ObjectInputStream {
    private final DataInputStream input;
    private final ValueReader<Object> valueReader;
    private final DefaultFieldsReaderWriter defaultFieldsReaderWriter;
    private final UnmarshallingContext context;

    UosObjectInputStream(DataInputStream input, ValueReader<Object> valueReader, DefaultFieldsReaderWriter defaultFieldsReaderWriter, UnmarshallingContext context) throws IOException {
        this.input = input;
        this.valueReader = valueReader;
        this.defaultFieldsReaderWriter = defaultFieldsReaderWriter;
        this.context = context;
    }

    @Override
    public int read() throws IOException {
        return input.read();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int read(byte[] buf) throws IOException {
        return input.read(buf);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return input.read(buf, off, len);
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    @Override
    public void readFully(byte[] buf) throws IOException {
        input.readFully(buf);
    }

    @Override
    public void readFully(byte[] buf, int off, int len) throws IOException {
        input.readFully(buf, off, len);
    }

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    protected Object readObjectOverride() throws IOException {
        return doReadObject();
    }

    private Object doReadObject() throws IOException {
        try {
            return valueReader.read(input, context);
        } catch (UnmarshalException e) {
            // TODO: IGNITE-16165 -  pass exception correctly
            throw new RuntimeException("Cannot read", e);
        }
    }

    @Override
    public Object readUnshared() throws IOException {
        // TODO: IGNITE-16165 - implement 'unshared' logic?
        return doReadObject();
    }

    @Override
    public void defaultReadObject() throws IOException {
        try {
            defaultFieldsReaderWriter.defaultFillFieldsFrom(
                    input,
                    context.objectCurrentlyReadWithReadObject(),
                    context.descriptorOfObjectCurrentlyReadWithReadObject(),
                    context
            );
        } catch (UnmarshalException e) {
            // TODO: IGNITE-16165 -  pass exception correctly
            throw new RuntimeException("Cannot read", e);
        }
    }

    @Override
    public int available() throws IOException {
        return input.available();
    }

    @Override
    public int skipBytes(int len) throws IOException {
        return input.skipBytes(len);
    }

    @Override
    public void close() throws IOException {
    }
}
