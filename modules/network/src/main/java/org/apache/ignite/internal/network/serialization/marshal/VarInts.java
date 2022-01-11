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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
class VarInts {
    private VarInts() {
    }

    static void writeUnsignedInt(int value, DataOutput output) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException(value + " is negative" );
        }

        if (value < 0xFF) {
            output.writeByte(value);
        } else if (value < 0xFFFF) {
            output.writeByte(0xFF);
            output.writeShort(value);
        } else {
            output.writeByte(0xFF);
            output.writeShort(0xFFFF);
            output.writeInt(value);
        }
    }

    static int readUnsignedInt(DataInput input) throws IOException {
        int first = input.readUnsignedByte();
        if (first < 0xFF) {
            return first;
        }

        int second = input.readUnsignedShort();
        if (second < 0xFFFF) {
            return second;
        }

        int third = input.readInt();
        if (third < 0) {
            throw new IllegalStateException(third + " is negative");
        }

        return third;
    }
}
