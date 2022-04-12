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
import java.io.IOException;
import java.util.BitSet;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Tools to read nulls bitset.
 *
 * @see NullsBitsetWriter
 */
class NullsBitsetReader {
    @Nullable
    static BitSet readNullsBitSet(DataInput input, ClassDescriptor descriptor) throws IOException {
        if (descriptor.fieldIndexInNullsBitmapSize() == 0) {
            return null;
        }

        return ProtocolMarshalling.readFixedLengthBitSet(descriptor.fieldIndexInNullsBitmapSize(), input);
    }

    private NullsBitsetReader() {
        // prevent instantiation
    }
}
