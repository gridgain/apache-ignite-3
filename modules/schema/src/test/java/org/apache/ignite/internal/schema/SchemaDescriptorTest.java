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

package org.apache.ignite.internal.schema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class SchemaDescriptorTest {
    /**
     *
     */
    @Test
    public void testColumnIndexedAccess() {
        SchemaDescriptor desc = new SchemaDescriptor(1,
            new Column[] {
                new Column("columnA", NativeType.INT8, false),
                new Column("columnB", NativeType.UUID, false),
                new Column("columnC", NativeType.INT32, false),
            },
            new Column[] {
                new Column("columnD", NativeType.INT8, false),
                new Column("columnE", NativeType.UUID, false),
                new Column("columnF", NativeType.INT32, false),
            }
        );

        assertEquals(6, desc.length());

        for (int i = 0; i < desc.length(); i++)
            assertEquals(i, desc.column(i).schemaIndex());
    }
}
