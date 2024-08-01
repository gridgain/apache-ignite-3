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

package org.apache.ignite.internal.table.distributed.replicator.action;


import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE_IF_EXIST;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.junit.jupiter.api.Test;

class RequestTypeTest {
    @Test
    void isRwReadWorksAsExpected() {
        Set<RequestType> expectedRwReads = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN);

        for (RequestType requestType : RequestType.values()) {
            if (expectedRwReads.contains(requestType)) {
                assertTrue(requestType.isRwRead(), requestType + " must be an RW read");
            } else {
                assertFalse(requestType.isRwRead(), requestType + " must not be an RW read");
            }
        }
    }

    @Test
    void isWriteWorksAsExpected() {
        Set<RequestType> expectedNonWrites = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN, RO_GET, RO_GET_ALL, RO_SCAN);

        for (RequestType requestType : RequestType.values()) {
            if (expectedNonWrites.contains(requestType)) {
                assertFalse(requestType.isWrite(), requestType + " must not be a write");
            } else {
                assertTrue(requestType.isWrite(), requestType + " must be a write");
            }
        }
    }

    /** Checks that the transferable ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @Test
    void testTransferableId() {
        assertEquals(0, RW_GET.transferableId());
        assertEquals(1, RW_GET_ALL.transferableId());
        assertEquals(2, RW_DELETE.transferableId());
        assertEquals(3, RW_DELETE_ALL.transferableId());
        assertEquals(4, RW_DELETE_EXACT.transferableId());
        assertEquals(5, RW_DELETE_EXACT_ALL.transferableId());
        assertEquals(6, RW_INSERT.transferableId());
        assertEquals(7, RW_INSERT_ALL.transferableId());
        assertEquals(8, RW_UPSERT.transferableId());
        assertEquals(9, RW_UPSERT_ALL.transferableId());
        assertEquals(10, RW_REPLACE.transferableId());
        assertEquals(11, RW_REPLACE_IF_EXIST.transferableId());
        assertEquals(12, RW_GET_AND_DELETE.transferableId());
        assertEquals(13, RW_GET_AND_REPLACE.transferableId());
        assertEquals(14, RW_GET_AND_UPSERT.transferableId());
        assertEquals(15, RW_SCAN.transferableId());
        assertEquals(16, RO_GET.transferableId());
        assertEquals(17, RO_GET_ALL.transferableId());
        assertEquals(18, RO_SCAN.transferableId());
    }
}
