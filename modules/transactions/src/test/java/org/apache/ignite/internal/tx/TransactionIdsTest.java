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

package org.apache.ignite.internal.tx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TransactionIdsTest {
    @ParameterizedTest
    @EnumSource(TxPriority.class)
    void transactionIdIsBuiltCorrectly(TxPriority priority) {
        HybridTimestamp beginTs = new HybridTimestamp(123L, 456);
        int retryCnt = 10;
        int nodeId = 1;

        UUID txId = TransactionIds.transactionId(beginTs, retryCnt, nodeId, priority);

        HybridTimestamp extractedTs = TransactionIds.beginTimestamp(txId);
        int extractedRetryCnt = TransactionIds.retryCnt(txId);
        int extractedNodeId = TransactionIds.nodeId(txId);
        TxPriority extractedPriority = TransactionIds.priority(txId);

        assertThat(extractedTs, is(beginTs));
        assertThat(extractedRetryCnt, is(retryCnt));
        assertThat(extractedNodeId, is(nodeId));
        assertThat(extractedPriority, is(priority));

        UUID txId2 = TransactionIds.transactionId(extractedTs, extractedRetryCnt, extractedNodeId, extractedPriority);

        assertThat(txId, is(txId2));
    }
}
