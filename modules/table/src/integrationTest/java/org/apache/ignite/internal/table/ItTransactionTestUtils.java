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

package org.apache.ignite.internal.table;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransaction;
import static org.apache.ignite.internal.TestWrappers.unwrapRecordBinaryViewImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

public class ItTransactionTestUtils {
    public static Set<String> partitionAssignment(IgniteImpl node, TablePartitionId grpId) {
        MetaStorageManager metaStorageManager = node.metaStorageManager();

        ByteArray stableAssignmentKey = stablePartAssignmentsKey(grpId);

        CompletableFuture<Entry> assignmentEntryFut = metaStorageManager.get(stableAssignmentKey);

        assertThat(assignmentEntryFut, willCompleteSuccessfully());

        Entry e = assignmentEntryFut.join();

        assertNotNull(e);
        assertFalse(e.empty());
        assertFalse(e.tombstone());

        Set<Assignment> a = requireNonNull(Assignments.fromBytes(e.value())).nodes();

        return a.stream().filter(Assignment::isPeer).map(Assignment::consistentId).collect(toSet());
    }

    public static int partitionIdForTuple(IgniteImpl node, String tableName, Tuple tuple, @Nullable Transaction tx) {
        TableImpl table = table(node, tableName);
        RecordBinaryViewImpl view = unwrapRecordBinaryViewImpl(table.recordView());

        CompletableFuture<BinaryRowEx> rowFut = view.marshal(tx, tuple);
        assertThat(rowFut, willCompleteSuccessfully());
        BinaryRowEx row = rowFut.join();

        return table.internalTable().partitionId(row);
    }

    public static Tuple findTupleToBeHostedOnNode(
            IgniteImpl node,
            String tableName,
            @Nullable Transaction tx,
            Tuple initialTuple,
            Function<Tuple, Tuple> nextTuple,
            boolean primary
    ) {
        Tuple t = initialTuple;
        int tableId = tableId(node, tableName);

        int maxAttempts = 100;

        while (maxAttempts >= 0) {
            int partId = partitionIdForTuple(node, tableName, t, tx);

            TablePartitionId grpId = new TablePartitionId(tableId, partId);

            if (primary) {
                ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, grpId);

                if (node.id().equals(replicaMeta.getLeaseholderId())) {
                    return t;
                }
            } else {
                Set<String> assignments = partitionAssignment(node, grpId);

                if (assignments.contains(node.name())) {
                    return t;
                }
            }

            t = nextTuple.apply(t);

            maxAttempts--;
        }

        throw new AssertionError("Failed to find a suitable tuple.");
    }

    public static TableImpl table(IgniteImpl node, String tableName) {
        return unwrapTableImpl(node.tables().table(tableName));
    }

    public static int tableId(IgniteImpl node, String tableName) {
        return table(node, tableName).tableId();
    }

    public static UUID txId(Transaction tx) {
        return ((ReadWriteTransactionImpl) unwrapIgniteTransaction(tx)).id();
    }

    public static ReplicaMeta waitAndGetPrimaryReplica(IgniteImpl node, ReplicationGroupId tblReplicationGrp) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node.clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        return primaryReplicaFut.join();
    }
}
