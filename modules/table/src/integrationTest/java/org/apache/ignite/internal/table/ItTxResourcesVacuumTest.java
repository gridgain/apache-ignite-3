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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.findTupleToBeHostedOnNode;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.partitionAssignment;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.partitionIdForTuple;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.tableId;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.table.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = "RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY", value = "1000")
public class ItTxResourcesVacuumTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    private static final Tuple INITIAL_TUPLE = Tuple.create().set("key", 1L).set("val", "1");

    private static final Function<Tuple, Tuple> NEXT_TUPLE = t -> Tuple.create()
            .set("key", t.longValue("key") + 1)
            .set("val", "" + (t.longValue("key") + 1));

    private static final int REPLICAS = 2;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {},\n"
            + "  raft: { responseTimeout: 30000 },"
            + "  compute.threadPoolSize: 1\n"
            + "}";

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=5, replicas=" + REPLICAS;
        String sql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("{"
                + "  transaction: {"
                + "      implicitTransactionTimeout: 30000,"
                + "      txnResourceTtl: 2"
                + "  },"
                + "  replication: {"
                + "      rpcTimeout: 30000"
                + "  },"
                + "}");
    }

    @Override
    protected int initialNodes() {
        return 3;
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    public void testVacuum() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int partId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        Set<String> nodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), partId));

        view.upsert(tx, tuple);

        tx.commit();

        checkVolatileTxStateOnNodes(nodes, txId);
        waitForTxStateReplication(nodes, txId, partId, 10_000);

        waitForTxStateVacuum(txId, 0, true, 10_000);
    }

    @Test
    public void testVacuumWithCleanupDelay() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple0 = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple0, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(0, initialNodes(), n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        // Some node that does not host the commit partition, will be the primary node for upserting another tuple.
        IgniteImpl leaseholderForAnotherTuple = findNode(0, initialNodes(), n -> !commitPartNodes.contains(n.name()));

        log.info("Test: leaseholderForAnotherTuple={}", leaseholderForAnotherTuple.name());

        Tuple tuple1 = findTupleToBeHostedOnNode(leaseholderForAnotherTuple, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        view.upsert(tx, tuple0);
        view.upsert(tx, tuple1);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        CompletableFuture<Void> cleanupAllowed = new CompletableFuture<>();

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                cleanupStarted.complete(null);

                if (commitPartNodes.contains(n)) {
                    cleanupAllowed.join();
                }
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        checkVolatileTxStateOnNodes(commitPartNodes, txId);
        waitForTxStateReplication(commitPartNodes, txId, commitPartId, 10_000);

        assertThat(cleanupStarted, willCompleteSuccessfully());

        waitForTxStateVacuum(Set.of(leaseholderForAnotherTuple.name()), txId, 0, false, 10_000);

        checkPersistentTxStateOnNodes(commitPartNodes, txId, commitPartId);

        cleanupAllowed.complete(null);

        assertThat(commitFut, willCompleteSuccessfully());

        waitForTxStateVacuum(txId, 0, true, 10_000);
    }

    @Test
    public void testCommitPartitionPrimaryChangesBeforeVacuum() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(0, initialNodes(), n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        view.upsert(tx, tuple);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        CompletableFuture<Void> cleanupAllowedFut = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage && !cleanupAllowed[0]) {
                cleanupStarted.complete(null);

                cleanupAllowedFut.join();

                return true;
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        //transferPrimary

        waitAndGetPrimaryReplica(node, commitPartGrpId);

        cleanupAllowedFut.complete(null);

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        waitForTxStateVacuum(txId, 0, true, 10_000);
    }

    @Test
    public void testVacuumPersistentStateAfterCleanupDelayAndVolatileStateVacuum() throws InterruptedException {
        IgniteImpl node = anyNode();

        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        log.info("Test: Loading the data [tx={}].", txId);

        Tuple tuple = findTupleToBeHostedOnNode(node, TABLE_NAME, tx, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(node, TABLE_NAME, tuple, tx);

        TablePartitionId commitPartGrpId = new TablePartitionId(tableId(node, TABLE_NAME), commitPartId);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, commitPartGrpId);
        IgniteImpl commitPartitionLeaseholder = findNode(0, initialNodes(), n -> n.id().equals(replicaMeta.getLeaseholderId()));

        Set<String> commitPartNodes = partitionAssignment(node, new TablePartitionId(tableId(node, TABLE_NAME), commitPartId));

        log.info("Test: Commit partition [leaseholder={}, hostingNodes={}].", commitPartitionLeaseholder.name(), commitPartNodes);

        view.upsert(tx, tuple);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        CompletableFuture<Void> cleanupAllowedFut = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage && !cleanupAllowed[0]) {
                cleanupStarted.complete(null);

                cleanupAllowedFut.join();

                return true;
            }

            return false;
        });

        CompletableFuture<Void> commitFut = tx.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        //transferPrimary

        waitAndGetPrimaryReplica(node, commitPartGrpId);

        cleanupAllowedFut.complete(null);

        cleanupAllowed[0] = true;

        assertThat(commitFut, willCompleteSuccessfully());

        waitForTxStateVacuum(txId, 0, true, 10_000);
    }

    @Test
    public void testRecoveryAfterPersistentStateVacuumized() throws InterruptedException {
        IgniteImpl coord0 = anyNode();

        RecordView<Tuple> view0 = coord0.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction tx0 = coord0.transactions().begin();
        UUID txId0 = txId(tx0);

        log.info("Test: Transaction 0 [tx={}].", txId0);

        // Find some node other than coordinator.
        IgniteImpl commitPartitionLeaseholder = findNode(0, initialNodes(), n -> !n.name().equals(coord0.name()));

        Tuple tuple0 = findTupleToBeHostedOnNode(commitPartitionLeaseholder, TABLE_NAME, tx0, INITIAL_TUPLE, NEXT_TUPLE, true);

        int commitPartId = partitionIdForTuple(commitPartitionLeaseholder, TABLE_NAME, tuple0, tx0);

        Set<String> nodes = partitionAssignment(coord0, new TablePartitionId(tableId(coord0, TABLE_NAME), commitPartId));

        view0.upsert(tx0, tuple0);

        CompletableFuture<Void> cleanupStarted = new CompletableFuture<>();
        boolean[] cleanupAllowed = new boolean[1];

        commitPartitionLeaseholder.dropMessages((n, msg) -> {
            if (msg instanceof TxCleanupMessage) {
                cleanupStarted.complete(null);

                return cleanupAllowed[0];
            }

            return false;
        });

        tx0.commitAsync();

        assertThat(cleanupStarted, willCompleteSuccessfully());

        // Check that the final tx state COMMITTED is saved to the persistent tx storage.
        assertTrue(waitForCondition(() -> cluster.runningNodes().filter(n -> nodes.contains(n.name())).allMatch(n -> {
                TransactionMeta meta = persistentTxState(n, txId0, commitPartId);

                return meta != null && meta.txState() == COMMITTED;
        }), 10_000));

        // Stop the first transaction coordinator.
        stopNode(coord0.name());

        // No cleanup happened, waiting for vacuum on the remaining nodes that participated on tx0.
        waitForTxStateVacuum(txId0, commitPartId, true, 10_000);

        // Preparing to run another tx.
        IgniteImpl coord1 = anyNode();

        RecordView<Tuple> view1 = coord1.tables().table(TABLE_NAME).recordView();

        Transaction tx1 = coord1.transactions().begin();
        UUID txId1 = txId(tx1);

        log.info("Test: Transaction 1 [tx={}].", txId1);

        IgniteImpl anyNodeContainingWriteIntent = findNode(0, initialNodes(), n -> nodes.contains(n.name()));

        Tuple tuple1 = findTupleToBeHostedOnNode(anyNodeContainingWriteIntent, TABLE_NAME, tx1, INITIAL_TUPLE, NEXT_TUPLE, true);

        // Tx 1 should get the data committed by tx 0.
        Tuple tx0Data = view1.get(tx1, tuple1);
        assertEquals(tuple0.longValue("key"), tx0Data.longValue("key"));

        tx1.commit();

        waitForTxStateVacuum(txId0, 0, true, 10_000);
        waitForTxStateVacuum(txId0, 0, true, 10_000);
    }

    private boolean checkVolatileTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId) {
        return cluster.runningNodes()
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .allMatch(n -> volatileTxState(n, txId) != null);
    }

    private boolean checkPersistentTxStateOnNodes(Set<String> nodeConsistentIds, UUID txId, int partId) {
        return cluster.runningNodes()
                .filter(n -> nodeConsistentIds.contains(n.name()))
                .allMatch(n -> persistentTxState(n, txId, partId) != null);
    }

    private void waitForTxStateReplication(Set<String> nodeConsistentIds, UUID txId, int partId, long timeMs)
            throws InterruptedException {
        assertTrue(waitForCondition(() -> checkPersistentTxStateOnNodes(nodeConsistentIds, txId, partId), timeMs));
    }

    private void waitForTxStateVacuum(UUID txId, int partId, boolean checkPersistent, long timeMs) throws InterruptedException {
        waitForTxStateVacuum(cluster.runningNodes().map(IgniteImpl::name).collect(toSet()), txId, partId, checkPersistent, timeMs);
    }

    private void waitForTxStateVacuum(Set<String> nodeConsistentIds, UUID txId, int partId, boolean checkPersistent, long timeMs) throws InterruptedException {
        boolean r = waitForCondition(() -> {
            boolean result = true;

            for (Iterator<IgniteImpl> iterator = cluster.runningNodes().iterator(); iterator.hasNext();) {
                IgniteImpl node = iterator.next();

                if (!nodeConsistentIds.contains(node.name())) {
                    continue;
                }

                result = result
                        && volatileTxState(node, txId) == null && (!checkPersistent || persistentTxState(node, txId, partId) == null);
            }

            return result;
        }, timeMs);

        if (!r) {
            cluster.runningNodes().forEach(node -> {
                log.info("Test: volatile   state [tx={}, node={}, state={}].", txId, node.name(), volatileTxState(node, txId));
                log.info("Test: persistent state [tx={}, node={}, state={}].", txId, node.name(), persistentTxState(node, txId, partId));
            });
        }

        assertTrue(r);
    }

    private IgniteImpl anyNode() {
        return runningNodes().findFirst().orElseThrow();
    }

    @Nullable
    private static TransactionMeta volatileTxState(IgniteImpl node, UUID txId) {
        TxManagerImpl txManager = (TxManagerImpl) node.txManager();
        return txManager.stateMeta(txId);
    }

    @Nullable
    private static TransactionMeta persistentTxState(IgniteImpl node, UUID txId, int partId) {
        TxStateStorage txStateStorage = table(node, TABLE_NAME).internalTable().txStateStorage().getTxStateStorage(partId);

        assertNotNull(txStateStorage);

        return txStateStorage.get(txId);
    }

    private IgniteImpl findNode(int startRange, int endRange, Predicate<IgniteImpl> filter) {
        return IntStream.range(startRange, endRange)
                .mapToObj(this::node)
                .filter(n -> n != null && filter.test(n))
                .findFirst()
                .get();
    }
}
