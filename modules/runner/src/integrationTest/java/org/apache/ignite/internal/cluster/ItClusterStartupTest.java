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

package org.apache.ignite.internal.cluster;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Test node start/stop in different scenarios and validate grid components behavior depending on availability/absence of quorums.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItClusterStartupTest extends AbstractClusterStartStopTest {
    @BeforeEach
    public void before() throws Exception {
        for (String name : nodesCfg.keySet()) {
            IgniteUtils.deleteIfExists(WORK_DIR.resolve(name));
        }

        initGrid(nodeAliasToNameMapping.values());

        // Shutdown cluster.
        stopAllNodes();
    }

    /** Runs after each test sequence. */
    @AfterEach
    public void afterEach() {
        stopAllNodes();

        for (String name : nodesCfg.keySet()) {
            IgniteUtils.deleteIfExists(WORK_DIR.resolve(name));
        }
    }

    /**
     * Generate node start sequences.
     *
     * @return Test parameters.
     */
    static Object[] generateParameters() {
        return new SequenceGenerator(
                nodeAliasToNameMapping.keySet(),
                (name, grid) -> (!grid.isEmpty() || "C".equals(name)) // CMG node always starts first.
                        && (!"D2".equals(name) || grid.contains("D")),  // Data nodes are interchangeable.
                grid -> grid.size() == nodeAliasToNameMapping.size()
        ).generate().toArray(Object[]::new);
    }

    /** Checks new node joining to the grid. */
    @ParameterizedTest(name = "Node order=" + ParameterizedTest.ARGUMENTS_PLACEHOLDER)
    @MethodSource("generateParameters")
    public void testNodeJoin(List<String> nodeAliases) {
        for (String alias : nodeAliases) {
            log.info("Starting node: alias=" + alias + ", name=" + resolve(alias));

            startNode(resolve(alias));

            checkNodeJoin();
        }
    }

    /** Checks table creation. */
    @ParameterizedTest(name = "Node order=" + ParameterizedTest.ARGUMENTS_PLACEHOLDER)
    @MethodSource("generateParameters")
    public void testCreateTable(List<String> nodeAliases) {
        for (String alias : nodeAliases) {
            log.info("Starting node: alias=" + alias + ", name=" + resolve(alias));

            startNode(resolve(alias));

            checkCreateTable();
        }
    }

    /** Checks implicit transaction. */
    @ParameterizedTest(name = "Node order=" + ParameterizedTest.ARGUMENTS_PLACEHOLDER)
    @MethodSource("generateParameters")
    public void testImplicitTransaction(List<String> nodeAliases) {
        for (String alias : nodeAliases) {
            log.info("Starting node: alias=" + alias + ", name=" + resolve(alias));

            startNode(resolve(alias));

            checkImplicitTx();
        }
    }

    /** Checks read-write transaction. */
    @ParameterizedTest(name = "Node order=" + ParameterizedTest.ARGUMENTS_PLACEHOLDER)
    @MethodSource("generateParameters")
    public void testReadWriteTransaction(List<String> nodeAliases) {
        for (String alias : nodeAliases) {
            log.info("Starting node: alias=" + alias + ", name=" + resolve(alias));

            startNode(resolve(alias));

            checkTxRW();
        }
    }

    /** Checks read-only transaction. */
    @ParameterizedTest(name = "Node order=" + ParameterizedTest.ARGUMENTS_PLACEHOLDER)
    @MethodSource("generateParameters")
    public void testReadOnlyTransaction(List<String> nodeAliases) {
        for (String alias : nodeAliases) {
            log.info("Starting node: alias=" + alias + ", name=" + resolve(alias));

            startNode(resolve(alias));

            checkTxRO();
        }
    }

    private void checkNodeJoin() {
        try {
            CompletableFuture<Ignite> fut = startNode(NEW_NODE);

            if (!isNodeStarted(CMG_NODE)) {
                assertThrowsWithCause(() -> fut.get(NODE_JOIN_WAIT_TIMEOUT, TimeUnit.MILLISECONDS), TimeoutException.class);

                assertTrue(physicalTopologyContainsNode(NEW_NODE));
                // CMG, which holds logical topology state, is unavailable.
                assertThrowsWithCause(() -> logicalTopologyContainsNode(NEW_NODE), IgniteException.class);

                return;
            } else if (!isNodeStarted(METASTORAGE_NODE)) {
                // Node future can't complete as some components requires Metastorage on start.
                assertThrowsWithCause(() -> fut.get(NODE_JOIN_WAIT_TIMEOUT, TimeUnit.MILLISECONDS), TimeoutException.class);

                assertTrue(physicalTopologyContainsNode(NEW_NODE));
                assertFalse(logicalTopologyContainsNode(NEW_NODE)); //TODO: Is Metastore required to promote node to logical topology?

                return;
            }

            assertThat(fut, willCompleteSuccessfully());

            assertTrue(physicalTopologyContainsNode(((IgniteImpl) fut.join()).id()));
            assertTrue(logicalTopologyContainsNode(((IgniteImpl) fut.join()).id()));
        } finally {
            IgnitionManager.stop(NEW_NODE);
        }
    }

    private void checkCreateTable() {
        Ignite node = initializedNode();

        if (node == null) {
            return;
        }

        String createTableCommand = "CREATE TABLE tempTbl (id INT PRIMARY KEY, val INT) WITH partitions = 1";
        String dropTableCommand = "DROP TABLE IF EXISTS tempTbl";

        try {
            sql(node, null, createTableCommand);
        } finally {
            sql(node, null, dropTableCommand);
        }
    }

    private void checkTxRO() {
        Ignite node = initializedNode();

        if (node == null) {
            return;
        }

        Transaction roTx = node.transactions().readOnly().begin();

        try {
            if (!isNodeStarted(DATA_NODE) && !isNodeStarted(DATA_NODE_2)) {
                assertThrowsWithCause(() -> sql(node, roTx, "SELECT * FROM tbl1"), IgniteException.class);

                return;

                // TODO: Bound table distribution zone to data nodes and uncomment.
                // else if (!clusterNodes.containsKey(DATA_NODE_2)) {
            } else if (isNodeStarted(DATA_NODE_2) && clusterNodes.size() <= 2 /* no quorum */) {
                // Fake transaction with a timestamp from the past.
                Transaction tx0 = Mockito.spy(roTx);
                Mockito.when(tx0.readTimestamp()).thenReturn(new HybridTimestamp(1L, 0));
                sql(node, roTx, "SELECT * FROM tbl1");

                // Transaction with recent timestamp.
                assertThrowsWithCause(() -> sql(node, roTx, "SELECT * FROM tbl1"), IgniteException.class);

                return;
            }

            sql(node, roTx, "SELECT * FROM tbl1");
        } finally {
            roTx.rollback();
        }
    }

    private void checkImplicitTx() {
        Ignite node = initializedNode();

        if (node == null) {
            return;
        }

        // TODO: Bound table distribution zone to data nodes and uncomment.
        // if (!clusterNodes.containsKey(DATA_NODE) || !clusterNodes.containsKey(DATA_NODE_2)) {
        if (clusterNodes.size() <= 2 || !isNodeStarted(DATA_NODE)) {
            assertThrowsWithCause(() -> sql(node, null, "INSERT INTO tbl1 VALUES (2, -2)"), Exception.class);

            return;
        }

        sql(node, null, "INSERT INTO tbl1 VALUES (2, 2)");

        try {
            assertThat(sql(node, null, "SELECT * FROM tbl1").size(), Matchers.equalTo(2));
        } finally {
            sql(node, null, "DELETE FROM tbl1 WHERE tbl1.id = 2");
        }
    }

    private void checkTxRW() {
        Ignite node = initializedNode();

        if (node == null) {
            return;
        }

        // TODO: Bound table distribution zone to data nodes and uncomment.
        // if (!clusterNodes.containsKey(DATA_NODE) || !clusterNodes.containsKey(DATA_NODE_2)) {
        if (clusterNodes.size() <= 2 || !isNodeStarted(DATA_NODE)) {
            Transaction tx = node.transactions().begin();
            try {
                assertThrowsWithCause(() -> sql(node, tx, "INSERT INTO tbl1 VALUES (2, -2)"), Exception.class);
            } finally {
                tx.rollback();
            }

            return;
        }

        Transaction tx = node.transactions().begin();
        try {
            try {
                sql(node, tx, "INSERT INTO tbl1 VALUES (2, 2)");

                tx.commit();
            } finally {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-18324
                // tx.rollback();
            }

            assertThat(sql(node, null, "SELECT * FROM tbl1").size(), Matchers.equalTo(2));

        } finally {
            sql(node, null, "DELETE FROM tbl1 WHERE tbl1.id = 2");
        }
    }

    /** Find started cluster node or return {@code null} if not found. */
    private @Nullable Ignite initializedNode() {
        assert !clusterNodes.isEmpty();

        CompletableFuture<Ignite> nodeFut = clusterNodes.values().iterator().next();

        if (!isNodeStarted(METASTORAGE_NODE)) {
            assertThrowsWithCause(() -> nodeFut.get(NODE_JOIN_WAIT_TIMEOUT, TimeUnit.MILLISECONDS), TimeoutException.class);

            // Assumed, there is no available Ignite instance in grid, which is required for running some checks.
            clusterNodes.forEach((k, v) -> assertNull(v.getNow(null), k));

            return null;
        }

        assertThat(nodeFut, willCompleteSuccessfully());

        return nodeFut.join();
    }
}
