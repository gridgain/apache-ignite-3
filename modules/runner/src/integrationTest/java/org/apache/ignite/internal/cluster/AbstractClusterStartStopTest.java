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

import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.EnvironmentDefaultValueProvider;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.text.IsEmptyString;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;


@ExtendWith(WorkDirectoryExtension.class)
@WithSystemProperty(key = "org.jline.terminal.dumb", value = "true")
abstract class AbstractClusterStartStopTest extends BaseIgniteAbstractTest {
    protected static final int NODE_JOIN_WAIT_TIMEOUT = 2_000;

    /** Work directory. */
    @WorkDirectory
    protected static Path WORK_DIR;

    /** Addresses for Node filder. */
    protected static final String connectionAddr = "\"localhost:3344\", \"localhost:3345\", \"localhost:3346\"";

    /** Correct ignite cluster url. */
    protected static final String NODE_URL = "http://localhost:10300";

    /** Cluster management group node name. */
    protected static final String CMG_NODE = "node1";
    /** MetaStorage group node name. */
    protected static final String METASTORAGE_NODE = "node3";
    /** Data node 1 name. */
    protected static final String DATA_NODE = "node2"; // Partition leader.
    /** Data node 2 name. */
    protected static final String DATA_NODE_2 = "node4";
    /** New node name. */
    protected static final String NEW_NODE = "newNode";

    /** Nodes configurations. */
    protected static final Map<String, String> nodesCfg = Map.of(
            "node1", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\":{\n"
                    + "      \"netClusterNodes\": [ " + connectionAddr + " ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
            "node2", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\":{\n"
                    + "      \"netClusterNodes\": [ " + connectionAddr + " ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
            "node3", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3346,\n"
                    + "    \"nodeFinder\":{\n"
                    + "      \"netClusterNodes\": [ " + connectionAddr + " ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
            "node4", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3347,\n"
                    + "    \"nodeFinder\":{\n"
                    + "      \"netClusterNodes\": [ " + connectionAddr + " ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",
            "newNode", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3348,\n"
                    + "    \"nodeFinder\":{\n"
                    + "      \"netClusterNodes\": [ " + connectionAddr + " ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}");

    // TODO: Change Map -> Set.
    // Map is used as node names uses for partition affinity calculation,
    // but we can't guarantee a node with name "DATA_NODE" will own a partition.
    protected static final Map<String, String> nodeAliasToNameMapping = Map.of(
            "C", CMG_NODE,
            "M", METASTORAGE_NODE,
            "D", DATA_NODE,
            "D2", DATA_NODE_2
    );

    /** Resolves node alias to node name. */
    protected static String resolve(String nodeAliases) {
        return nodeAliasToNameMapping.get(nodeAliases);
    }

    /** Cluster nodes. */
    protected final Map<String, CompletableFuture<Ignite>> clusterNodes = new HashMap<>();

    /** Starts and initialize grid. */
    protected List<CompletableFuture<Ignite>> initGrid(Collection<String> nodes) throws Exception {
        List<CompletableFuture<Ignite>> futures = startNodes(nodes);

        // Init cluster.
        IgnitionManager.init(CMG_NODE, List.of(METASTORAGE_NODE), List.of(CMG_NODE), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());
        }

        // Create tables.
        IgniteImpl node = (IgniteImpl) futures.get(0).join();
        sql(node, null, "CREATE TABLE tbl1 (id INT PRIMARY KEY, val INT) WITH partitions = 1, replicas = 1");

        for (CompletableFuture<Ignite> f : futures) {
            ReplicaManager replicaMgr = (ReplicaManager) MethodHandles.privateLookupIn(IgniteImpl.class, MethodHandles.lookup())
                    .findVarHandle(IgniteImpl.class, "replicaMgr", ReplicaManager.class)
                    .get(f.get());

            assertTrue(DATA_NODE.equals(f.get().name()) ^ replicaMgr.startedGroups().isEmpty());
        }

        sql(node, null, "INSERT INTO tbl1(id, val) VALUES (1,1)");

        return futures;
    }

    protected List<CompletableFuture<Ignite>> startNodes(Collection<String> names) {
        return names.stream()
                .map(this::startNode)
                .collect(Collectors.toList());
    }


    protected void stopAllNodes() {
        List<String> names0 = List.copyOf(clusterNodes.keySet());

        for (int i = names0.size() - 1; i >= 0; i--) {
            stopNode(names0.get(i));
        }
    }

    protected CompletableFuture<Ignite> startNode(String nodeName) {
        String nodeConfig = nodesCfg.get(nodeName);

        CompletableFuture<Ignite> fut = IgnitionManager.start(nodeName, nodeConfig, WORK_DIR.resolve(nodeName));

        clusterNodes.put(nodeName, fut);

        return fut;
    }

    protected void stopNode(String nodeName) {
        CompletableFuture<Ignite> rmv = clusterNodes.remove(nodeName);

        assert rmv != null;

        IgnitionManager.stop(nodeName);
    }

    protected boolean isNodeStarted(String n) {
        return clusterNodes.containsKey(n);
    }

    protected static List<List<Object>> sql(Ignite node, @Nullable Transaction tx, String sql, Object... args) {
        var queryEngine = ((IgniteImpl) node).queryEngine();

        SessionId sessionId = queryEngine.createSession(5_000, PropertiesHolder.fromMap(
                Map.of(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
        ));

        try {
            var context = tx != null ? QueryContext.of(tx) : QueryContext.of();

            return getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, sql, args))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    protected static boolean logicalTopologyContainsNode(String nodeId) {
        return nodeInTopology("logical", nodeId);
    }

    protected static boolean physicalTopologyContainsNode(String nodeId) {
        return nodeInTopology("physical", nodeId);
    }

    private static boolean nodeInTopology(String topologyType, String nodeId) {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();

        new CommandLine(TopLevelCliCommand.class, new MicronautFactory(ApplicationContext.run(Environment.TEST)))
                .setDefaultValueProvider(new EnvironmentDefaultValueProvider())
                .setOut(new PrintWriter(out, true))
                .setErr(new PrintWriter(err, true))
                .execute("cluster", "topology", topologyType, "--cluster-endpoint-url", NODE_URL);

        assertThat(err.toString(), IsEmptyString.emptyString());

        return Pattern.compile("\\b" + nodeId + "\\b").matcher(out.toString()).find();
    }

    /**
     * Grids configurations generator.
     */
    protected static class GridGenerator {
        private final LinkedHashSet<String> currentGrid = new LinkedHashSet<>();
        private final List<List<String>> gridStartSequences = new ArrayList<>();
        private final BiPredicate<String, Set<String>> nodeFilter;
        private final Predicate<Set<String>> gridFilter;
        private Collection<String> nodeNames;

        protected GridGenerator(Set<String> nodeNames, BiPredicate<String, Set<String>> nodeFilter, Predicate<Set<String>> gridFilter) {
            this.nodeNames = nodeNames;
            this.nodeFilter = nodeFilter;
            this.gridFilter = gridFilter;
        }

        /** Generates tests execution sequence recursively. */
        List<List<String>> generate() {
            generate0(nodeNames);

            return gridStartSequences;
        }

        /** Generates tests execution sequence recursively. */
        private void generate0(Collection<String> availableNodes) {
            if (gridFilter.test(currentGrid)) {
                gridStartSequences.add(new ArrayList<>(currentGrid)); // Copy mutable collection.
            }

            for (String node : availableNodes) {
                if (!nodeFilter.test(node, currentGrid)) {
                    continue; // Skip node from adding to the current grid.
                }

                currentGrid.add(node);

                HashSet<String> unusedNodes = new HashSet<>(availableNodes);
                unusedNodes.remove(node);

                generate0(unusedNodes);

                currentGrid.remove(node);
            }
        }

    }
}
