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

package org.apache.ignite.internal.cluster.management;


import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.TestInfo;

/**
 * Fake node for integration tests.
 */
public class MockNode {
    private ClusterManagementGroupManager clusterManager;

    private ClusterService clusterService;

    private ClusterInitializer clusterInitializer;

    private final TestInfo testInfo;

    private final NodeFinder nodeFinder;

    private final Path workDir;

    private final RaftConfiguration raftConfiguration;

    private final ClusterManagementConfiguration cmgConfiguration;

    private final NodeAttributesCollector nodeAttributes;

    private final List<IgniteComponent> components = new ArrayList<>();

    private CompletableFuture<Void> startFuture;

    /**
     * Fake node constructor.
     */
    public MockNode(
            TestInfo testInfo,
            NetworkAddress addr,
            NodeFinder nodeFinder,
            Path workDir,
            RaftConfiguration raftConfiguration,
            ClusterManagementConfiguration cmgConfiguration,
            NodeAttributesConfiguration nodeAttributes
    ) {
        this.testInfo = testInfo;
        this.nodeFinder = nodeFinder;
        this.workDir = workDir;
        this.raftConfiguration = raftConfiguration;
        this.cmgConfiguration = cmgConfiguration;
        this.nodeAttributes = new NodeAttributesCollector(nodeAttributes);

        try {
            init(addr.port());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init(int port) throws IOException {
        Path vaultDir = workDir.resolve("vault");

        var vaultManager = new VaultManager(new PersistentVaultService(testNodeName(testInfo, port), Files.createDirectories(vaultDir)));

        this.clusterService = ClusterServiceTestUtils.clusterService(testInfo, port, nodeFinder);

        Loza raftManager = new Loza(clusterService, raftConfiguration, workDir, new HybridClockImpl());

        var clusterStateStorage = new RocksDbClusterStateStorage(workDir.resolve("cmg"));

        var logicalTopologyService = new LogicalTopologyImpl(clusterStateStorage);

        this.clusterInitializer = new ClusterInitializer(
                clusterService,
                hocon -> hocon,
                new TestConfigurationValidator()
        );

        this.clusterManager = new ClusterManagementGroupManager(
                vaultManager,
                clusterService,
                clusterInitializer,
                raftManager,
                clusterStateStorage,
                logicalTopologyService,
                cmgConfiguration,
                nodeAttributes
        );

        components.add(vaultManager);
        components.add(clusterService);
        components.add(raftManager);
        components.add(clusterStateStorage);
        components.add(clusterManager);
    }

    /**
     * Start fake node.
     */
    public void startComponents() {
        components.forEach(IgniteComponent::start);
    }

    /**
     * Start fake node.
     */
    public void start() {
        startComponents();

        startFuture = clusterManager.onJoinReady();
    }

    /**
     * Method should be called before node stop.
     */
    public void beforeNodeStop() {
        ReverseIterator<IgniteComponent> it = new ReverseIterator<>(components);

        it.forEachRemaining(IgniteComponent::beforeNodeStop);
    }

    /**
     * Stop fake node.
     */
    public void stop() {
        ReverseIterator<IgniteComponent> it = new ReverseIterator<>(components);

        it.forEachRemaining(component -> {
            try {
                component.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Restart fake node.
     */
    public void restart() throws Exception {
        int port = localMember().address().port();

        beforeNodeStop();
        stop();

        components.clear();

        init(port);

        start();
    }

    public ClusterNode localMember() {
        return clusterService.topologyService().localMember();
    }

    public String name() {
        return localMember().name();
    }

    public ClusterInitializer clusterInitializer() {
        return clusterInitializer;
    }

    public ClusterManagementGroupManager clusterManager() {
        return clusterManager;
    }

    public CompletableFuture<Void> startFuture() {
        return startFuture;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    CompletableFuture<Set<LogicalNode>> logicalTopologyNodes() {
        return clusterManager().logicalTopology().thenApply(LogicalTopologySnapshot::nodes);
    }

    CompletableFuture<Set<ClusterNode>> validatedNodes() {
        return clusterManager().validatedNodes();
    }
}
