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

package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.metastorage.MetaStorageManager.APPLIED_REV;
import static org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.EntryImpl;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests distribution zones logical topology changes and reaction to that changes.
 */
public class DistributionZoneManagerWatchListenerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME_1 = "zone1";
    private static final String ZONE_NAME_2 = "zone2";

    @Mock
    private ClusterManagementGroupManager cmgManager;

    private VaultManager vaultMgr;

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    private WatchListener watchListener;

    private DistributionZonesConfiguration zonesConfiguration;

    private MetaStorageManager metaStorageManager;

    @BeforeEach
    public void setUp() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        zonesConfiguration = mock(DistributionZonesConfiguration.class);

        metaStorageManager = mock(MetaStorageManager.class);

        cmgManager = mock(ClusterManagementGroupManager.class);

        vaultMgr = mock(VaultManager.class);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                metaStorageManager,
                cmgManager,
                vaultMgr
        );

        clusterCfgMgr.start();

        mockVaultAppliedRevision(1);

        when(vaultMgr.get(zonesLogicalTopologyKey())).thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        when(metaStorageManager.registerWatch(any(ByteArray.class), any())).then(invocation -> {
            watchListener = invocation.getArgument(1);

            return completedFuture(null);
        });

        mockEmptyZonesList();

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage());

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(any());

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            If iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

            return metaStorageService.run(multiInvokeCommand).thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
        }).when(metaStorageManager).invoke(any());
    }

    @AfterEach
    public void tearDown() throws Exception {
        vaultMgr.stop();

        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesUpdatedOnWatchListenerEvent() {
        mockVaultZonesLogicalTopologyKey(Set.of());

        distributionZoneManager.start();

        //TODO: Add second distribution zone, when distributionZones.change.trigger per zone will be created.
        mockZones(mockZoneWithAutoAdjust());

        //first event

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, 1);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, nodes);

        //second event

        nodes = Set.of("node1", "node3");

        watchListenerOnUpdate(nodes, 2);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        checkDataNodesOfZone(1, nodes);

        //third event

        nodes = Collections.emptySet();

        watchListenerOnUpdate(nodes, 3);

        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        checkDataNodesOfZone(1, nodes);
    }

    private void checkDataNodesOfZone(int zoneId, Set<String> nodes) {
        Entry entry = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes());

        if (nodes == null) {
            assertNull(entry.value());
        } else {
            Set<String> newDataNodes = fromBytes(entry.value());

            assertTrue(newDataNodes.containsAll(nodes));
            assertEquals(nodes.size(), newDataNodes.size());
        }
    }

    @Test
    void testStaleWatchEvent() {
        mockVaultZonesLogicalTopologyKey(Set.of());

        distributionZoneManager.start();

        mockZones(mockZoneWithAutoAdjust());

        mockVaultAppliedRevision(1);

        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, revision);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, null);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() {
        mockZones(mockZoneWithAutoAdjust());

        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockVaultAppliedRevision(revision);

        distributionZoneManager.start();

        verify(metaStorageManager, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, null);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() {
        mockZones(mockZoneWithAutoAdjust());

        mockVaultAppliedRevision(2);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, nodes);
    }

    @Test
    void testLogicalTopologyFromCmgOnZoneManagerStart() {
        mockZones(mockZoneWithAutoAdjust());

        mockVaultAppliedRevision(2);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        mockCmgLocalNodes(Set.of(new ClusterNode("1", "node1", null)));

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, Set.of("node1"));
    }

    private void mockEmptyZonesList() {
        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        when(zonesConfiguration.distributionZones().value().namedListKeys()).thenReturn(Collections.emptyList());
    }

    private void mockZones(DistributionZoneConfiguration... zones) {
        List<String> names = new ArrayList<>();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        for (DistributionZoneConfiguration zone : zones) {
            names.add(zone.name().value());

            when(namedConfigurationTree.get(zone.name().value())).thenReturn(zone);
        }

        when(namedListView.namedListKeys()).thenReturn(names);
    }

    private DistributionZoneConfiguration mockZone(
            Integer zoneId,
            String name,
            Integer dataNodesAutoAdjustTime,
            Integer dataNodesAutoAdjustScaleUpTime,
            Integer dataNodesAutoAdjustScaleDownTime
    ) {
        DistributionZoneConfiguration distributionZoneConfiguration = mock(DistributionZoneConfiguration.class);

        ConfigurationValue<String> nameValue = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.name()).thenReturn(nameValue);
        when(nameValue.value()).thenReturn(name);

        ConfigurationValue<Integer> zoneIdValue = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.zoneId()).thenReturn(zoneIdValue);
        when(zoneIdValue.value()).thenReturn(zoneId);

        ConfigurationValue<Integer> dataNodesAutoAdjust = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjust()).thenReturn(dataNodesAutoAdjust);
        when(dataNodesAutoAdjust.value()).thenReturn(dataNodesAutoAdjustTime);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleUp = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjustScaleUp()).thenReturn(dataNodesAutoAdjustScaleUp);
        when(dataNodesAutoAdjustScaleUp.value()).thenReturn(dataNodesAutoAdjustScaleUpTime);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleDown = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjustScaleDown()).thenReturn(dataNodesAutoAdjustScaleDown);
        when(dataNodesAutoAdjustScaleDown.value()).thenReturn(dataNodesAutoAdjustScaleDownTime);

        return distributionZoneConfiguration;
    }

    private DistributionZoneConfiguration mockZoneWithAutoAdjust() {
        return mockZone(1, ZONE_NAME_1, 100, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private DistributionZoneConfiguration mockZoneWithAutoAdjustScaleUpScaleDown() {
        return mockZone(2, ZONE_NAME_2, Integer.MAX_VALUE, 200, 300);
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    private void watchListenerOnUpdate(Set<String> nodes, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);

        org.apache.ignite.internal.metastorage.client.Entry newEntry =
                new EntryImpl(zonesLogicalTopologyKey(), newLogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private void mockVaultAppliedRevision(long revision) {
        when(vaultMgr.get(APPLIED_REV)).thenReturn(completedFuture(new VaultEntry(APPLIED_REV, longToBytes(revision))));
    }

    private LogicalTopologySnapshot mockCmgLocalNodes(Set<ClusterNode> clusterNodes) {
        LogicalTopologySnapshot logicalTopologySnapshot = mock(LogicalTopologySnapshot.class);

        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        when(logicalTopologySnapshot.nodes()).thenReturn(clusterNodes);

        return logicalTopologySnapshot;
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<ClusterNode> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value(), nodes), 1000));
    }

    private void assertZonesChangeTriggerKey(int revision) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> bytesToLong(keyValueStorage.get(zonesChangeTriggerKey().bytes()).value()) == revision, 1000
                )
        );
    }
}
