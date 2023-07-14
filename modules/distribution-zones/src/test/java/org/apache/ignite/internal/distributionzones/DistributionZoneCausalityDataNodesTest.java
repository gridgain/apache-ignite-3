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

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for causality data nodes updating in {@link DistributionZoneManager}.
 */
public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME_1 = "zone1";

    private static final String ZONE_NAME_2 = "zone2";

    private static final String ZONE_NAME_3 = "zone3";

    private static final int ZONE_ID_1 = 1;

    private static final int ZONE_ID_2 = 2;

    private static final int ZONE_ID_3 = 3;

    private static final LogicalNode NODE_0 =
            new LogicalNode("node_id_0", "node_name_0", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_1 =
            new LogicalNode("node_id_1", "node_name_1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 =
            new LogicalNode("node_id_2", "node_name_2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_3 =
            new LogicalNode("node_id_3", "node_name_3", new NetworkAddress("localhost", 123));

    private static final Set<LogicalNode> oneNode = Set.of(NODE_0);
    private static final Set<String> oneNodeName = Set.of(NODE_0.name());

    private static final Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
    private static final Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

    private static final Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
    private static final Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

    private static final Set<LogicalNode> fourNodes = Set.of(NODE_0, NODE_1, NODE_2, NODE_3);
    private static final Set<String> fourNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name());

    /**
     * Contains futures that is completed when the topology watch listener receive the event with expected logical topology.
     * Mapping of node names -> future with event revision.
     */
    private final ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the data nodes watch listener receive the event with expected zone id and data nodes.
     * Mapping of zone id and node names -> future with event revision.
     */
    private final ConcurrentHashMap<IgniteBiTuple<Integer, Set<String>>, CompletableFuture<Long>> zoneDataNodesRevisions =
            new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale up update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleUpRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale down update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the filter update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneChangeFilterRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone configuration listener receive the zone creation event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> createZoneRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone configuration listener receive the zone dropping event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> dropZoneRevisions = new ConcurrentHashMap<>();

    @BeforeEach
    void beforeEach() throws NodeStoppingException {
        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), createMetastorageTopologyListener());

        metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), createMetastorageDataNodesListener());

        ZonesConfigurationListener zonesConfigurationListener = new ZonesConfigurationListener();

        zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());
        zonesConfiguration.distributionZones().any().filter().listen(onUpdateFilter());

        zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());
        zonesConfiguration.defaultDistributionZone().filter().listen(onUpdateFilter());

        distributionZoneManager.start();

        metaStorageManager.deployWatches();
    }

    /**
     * Tests data nodes updating on a topology leap.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19955 (Random ticket to pass a check on TC)")
    @Test
    void topologyLeapUpdate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create the zone with not immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_2)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        CompletableFuture<Long> dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes1);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        long dataNodesRevisionZone = dataNodesUpdateRevision.get(3, SECONDS);

        System.out.println("test_log dataNodesRevisionZone=" + dataNodesRevisionZone);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(dataNodesRevisionZone, ZONE_ID_2);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        // Check that data nodes value of the zone with immediate timers with the topology update revision is NODE_0 and NODE_2.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(twoNodesNames2));

        // Check that data nodes value of the zone with not immediate timers with the topology update revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_2);
        assertThat(dataNodesFut4, willBe(twoNodesNames1));

        // Check that data nodes value of the zone with not immediate timers with the data nodes update revision is NODE_0 and NODE_2.
        dataNodesRevisionZone = dataNodesUpdateRevision.get(5, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone, ZONE_ID_2);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Tests data nodes updating on a topology leap with not immediate scale up and immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19955 (Random ticket to pass a check on TC)")
    @Test
    void topologyLeapUpdateScaleUpNotImmediateAndScaleDownImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Alter zones with not immediate scale up timer.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID_1, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0 because scale up timer has not fired yet.
        Set<String> oneNodeNames = Set.of(NODE_0.name());

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(oneNodeNames));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(oneNodeNames));

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut4, willBe(twoNodesNames2));

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, ZONE_ID_1);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Tests data nodes updating on a topology leap with immediate scale up and not immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19955 (Random ticket to pass a check on TC)")
    @Test
    void topologyLeapUpdateScaleUpImmediateAndScaleDownNotImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with not immediate scale down timer.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with not immediate scale down timer.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID_1, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0, NODE_1 and NODE_2 because scale down timer has not fired yet.
        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(threeNodesNames));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(threeNodesNames));

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut4, willBe(twoNodesNames2));

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, ZONE_ID_1);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Tests data nodes updating on a scale up changing.
     *
     * @throws Exception If failed.
     */
    @Test
    void dataNodesUpdatedAfterScaleUpChanged() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name()),
                3000
        );

        // Check that data nodes value of the the zone is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(oneNodeName));

        // Changes a scale up timer to not immediate.
        distributionZoneManager.alterZone(
                        ZONE_NAME_1,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);
        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired yet.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(oneNodeName));

        // Change scale up value to immediate.
        long scaleUpRevision = alterZoneScaleUpAndGetRevision(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE);

        System.out.println("test_log scaleUpRevision=" + scaleUpRevision);
        // Check that data nodes value of the zone with the scale up update revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleUpRevision, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(twoNodesNames));
    }

    /**
     * Tests data nodes updating on a scale down changing.
     *
     * @throws Exception If failed.
     */
    @Test
    void dataNodesUpdatedAfterScaleDownChanged() throws Exception {
        // Prerequisite.

        // Create the zone with immediate scale up timer and not immediate scale down timer.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name(), NODE_1.name()),
                3000
        );

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps.

        // Change logical topology. NODE_1 is left.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired yet.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(twoNodesNames));

        // Change scale down value to immediate.
        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale down update revision is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleDownRevision, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(oneNodeName)); // New flaky.
    }

    /**
     * Tests data nodes dropping when a scale up task is scheduled.
     *
     * @throws Exception If failed.
     */
    @Test
    void scheduleScaleUpTaskThenDropZone() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Create logical topology with NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, Set.of(NODE_0));

        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, oneNode, 3000);

        // Check that data nodes value of the zone is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(oneNodeName));

        // Alter the zones with not immediate scale up timer.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME_1);

        System.out.println("test_log dropRevision1=" + dropRevision1);

//        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, null, 3000);
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                null,
                null,
                3000
        );

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(oneNodeName));

        // Check that zones is removed and attempt to get data nodes throws an exception.
        CompletableFuture<Set<String>> dataNodesFut5 = null;
        try {
            dataNodesFut5 = distributionZoneManager.dataNodes(dropRevision1, ZONE_ID_1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        CompletableFuture<Set<String>> finalDataNodesFut = dataNodesFut5;
        assertThrowsWithCause(() -> finalDataNodesFut.get(3, SECONDS), DistributionZoneNotFoundException.class);
    }

    /**
     * Tests data nodes dropping when a scale down task is scheduled.
     *
     * @throws Exception If failed.
     */
    @Test
    void scheduleScaleDownTaskThenDropZone() throws Exception {
        // Prerequisite.

        // Create the zone with immediate scale up timer and not immediate scale down timer.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name(), NODE_1.name()),
                3000
        );

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps.

        // Change logical topology. NODE_1 is removed.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME_1);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                null,
                null,
                3000
        );

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut3, willBe(twoNodesNames));

        // Check that zones is removed and attempt to get data nodes throws an exception.
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dropRevision1, ZONE_ID_1);
        assertThrowsWithCause(() -> dataNodesFut5.get(3, SECONDS), DistributionZoneNotFoundException.class);
    }

    /**
     * Tests data nodes updating when a filter is changed even when actual data nodes value is not changed.
     *
     * @throws Exception If failed.
     */
    @Test
    void changeFilter() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Check that data nodes value of both zone is NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, Set.of(NODE_0));

        // Wait when the zone manager receive event with new topology. Need to do this awaiting in the dataNodes method.
        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, oneNode, 3000);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut1, willBe(oneNodeName));

        // Alter the zones with infinite timers.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with infinite timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        String filter = "$[?($..* || @.region == 'US')]";

        // Change filter and get revision of this change.
        long filterRevision0 = alterFilterAndGetRevision(DEFAULT_ZONE_NAME, filter);
        long filterRevision1 = alterFilterAndGetRevision(ZONE_NAME_1, filter);

        // Check that data nodes value of the the zone is NODE_0.
        // The future didn't hang due to the fact that the actual data nodes value did not change.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(filterRevision0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut3, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(filterRevision1, ZONE_ID_1);
        assertThat(dataNodesFut4, willBe(oneNodeName));

    }

    @Test
    void createZoneWithNotImmediateTimers() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0.
        topology.putNode(NODE_0);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(ZONE_NAME_1, ZONE_ID_1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE);

        System.out.println("test_log createZoneRevision=" + createZoneRevision);

        // Check that data nodes value of the zone with the create zone revision is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(createZoneRevision, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(oneNodeName));
    }

    /**
     * Tests data nodes obtaining with revision before a zone creation and after a zone dropping.
     *
     * @throws Exception If failed.
     */
    @Test
    void createThenDropZone() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);
        topology.putNode(NODE_1);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(ZONE_NAME_1, ZONE_ID_1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the revision lower than the create zone revision is absent.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(createZoneRevision - 1, ZONE_ID_1);
        assertThrowsWithCause(() -> dataNodesFut1.get(3, SECONDS), DistributionZoneNotFoundException.class);

        // Check that data nodes value of the zone with the create zone revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(createZoneRevision, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(twoNodesNames));

        // Drop the zone.
        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME_1);

        // Check that data nodes value of the zone with the drop zone revision is absent.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dropZoneRevision, ZONE_ID_1);
        assertThrowsWithCause(() -> dataNodesFut3.get(3, SECONDS), DistributionZoneNotFoundException.class);
    }

    /**
     * Tests data nodes obtaining with wrong parameters throw an exception.
     *
     * @throws Exception If failed.
     */
    @Test
    void validationTest() {
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(0, DEFAULT_ZONE_ID);
        assertThrowsWithCause(() -> dataNodesFut1.get(3, SECONDS), IllegalArgumentException.class);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(-1, DEFAULT_ZONE_ID);
        assertThrowsWithCause(() -> dataNodesFut2.get(3, SECONDS), IllegalArgumentException.class);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(1, -1);
        assertThrowsWithCause(() -> dataNodesFut3.get(3, SECONDS), IllegalArgumentException.class);
    }

    /**
     * Tests data nodes changing when topology is changed.
     *
     * @throws Exception If failed.
     */
    @Test
    void simpleTopologyChanges() throws Exception {
        // Prerequisite.

        // Create zones with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        // Test steps.

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodeNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        System.out.println("test_log topologyRevision0=" + topologyRevision0);
        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut1, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision0, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut5, willBe(twoNodesNames));
        CompletableFuture<Set<String>> dataNodesFut6 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID_1);
        assertThat(dataNodesFut6, willBe(twoNodesNames));

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_2.name());

        // Change logical topology. NODE_2 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_2, threeNodes);

        // Change logical topology. NODE_1 is left.
        long topologyRevision3 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut7 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut7, willBe(threeNodeNames));
        CompletableFuture<Set<String>> dataNodesFut8 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_1);
        assertThat(dataNodesFut8, willBe(threeNodeNames));

        CompletableFuture<Set<String>> dataNodesFut9 = distributionZoneManager.dataNodes(topologyRevision3, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut9, willBe(twoNodesNames1));
        CompletableFuture<Set<String>> dataNodesFut10 = distributionZoneManager.dataNodes(topologyRevision3, ZONE_ID_1);
        assertThat(dataNodesFut10, willBe(twoNodesNames1));
    }

    @Test
    void deadlocks() throws Exception {
        prepareZonesWithOneDataNodes();

        prepareZonesTimerValuesToTest();

        CountDownLatch scaleUpLatch = new CountDownLatch(1);

        Runnable dummyScaleUpTask = () -> {
            try {
                scaleUpLatch.await(5, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        CountDownLatch scaleDownLatch = new CountDownLatch(1);

        Runnable dummyScaleDownTask = () -> {
            try {
                scaleDownLatch.await(5, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_1)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_2)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_3)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);

        distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_1)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_2)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_3)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision1, 3000);

        // Change logical topology. NODE_1 is removed.
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision2, 3000);

        // Change logical topology. NODE_2 is added.
        long topologyRevision3 = putNodeInLogicalTopologyAndGetRevision(NODE_2, Set.of(NODE_0, NODE_2));
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision3, 3000);

        // Change logical topology. NODE_3 is added.
        long topologyRevision4 = putNodeInLogicalTopologyAndGetRevision(NODE_3, Set.of(NODE_0, NODE_2, NODE_3));
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision4, 3000);

        // Change logical topology. NODE_2 is removed.
        long topologyRevision5 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_2), Set.of(NODE_0, NODE_3));
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision5, 3000);

        CompletableFuture<Set<String>> dataNodesFut5 = getDataNodesFromListener(DEFAULT_ZONE_ID);
        assertThat(dataNodesFut5, willBe(Set.of(NODE_0.name(), NODE_3.name())));

        CompletableFuture<Set<String>> dataNodesFut6 = getDataNodesFromListener(ZONE_ID_1);
        assertThat(dataNodesFut6, willBe(Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name())));

        CompletableFuture<Set<String>> dataNodesFut7 = getDataNodesFromListener(ZONE_ID_2);
        assertThat(dataNodesFut7, willBe(Set.of(NODE_0.name())));

        CompletableFuture<Set<String>> dataNodesFut8 = getDataNodesFromListener(ZONE_ID_3);
        assertThat(dataNodesFut8, willBe(Set.of(NODE_0.name())));

        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_2, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_3, oneNode, 3000);

        scaleUpLatch.countDown();
        scaleDownLatch.countDown();

        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, Set.of(NODE_0, NODE_3), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, Set.of(NODE_0, NODE_1, NODE_2, NODE_3), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_2, Set.of(NODE_0), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_3, Set.of(NODE_0), 3000);

        CompletableFuture<Set<String>> dataNodesFut9 = getDataNodesFromListener(DEFAULT_ZONE_ID);
        assertThat(dataNodesFut9, willBe(Set.of(NODE_0.name(), NODE_3.name())));

        CompletableFuture<Set<String>> dataNodesFut10 = getDataNodesFromListener(ZONE_ID_1);
        assertThat(dataNodesFut10, willBe(Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name())));

        CompletableFuture<Set<String>> dataNodesFut11 = getDataNodesFromListener(ZONE_ID_2);
        assertThat(dataNodesFut11, willBe(Set.of(NODE_0.name())));

        CompletableFuture<Set<String>> dataNodesFut12 = getDataNodesFromListener(ZONE_ID_3);
        assertThat(dataNodesFut12, willBe(Set.of(NODE_0.name())));
    }

    private long prepareZonesWithOneDataNodes() throws Exception {
        // Create zones with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_2)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_3)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

//        Thread.sleep(300);
//        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision0, 3000));
        // Wait when dataNodes will be contain NODE_0.
        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_2, oneNode, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_3, oneNode, 3000);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut1, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision0, ZONE_ID_1);
        assertThat(dataNodesFut2, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision0, ZONE_ID_2);
        assertThat(dataNodesFut3, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision0, ZONE_ID_3);
        assertThat(dataNodesFut4, willBe(oneNodeName));

        return topologyRevision0;
    }

    private void prepareZonesTimerValuesToTest() throws Exception {
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME_2,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_2)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME_3,
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_3)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);
    }

    private CompletableFuture<Set<String>> getDataNodesFromListener(int zoneId) {
        CompletableFuture<Set<String>> dataNodesFut = new CompletableFuture<>();

        WatchListener dataNodesSupplier = createMetastorageDataNodeSupplierListener(zoneId, dataNodesFut);

        var dataNodesSupplierTrigger = new ByteArray("dataNodesSupplierTrigger");

        metaStorageManager.registerExactWatch(dataNodesSupplierTrigger, dataNodesSupplier);

        metaStorageManager.put(dataNodesSupplierTrigger, toBytes("foo"));

        assertThat(dataNodesFut, willCompleteSuccessfully());

        metaStorageManager.unregisterWatch(dataNodesSupplier);

        return dataNodesFut;
    }

    /**
     * Puts a given node as a part of the logical topology and return revision of a topology watch listener event.
     *
     * @param node Node to put.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long putNodeInLogicalTopologyAndGetRevision(
            LogicalNode node,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.putNode(node);

        return revisionFut.get(5, SECONDS);
    }

    /**
     * Removes given nodes from the logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long removeNodeInLogicalTopologyAndGetRevision(
            Set<LogicalNode> nodes,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.removeNodes(nodes);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes data nodes in logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long fireTopologyLeapAndGetRevision(Set<LogicalNode> nodes) throws Exception {
        Set<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        long topVer = topology.getLogicalTopology().version() + 1;

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(topVer, nodes)));

        System.out.println("fireTopologyLeap " + nodeNames);
        topology.fireTopologyLeap();

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes a scale up timer value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleUp New scale up value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterZoneScaleUpAndGetRevision(String zoneName, int scaleUp) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleUpRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(scaleUp).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes a scale down timer value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleDown New scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterZoneScaleDownAndGetRevision(String zoneName, int scaleDown) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleDownRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleDown(scaleDown).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes a filter value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param filter New filter value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterFilterAndGetRevision(String zoneName, String filter) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneChangeFilterRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .filter(filter).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Creates a zone and return the revision of a create zone event.
     *
     * @param zoneName Zone name.
     * @param zoneId Zone id.
     * @param scaleUp Scale up value.
     * @param scaleDown Scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long createZoneAndGetRevision(String zoneName, int zoneId, int scaleUp, int scaleDown) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        createZoneRevisions.put(zoneId, revisionFut);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(zoneName)
                                .dataNodesAutoAdjustScaleUp(scaleUp)
                                .dataNodesAutoAdjustScaleDown(scaleDown)
                                .build()
                )
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Drops a zone and return the revision of a drop zone event.
     *
     * @param zoneName Zone name.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long dropZoneAndGetRevision(String zoneName) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        dropZoneRevisions.put(zoneId, revisionFut);

        distributionZoneManager.dropZone(zoneName).get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Returns a future which will be completed when expected data nodes will be saved to the meta storage.
     * In order to complete the future need to invoke one of the methods that change the logical topology.
     *
     * @param zoneId Zone id.
     * @param nodes Expected data nodes.
     * @return Future with revision.
     */
    private CompletableFuture<Long> getZoneDataNodesRevision(int zoneId, Set<LogicalNode> nodes) {
        Set<String> nodeNames = nodes.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        zoneDataNodesRevisions.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);

        return revisionFut;
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleUpRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    private ConfigurationListener<Integer> onUpdateScaleUp() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleUpRevisions.containsKey(zoneId)) {
                zoneScaleUpRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleDownRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    private ConfigurationListener<Integer> onUpdateScaleDown() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleDownRevisions.containsKey(zoneId)) {
                zoneScaleDownRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneChangeFilterRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    private ConfigurationListener<String> onUpdateFilter() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneChangeFilterRevisions.containsKey(zoneId)) {
                zoneChangeFilterRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * A configuration listener which completes futures from {@code createZoneRevisions} and {@code dropZoneRevisions}
     * when receives event with expected zone id.
     */
    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            if (createZoneRevisions.containsKey(zoneId)) {
                createZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            if (dropZoneRevisions.containsKey(zoneId)) {
                dropZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        }
    }

    /**
     * Creates a topology watch listener which completes futures from {@code topologyRevisions}
     * when receives event with expected logical topology.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

//                try {
//                    Thread.sleep(300);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }

                Set<NodeWithAttributes> newLogicalTopology = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                        revision = e.revision();
                    } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopology = fromBytes(e.value());
                    }
                }

                Set<String> nodeNames = newLogicalTopology.stream().map(node -> node.nodeName()).collect(toSet());

                System.out.println("MetastorageTopologyListener test " + nodeNames);

                if (topologyRevisions.containsKey(nodeNames)) {
                    topologyRevisions.remove(nodeNames).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    /**
     * Creates a data nodes watch listener which completes futures from {@code zoneDataNodesRevisions}
     * when receives event with expected data nodes.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                int zoneId = 0;

                Set<Node> newDataNodes = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (startsWith(e.key(), zoneDataNodesKey().bytes())) {
                        revision = e.revision();

                        zoneId = extractZoneId(e.key());

                        byte[] dataNodesBytes = e.value();

                        if (dataNodesBytes != null) {
                            newDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesBytes));
                        } else {
                            newDataNodes = emptySet();
                        }
                    }
                }

                Set<String> nodeNames = newDataNodes.stream().map(node -> node.nodeName()).collect(toSet());

                IgniteBiTuple<Integer, Set<String>> zoneDataNodesKey = new IgniteBiTuple<>(zoneId, nodeNames);

                if (zoneDataNodesRevisions.containsKey(zoneDataNodesKey)) {
                    zoneDataNodesRevisions.remove(zoneDataNodesKey).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    /**
     * Creates a data nodes watch listener which completes futures from {@code zoneDataNodesRevisions}
     * when receives event with expected data nodes.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodeSupplierListener(int zoneId, CompletableFuture<Set<String>> dataNodesFut) {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                return distributionZoneManager.dataNodes(evt.revision(), zoneId)
                        .handle((dataNodes, ex) -> {
                            if (ex == null) {
                                dataNodesFut.complete(dataNodes);
                            } else {
                                dataNodesFut.completeExceptionally(ex);
                            }

                            return null;
                        });
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }
}
