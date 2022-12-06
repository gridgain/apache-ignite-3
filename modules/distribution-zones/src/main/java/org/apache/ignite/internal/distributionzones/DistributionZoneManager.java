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

import static org.apache.ignite.lang.ErrorGroups.Common.UNEXPECTED_ERR;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneRenameException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.Statement;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.LogicalTopologyEventListener;
import org.apache.ignite.network.LogicalTopologyService;
import org.apache.ignite.network.LogicalTopologySnapshot;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** Distribution zone configuration. */
    private final DistributionZonesConfiguration zonesConfiguration;

    public static final String DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_PREFIX = "distributionZone.logicalTopology";

    public static final String DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_VERSION_PREFIX = "distributionZone.logicalTopologyVersion";

    private final ClusterManagementGroupManager cmgMgr;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final LogicalTopologyService logicalTopologyService;

    private final MetaStorageManager metaStorageMgr;

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     */
    public DistributionZoneManager(
            DistributionZonesConfiguration zonesConfiguration,
            ClusterManagementGroupManager cmgMgr,
            LogicalTopologyService logicalTopologyService,
            MetaStorageManager metaStorageMgr
    ) {
        this.zonesConfiguration = zonesConfiguration;

        this.cmgMgr = cmgMgr;

        this.logicalTopologyService = logicalTopologyService;

        this.metaStorageMgr = metaStorageMgr;
    }

    /**
     * Creates a new distribution zone with the given {@code name} asynchronously.
     *
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> createZone(DistributionZoneConfigurationParameters distributionZoneCfg) {
        Objects.requireNonNull(distributionZoneCfg, "Distribution zone configuration is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                try {
                    zonesListChange.create(distributionZoneCfg.name(), zoneChange -> {
                        if (distributionZoneCfg.dataNodesAutoAdjust() == null) {
                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(
                                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                        }

                        int intZoneId = zonesChange.globalIdCounter() + 1;
                        zonesChange.changeGlobalIdCounter(intZoneId);

                        zoneChange.changeZoneId(intZoneId);
                    });
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneAlreadyExistsException(distributionZoneCfg.name(), e);
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
                }
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Alters a distribution zone.
     *
     * @param name Distribution zone name.
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneConfigurationParameters distributionZoneCfg) {
        Objects.requireNonNull(name, "Distribution zone name is null.");
        Objects.requireNonNull(distributionZoneCfg, "Distribution zone configuration is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                NamedListChange<DistributionZoneView, DistributionZoneChange> renameChange;

                try {
                    renameChange = zonesListChange
                            .rename(name, distributionZoneCfg.name());
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneRenameException(name, distributionZoneCfg.name(), e);
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
                }

                try {
                    renameChange
                            .update(
                                    distributionZoneCfg.name(), zoneChange -> {
                                        if (distributionZoneCfg.dataNodesAutoAdjust() != null) {
                                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                                            zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                                            zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                                        }

                                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() != null) {
                                            zoneChange.changeDataNodesAutoAdjustScaleUp(
                                                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                        }

                                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() != null) {
                                            zoneChange.changeDataNodesAutoAdjustScaleDown(
                                                    distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                        }
                                    });
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneNotFoundException(distributionZoneCfg.name(), e);
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
                }
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops a distribution zone with the name specified.
     *
     * @param name Distribution zone name.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> dropZone(String name) {
        Objects.requireNonNull(name, "Distribution zone name is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                DistributionZoneView view = zonesListChange.get(name);

                if (view == null) {
                    throw new DistributionZoneNotFoundException(name);
                }

                zonesListChange.delete(name);
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        logicalTopologyService.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
                //Timers here
                updateMetastorageKeys(appearedNode, newTopology);
            }

            @Override
            public void onDisappeared(ClusterNode disappearedNode, LogicalTopologySnapshot newTopology) {
                //Timers here
                updateMetastorageKeys(disappearedNode, newTopology);
            }

            @Override
            public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                updateMetastorageKeys(newTopology);
            }
        });

        initMetastorageKeys();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {

    }

    private void updateMetastorageKeys(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
        If iff = new If(Conditions.value(new ByteArray(DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_VERSION_PREFIX)).eq(ByteUtils.longToBytes(newTopology.version() - 1)),
                new Statement(), new Statement());

        metaStorageMgr.invoke(iff);
    }

    private void updateMetastorageKeys(LogicalTopologySnapshot newTopology) {
        //Compare version from newTopology and metastorage
        If iff = new If();
        metaStorageMgr.invoke(iff);
    }

    private void initMetastorageKeys() throws Exception {
        logicalTopologyService.logicalTopologyOnLeader().thenAccept(snapshot -> {
            long topVerFromCmg = snapshot.version();

            //long topVerFromMetastorage = metaStorageMgr.get(new ByteArray(DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_PREFIX)).get();

            Set<ClusterNode> nodesFromCmg = snapshot.nodes();

            //Set<ClusterNode> nodesFromMetastorege = metaStorageMgr.get(new ByteArray(DISTRIBUTION_ZONE_LOGICAL_TOPOLOGY_VERSION_PREFIX)).get();

            if (...) {
                If iff = new If();
                metaStorageMgr.invoke(iff);
            }
        });
    }
}
