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

package org.apache.ignite.internal.catalog.compaction;

import static java.util.function.Predicate.not;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.message.CatalogMessageGroup;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeRequest;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeRequestImpl;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeResponse;
import org.apache.ignite.internal.catalog.message.CatalogMinimumRequiredTimeResponseImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * TODO Catalog compaction runner.
 */
public class CatalogCompactionRunner implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogCompactionRunner.class);

    private static final long ACK_TIMEOUT = 5_000;

    private final CatalogCompactionRequiredTimeProvider localMinTimeProvider;

    private final CatalogManagerImpl catalogManager;

    private final MessagingService messagingService;

    private final LogicalTopologyService logicalTopologyService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final TopologyService topologyService;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private volatile CompletableFuture<Boolean> lastRunFuture = CompletableFutures.nullCompletedFuture();

    /**
     * Constructs catalog compaction service.
     */
    public CatalogCompactionRunner(
            TopologyService topologyService,
            MessagingService messagingService,
            LogicalTopologyService logicalTopologyService,
            ClockService clockService,
            PlacementDriver placementDriver,
            CatalogManagerImpl catalogManager,
            CatalogCompactionRequiredTimeProvider localMinTimeProvider,
            Executor executor
    ) {
        this.messagingService = messagingService;
        this.topologyService = topologyService;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManager = catalogManager;
        this.clockService = clockService;
        this.placementDriver = placementDriver;
        this.localMinTimeProvider = localMinTimeProvider;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(CatalogMessageGroup.class, (message, sender, correlationId) -> {
            assert message.groupType() == CatalogMessageGroup.GROUP_TYPE : message.groupType();
            assert message.messageType() == CatalogMessageGroup.MINIMUM_REQUIRED_TIME_REQUEST : message.messageType();
            assert correlationId != null;

            onMessage(sender, correlationId);
        });

        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        return CompletableFutures.nullCompletedFuture();
    }

    /** Called when the low watermark has been changed. */
    public CompletableFuture<Boolean> onLowWatermarkChanged(HybridTimestamp newLowWatermark) {
        if (topologyService.localMember().name().equals(catalogManager.compactionCoordinator())) {
            startCompaction(newLowWatermark);
        }

        return CompletableFutures.falseCompletedFuture();
    }

    @TestOnly
    CompletableFuture<Boolean> lastRunFuture() {
        return lastRunFuture;
    }

    /** Starts the catalog compaction routine. */
    CompletableFuture<Boolean> startCompaction(HybridTimestamp newLowWatermark) {
        return inBusyLock(busyLock, () -> {
            CompletableFuture<Boolean> fut = lastRunFuture;

            if (!fut.isDone()) {
                LOG.info("Catalog compaction is already in progress, skipping for lwm={}", newLowWatermark.longValue());

                return CompletableFutures.falseCompletedFuture();
            }

            fut = startCompaction().whenComplete((res, ex) -> {
                if (ex != null) {
                    LOG.warn("Catalog compaction has failed (timestamp={})", ex, newLowWatermark.longValue());
                } else if (res && LOG.isDebugEnabled()) {
                    LOG.debug("Catalog compaction completed successfully (timestamp={})", newLowWatermark.longValue());
                }
            });

            lastRunFuture = fut;

            return fut;
        });
    }

    private CompletableFuture<Boolean> startCompaction() {
        return resolveMinimumRequiredTimeInCluster()
                .thenCompose(ts -> {
                    Catalog catalog = catalogByTsNullable(ts);

                    if (catalog == null) {
                        return CompletableFutures.falseCompletedFuture();
                    }

                    Set<String> requiredNodes = requiredNodes(catalog);
                    List<String> missingNodes = missingNodes(requiredNodes);

                    if (!missingNodes.isEmpty()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Catalog compaction aborted duo to missing members: {}", missingNodes);
                        }

                        return CompletableFutures.falseCompletedFuture();
                    }

                    return catalogManager.compactCatalog(catalog.time());
                });
    }

    private @Nullable Catalog catalogByTsNullable(long ts) {
        try {
            int catalogVer = catalogManager.activeCatalogVersion(ts);
            return catalogManager.catalog(catalogVer - 1);
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private CompletableFuture<Long> resolveMinimumRequiredTimeInCluster() {
        CatalogMinimumRequiredTimeRequest request = CatalogMinimumRequiredTimeRequestImpl.builder().build();
        LogicalTopologySnapshot logicalTop = logicalTopologyService.localLogicalTopology();
        List<CompletableFuture<?>> ackFutures = new ArrayList<>(logicalTop.nodes().size());
        AtomicLong minimumRequiredTimeHolder = new AtomicLong(Long.MAX_VALUE);

        for (LogicalNode node : logicalTop.nodes()) {
            CompletableFuture<NetworkMessage> fut = messagingService.invoke(node, request, ACK_TIMEOUT)
                    .whenComplete((msg, e) -> {
                        long time = ((CatalogMinimumRequiredTimeResponse) msg).timestamp();
                        long prevTime = minimumRequiredTimeHolder.get();

                        while (time < prevTime) {
                            if (!minimumRequiredTimeHolder.compareAndSet(prevTime, time)) {
                                prevTime = minimumRequiredTimeHolder.get();

                                continue;
                            }

                            break;
                        }
                    });

            ackFutures.add(fut);
        }

        return CompletableFutures.allOf(ackFutures)
                .thenApplyAsync(ignore -> minimumRequiredTimeHolder.get(), executor);
    }

    private void onMessage(ClusterNode sender, long correlationId) {
        CatalogMinimumRequiredTimeResponse response =
                CatalogMinimumRequiredTimeResponseImpl.builder()
                        .timestamp(localMinTimeProvider.timestamp().longValue())
                        .build();

        messagingService.respond(sender, response, correlationId);
    }

    private Set<String> requiredNodes(Catalog catalog) {
        HybridTimestamp nowTs = clockService.now();

        Set<String> required = Collections.newSetFromMap(new ConcurrentHashMap<>());

        Collection<CatalogTableDescriptor> tables = catalog.tables();

        for (CatalogTableDescriptor table : tables) {
            CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

            assert zone != null : table.zoneId();

            List<CompletableFuture<?>> partitionFutures = new ArrayList<>(zone.partitions());

            for (int p = 0; p < zone.partitions(); p++) {
                ReplicationGroupId replicationGroupId = new TablePartitionId(table.id(), p);

                CompletableFuture<TokenizedAssignments> assignmentsFut = placementDriver.getAssignments(replicationGroupId, nowTs)
                        .whenComplete((tokenizedAssignments, ex) -> {
                            if (tokenizedAssignments != null) {
                                List<String> assignments = tokenizedAssignments.nodes().stream()
                                        .map(Assignment::consistentId)
                                        .collect(Collectors.toList());

                                required.addAll(assignments);
                            }
                        });

                partitionFutures.add(assignmentsFut);
            }

            CompletableFutures.allOf(partitionFutures).join();
        }

        return required;
    }

    private List<String> missingNodes(Set<String> requiredNodes) {
        Set<String> logicalNodeIds = logicalTopologyService.localLogicalTopology().nodes()
                .stream()
                .map(ClusterNodeImpl::name)
                .collect(Collectors.toSet());

        return requiredNodes.stream().filter(not(logicalNodeIds::contains)).collect(Collectors.toList());
    }
}
