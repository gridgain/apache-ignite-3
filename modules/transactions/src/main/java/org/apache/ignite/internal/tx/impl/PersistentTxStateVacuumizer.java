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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.network.ClusterNode;

/**
 * Implements the logic of persistent tx states vacuum.
 */
public class PersistentTxStateVacuumizer {
    private static final IgniteLogger LOG = Loggers.forClass(PersistentTxStateVacuumizer.class);

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private final ReplicaService replicaService;

    private final ClusterNode localNode;

    private final ClockService clockService;

    private final PlacementDriver placementDriver;

    /**
     * Constructor.
     *
     * @param replicaService Replica service.
     * @param localNode Local node.
     * @param clockService Clock service.
     * @param placementDriver Placement driver.
     */
    public PersistentTxStateVacuumizer(
            ReplicaService replicaService,
            ClusterNode localNode,
            ClockService clockService,
            PlacementDriver placementDriver
    ) {
        this.replicaService = replicaService;
        this.localNode = localNode;
        this.clockService = clockService;
        this.placementDriver = placementDriver;
    }

    /**
     * Vacuum persistent tx states.
     *
     * @param txIds Transaction ids to vacuum; map of commit partition ids to sets of tx ids.
     * @return A future.
     */
    public CompletableFuture<IgniteBiTuple<Set<UUID>, Integer>> vacuumPersistentTxStates(Map<TablePartitionId, Set<UUID>> txIds) {
        Set<UUID> successful = ConcurrentHashMap.newKeySet();
        AtomicInteger unsuccessfulCount = new AtomicInteger(0);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        HybridTimestamp now = clockService.now();

        txIds.forEach((commitPartitionId, txs) -> {
            ReplicaMeta replicaMeta = placementDriver.getPrimaryReplica(commitPartitionId, now).join();

            if (replicaMeta != null) {
                VacuumTxStateReplicaRequest request = TX_MESSAGES_FACTORY.vacuumTxStateReplicaRequest()
                        .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                        .groupId(commitPartitionId)
                        .transactionIds(txs)
                        .build();

                CompletableFuture<?> future;

                if (localNode.id().equals(replicaMeta.getLeaseholderId())) {
                    future = replicaService.invoke(localNode, request).whenComplete((v, e) -> {
                        if (e == null) {
                            successful.addAll(txs);
                        } else if (!(unwrapCause(e) instanceof PrimaryReplicaMissException)) {
                            LOG.warn("Failed to vacuum tx states from the persistent storage.", e);

                            unsuccessfulCount.incrementAndGet();
                        }
                    });
                } else {
                    successful.addAll(txs);

                    future = nullCompletedFuture();
                }

                futures.add(future);
            }
        });

        return allOf(futures.toArray(new CompletableFuture[0]))
                .handle((unused, unusedEx) -> new IgniteBiTuple<>(successful, unsuccessfulCount.get()));
    }
}
