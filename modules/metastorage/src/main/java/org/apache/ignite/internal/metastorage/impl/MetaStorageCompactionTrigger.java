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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Metastorage compaction trigger.
 *
 * <p>Algorithm:</p>
 * <ol>
 *     <li>Metastorage leader waits locally for the start of scheduled compaction.</li>
 *     <li>Metastorage leader locally calculates revision for compaction: it takes the current safe time and subtracts the data
 *     availability time and uses that timestamp to get the revision.</li>
 *     <li>If the revision is less than or equal to the last compacted revision, then go to point 6.</li>
 *     <li>Metastorage leader creates and sends a {@link CompactionCommand} (see the command description what each node will do) with a new
 *     revision for compaction.</li>
 *     <li>Metastorage leader locally gets notification of the completion of the local compaction for the new revision.</li>
 *     <li>Metastorage leader locally schedules a new start of compaction.</li>
 * </ol>
 */
// TODO: IGNITE-23280 Turn on compaction
public class MetaStorageCompactionTrigger implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageCompactionTrigger.class);

    private final String localNodeName;

    private final KeyValueStorage storage;

    private final MetaStorageManagerImpl metaStorageManager;

    private final ScheduledExecutorService scheduler;

    /** Guarded by {@link #lock}. */
    private @Nullable ScheduledFuture<?> lastScheduledFuture;

    /** Guarded by {@link #lock}. */
    private boolean isLocalNodeLeader;

    private final Lock lock = new ReentrantLock();

    private final PendingComparableValuesTracker<Long, Void> completeCompactionRevisionLocallyTracker
            = new PendingComparableValuesTracker<>(-1L);

    /** Compaction start interval (in milliseconds). */
    // TODO: IGNITE-23279 Change configuration
    private final long startInterval = IgniteSystemProperties.getLong("IGNITE_COMPACTION_START_INTERVAL", Long.MAX_VALUE);

    /** Data availability time (in milliseconds). */
    // TODO: IGNITE-23279 Change configuration
    private final long dataAvailabilityTime = IgniteSystemProperties.getLong("IGNITE_COMPACTION_DATA_AVAILABILITY_TIME", Long.MAX_VALUE);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param localNodeName Local node name.
     * @param storage Storage.
     * @param metaStorageManager Metastorage manager.
     * @param scheduler Scheduler to run trigger actions.
     */
    public MetaStorageCompactionTrigger(
            String localNodeName,
            KeyValueStorage storage,
            MetaStorageManagerImpl metaStorageManager,
            ScheduledExecutorService scheduler
    ) {
        this.localNodeName = localNodeName;
        this.storage = storage;
        this.metaStorageManager = metaStorageManager;
        this.scheduler = scheduler;

        storage.registerCompactionListener(
                compactionRevision -> inBusyLockSafe(busyLock, () -> onCompactionCompleteLocallyBusy(compactionRevision))
        );

        metaStorageManager.addElectionListener(newLeader -> inBusyLockSafe(busyLock, () -> onLeaderElectedBusy(newLeader)));
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        cancelLastScheduledFutureBusy();

        return nullCompletedFuture();
    }

    private void doCompactionBusy() {
        lock.lock();

        try {
            if (!isLocalNodeLeader) {
                return;
            }

            HybridTimestamp candidateCompactionRevisionTimestamp = createCandidateCompactionRevisionTimestampBusy();

            Long newCompactionRevision = calculateCandidateCompactionRevisionBusy(candidateCompactionRevisionTimestamp);

            if (newCompactionRevision == null) {
                scheduleNextCompactionBusy();
            } else {
                metaStorageManager
                        .sendCompactionCommand(newCompactionRevision)
                        .thenCompose(
                                unused -> inBusyLockAsync(busyLock, () -> awaitCompleteCompactionLocallyFutureBusy(newCompactionRevision))
                        )
                        .thenRun(() -> inBusyLock(busyLock, this::scheduleNextCompactionBusy))
                        .whenComplete((unused, throwable) -> {
                            if (throwable != null) {
                                Throwable cause = unwrapCause(throwable);

                                if (!(cause instanceof NodeStoppingException)) {
                                    LOG.error(
                                            "Unknown error on new metastorage compaction revision: ",
                                            cause,
                                            newCompactionRevision
                                    );

                                    inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
                                }
                            }
                        });
            }
        } catch (Throwable t) {
            LOG.error("Unknown error on new metastorage compaction revision", t);

            inBusyLockSafe(busyLock, this::scheduleNextCompactionBusy);
        } finally {
            lock.unlock();
        }
    }

    private HybridTimestamp createCandidateCompactionRevisionTimestampBusy() {
        HybridTimestamp safeTime = metaStorageManager.clusterTime().currentSafeTime();

        return safeTime.getPhysical() <= dataAvailabilityTime
                ? HybridTimestamp.MIN_VALUE
                : safeTime.subtractPhysicalTime(dataAvailabilityTime);
    }

    /** Returns {@code null} if there is no need to compact yet. */
    private @Nullable Long calculateCandidateCompactionRevisionBusy(HybridTimestamp candidateTimestamp) {
        try {
            long candidateCompactionRevision = storage.revisionByTimestamp(candidateTimestamp);
            long currentStorageRevision = storage.revision();

            if (candidateCompactionRevision >= currentStorageRevision) {
                candidateCompactionRevision = currentStorageRevision - 1;
            }

            return candidateCompactionRevision <= storage.getCompactionRevision() ? null : candidateCompactionRevision;
        } catch (CompactedException exception) {
            // Revision has already been compacted, we need to plan the next compaction.
            return null;
        }
    }

    private CompletableFuture<Void> awaitCompleteCompactionLocallyFutureBusy(long compactionRevision) {
        return completeCompactionRevisionLocallyTracker.waitFor(compactionRevision);
    }

    private void scheduleNextCompactionBusy() {
        lock.lock();

        try {
            if (isLocalNodeLeader) {
                lastScheduledFuture = scheduler.schedule(
                        () -> inBusyLock(busyLock, this::doCompactionBusy),
                        startInterval,
                        MILLISECONDS
                );
            }
        } finally {
            lock.unlock();
        }
    }

    private void onCompactionCompleteLocallyBusy(long compactionRevision) {
        completeCompactionRevisionLocallyTracker.update(compactionRevision, null);
    }

    private void onLeaderElectedBusy(ClusterNode newLeader) {
        lock.lock();

        try {
            if (localNodeName.equals(newLeader.name())) {
                isLocalNodeLeader = true;

                scheduleNextCompactionBusy();
            } else {
                isLocalNodeLeader = false;

                cancelLastScheduledFutureBusy();
            }
        } finally {
            lock.unlock();
        }
    }

    private void cancelLastScheduledFutureBusy() {
        lock.lock();

        try {
            ScheduledFuture<?> lastScheduledFuture = this.lastScheduledFuture;

            if (lastScheduledFuture != null) {
                lastScheduledFuture.cancel(true);
            }

            this.lastScheduledFuture = null;
        } finally {
            lock.unlock();
        }
    }
}
