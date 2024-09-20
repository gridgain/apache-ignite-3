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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * A default implementation of {@link SchemaSyncService}.
 */
public class SchemaSyncServiceImpl implements SchemaSyncService {
    private final ClusterTime clusterTime;

    private final LongSupplier delayDurationMs;

    /**
     * Constructor.
     */
    public SchemaSyncServiceImpl(ClusterTime clusterTime, LongSupplier delayDurationMs) {
        this.clusterTime = clusterTime;
        this.delayDurationMs = delayDurationMs;
    }

    @Override
    public CompletableFuture<Void> waitForMetadataCompleteness(HybridTimestamp ts) {
        // There a corner case for tests that are using {@code WatchListenerInhibitor#metastorageEventsInhibitor}
        // that leads to current time equals {@link HybridTimestamp#MIN_VALUE} and this method is waiting forever.
//        if (HybridTimestamp.MIN_VALUE.equals(clusterTime.currentSafeTime())) {
//            return nullCompletedFuture();
//        }
//        return clusterTime.waitFor(ts.subtractPhysicalTime(delayDurationMs.getAsLong()));

        ts.subtractPhysicalTime2(delayDurationMs.getAsLong());

        return nullCompletedFuture();
    }
}
