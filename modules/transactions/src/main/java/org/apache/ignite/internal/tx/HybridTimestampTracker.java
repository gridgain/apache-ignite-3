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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Hybrid timestamp tracker.
 */
public class HybridTimestampTracker {
    /** Timestamp. */
    private volatile long timestamp = NULL_HYBRID_TIMESTAMP;

    /**
     * Var handle for {@link #timestamp}.
     */
    private static final AtomicLongFieldUpdater<HybridTimestampTracker> LATEST_TIME = AtomicLongFieldUpdater.newUpdater(
            HybridTimestampTracker.class,
            "timestamp"
    );

    /**
     * Get current tracked timestamp.
     *
     * @return Timestamp or {@code null} if the tracker has never updated.
     */
    public @Nullable HybridTimestamp get() {
        return nullableHybridTimestamp(timestamp);
    }

    /**
     * Updates the tracked timestamp if a provided timestamp is greater.
     *
     * @param ts Timestamp to use for update.
     */
    public void update(@Nullable HybridTimestamp ts) {
        long tsVal = hybridTimestampToLong(ts);

        while (true) {
            long oldTimestamp = this.timestamp;

            if (tsVal <= oldTimestamp) {
                return;
            }

            if (LATEST_TIME.compareAndSet(this, oldTimestamp, tsVal)) {
                return;
            }
        }
    }
}
