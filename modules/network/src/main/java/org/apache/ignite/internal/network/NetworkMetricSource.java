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

package org.apache.ignite.internal.network;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetricSource} for global networking metrics.
 */
public class NetworkMetricSource implements MetricSource {
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    private final AtomicLongMetric sentMessages;

    private final AtomicLongMetric attemptedWrites;

    private final DistributionMetric attemptedWriteSize;
    private final DistributionMetric writeBuffers;
    private final DistributionMetric performedWriteSize;

    private final Map<String, Metric> metrics;

    /**
     * Constructor.
     */
    public NetworkMetricSource() {
        Map<String, Metric> map = new HashMap<>();

        sentMessages = new AtomicLongMetric("SentMessages", "Total number of messages enqueued for sending");
        attemptedWrites = new AtomicLongMetric("AttemptedWrites", "Total number of writes attempted via socket channels");

        attemptedWriteSize = new DistributionMetric(
                "AttemptedWriteSize",
                "Total number of bytes that are attempted to be written by SocketChannel.write() calls",
                new long[]{10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000}
        );
        writeBuffers = new DistributionMetric(
                "WriteBuffers",
                "Number of buffers that are attempted to be written by SocketChannel.write() calls",
                new long[]{1, 2, 3, 4, 5, 10, 100, 1000, 10_000}
        );
        performedWriteSize = new DistributionMetric(
                "PerformedWriteSize",
                "Number of bytes that are actually written by SocketChannel.write() calls",
                new long[]{10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000}
        );

        map.put("SentMessages", sentMessages);
        map.put("AttemptedWrites", attemptedWrites);

        map.put("AttemptedWriteSize", attemptedWriteSize);
        map.put("WriteBuffers", writeBuffers);
        map.put("PerformedWriteSize", performedWriteSize);

        metrics = Map.copyOf(map);
    }

    /**
     * Returns total number of messages enqueued for sending.
     */
    public AtomicLongMetric sentMessages() {
        return sentMessages;
    }

    /**
     * Returns total number of writes attempted via socket channels.
     */
    public AtomicLongMetric attemptedWrites() {
        return attemptedWrites;
    }

    /**
     * Returns metric for total amount of bytes attempted to be written to channel in one call.
     */
    public DistributionMetric attemptedWriteSize() {
        return attemptedWriteSize;
    }

    /**
     * Returns metric for number of buffers attempted to be written to channel in one call.
     */
    public DistributionMetric writeBuffers() {
        return writeBuffers;
    }

    /**
     * Returns metric for amount of bytes that was actually written to channel in one call.
     */
    public DistributionMetric performedWriteSize() {
        return performedWriteSize;
    }

    @Override
    public String name() {
        return "network";
    }

    @Override
    public @Nullable MetricSet enable() {
        if (enabled.compareAndSet(false, true)) {
            return new MetricSet(name(), metrics);
        }

        return null;
    }

    @Override
    public void disable() {
        enabled.set(false);
    }

    @Override
    public boolean enabled() {
        return enabled.get();
    }
}
