/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Metric registry. Metrics source (see {@link MetricSource} must be registered in this metrics registry after initialization
 * of corresponding component and must be unregistered in case of component is destroyed or stopped. Metrics registry also
 * provides access to all enabled metrics through corresponding metrics sets. Metrics registry lifetime is equal to the node lifetime.
 */
public class MetricRegistry {
    private static final AtomicReferenceFieldUpdater<MetricRegistry, IgniteBiTuple> metricSnapshotUpdater =
            newUpdater(MetricRegistry.class, IgniteBiTuple.class, "metricSnapshot");

    private final Lock lock = new ReentrantLock();

    /** Registered metric sources. */
    private final Map<String, MetricSource> sources = new HashMap<>();

    /** Enabled metric sets. */
    private final Map<String, MetricSet> sets = new TreeMap<>();

    /**
     * Metrics snapshot. This is a list of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     */
    private volatile IgniteBiTuple<List<MetricSet>, Long> metricSnapshot = new IgniteBiTuple<>(emptyList(), 0L);

    /**
     * Register metric source. It must be registered in this metrics registry after initialization of corresponding component
     * and must be unregistered in case of component is destroyed or stopped, see {@link #unregisterSource(MetricSource)}.
     * By registering, the metric source isn't enabled implicitly.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source with the given name already exists.
     */
    public void registerSource(MetricSource src) {
        lock.lock();

        try {
            // Metric source shouldn't be enabled before because the second call of MetricSource#enable will return null.
            assert !src.enabled() : "Metric source shouldn't be enabled before registration in registry.";

            MetricSource old = sources.putIfAbsent(src.name(), src);

            if (old != null) {
                throw new IllegalStateException("Metrics source with given name already exists: " + src.name());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param src Metric source.
     */
    public void unregisterSource(MetricSource src) {
        unregisterSource(src.name());
    }

    /**
     * Unregister metric source. It must be unregistered in case of corresponding component is destroyed or stopped.
     * Metric source is also disabled while unregistered, see {@link #disable(String)}.
     *
     * @param srcName Metric source name.
     */
    public void unregisterSource(String srcName) {
        lock.lock();

        try {
            MetricSource registered = sources.get(srcName);

            if (registered != null) {
                disable(registered);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param src Metric source.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public MetricSet enable(@NotNull MetricSource src) {
        lock.lock();

        try {
            MetricSource registered = checkAndGetRegistered(src);

            MetricSet metricSet = registered.enable();

            if (metricSet != null) {
                sets.put(src.name(), metricSet);

                updateMetricSnapshot();
            }

            return metricSet;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @return Metric set, or {@code null} if the metric set is already enabled.
     * @throws IllegalStateException If metric source with the given name doesn't exist.
     */
    public MetricSet enable(final String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exist: " + srcName);
            }

            MetricSet metricSet = src.enable();

            if (metricSet != null) {
                sets.put(src.name(), metricSet);

                updateMetricSnapshot();
            }

            return metricSet;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param src Metric source.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    public void disable(@NotNull MetricSource src) {
        lock.lock();

        try {
            MetricSource registered = checkAndGetRegistered(src);

            if (!registered.enabled()) {
                return;
            }

            registered.disable();

            sets.remove(registered.name());

            updateMetricSnapshot();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Disable metric set for the given metric source.
     *
     * @param srcName Metric source name.
     * @throws IllegalStateException If metric source with given name doesn't exists.
     */
    public void disable(final String srcName) {
        lock.lock();

        try {
            MetricSource src = sources.get(srcName);

            if (src == null) {
                throw new IllegalStateException("Metrics source with given name doesn't exists: " + srcName);
            }

            if (!src.enabled()) {
                return;
            }

            src.disable();

            sets.remove(srcName);

            updateMetricSnapshot();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check that the given metric source is registered. This method should be called under the {@link MetricRegistry#lock}.
     *
     * @param src Metric source.
     * @return Registered metric source.
     * @throws IllegalStateException If metric source isn't registered.
     * @throws IllegalArgumentException If metric source isn't the same as registered.
     */
    @NotNull
    private MetricSource checkAndGetRegistered(@NotNull MetricSource src) {
        requireNonNull(src);

        MetricSource registered = sources.get(src.name());

        if (registered == null) {
            throw new IllegalStateException("Metrics source isn't registered: " + src.name());
        }

        if (!src.equals(registered)) {
            throw new IllegalArgumentException("Given metric source is not the same as registered by the same name: " + src.name());
        }

        return registered;
    }

    /**
     * Update {@link MetricRegistry#metricSnapshot}, only metric sets from registered and enabled metric sources are included,
     * version is incremented.
     */
    private void updateMetricSnapshot() {
        List<MetricSet> metricSets = new ArrayList<>(sets.size());

        for (MetricSet metricSet : sets.values()) {
            metricSets.add(metricSet);
        }

        long newVersion = metricSnapshot.get2() + 1;

        IgniteBiTuple<List<MetricSet>, Long> newMetricSnapshot = new IgniteBiTuple<>(unmodifiableList(metricSets), newVersion);

        metricSnapshotUpdater.compareAndSet(this, this.metricSnapshot, newMetricSnapshot);
    }

    /**
     * Metrics snapshot. This is a list of metric sets with corresponding version, the values of the metrics in the
     * metric sets that are included into the snapshot, are changed dynamically.
     *
     * @return Metrics snapshot.
     */
    public IgniteBiTuple<List<MetricSet>, Long> metricSnapshot() {
        return metricSnapshot;
    }
}
