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

import java.util.Map;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Read-only metrics registry.
 */
public class MetricProvider {
    /** Metrics registry. */
    private MetricRegistry metricRegistry;

    /**
     * Constructor.
     *
     * @param metricRegistry Metrics registry.
     */
    public MetricProvider(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    /**
     * Returns a map of (metricSetName -> metricSet) pairs with available metrics from {@link MetricRegistry}.
     *
     * @return map of metrics
     */
    public IgniteBiTuple<Map<String, MetricSet>, Long> metrics() {
        return metricRegistry.metricSnapshot();
    }
}
