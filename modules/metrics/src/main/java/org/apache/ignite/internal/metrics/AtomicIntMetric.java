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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.jetbrains.annotations.Nullable;

/**
 * Integer metric is implemented as a volatile {@code int} value.
 */
public class AtomicIntMetric extends AbstractMetric implements IntMetric {
    /** Field updater. */
    private static final AtomicIntegerFieldUpdater<AtomicIntMetric> updater =
            AtomicIntegerFieldUpdater.newUpdater(AtomicIntMetric.class, "val");

    /** Value. */
    private volatile int val;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     */
    public AtomicIntMetric(String name, @Nullable String desc) {
        super(name, desc);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(int x) {
        updater.addAndGet(this, x);
    }

    /** Increment the metric. */
    public void increment() {
        updater.incrementAndGet(this);
    }

    /** Decrement the metric. */
    public void decrement() {
        updater.decrementAndGet(this);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(int val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return val;
    }
}
