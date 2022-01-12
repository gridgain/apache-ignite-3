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

package org.apache.ignite.raft.jraft.util;

import org.apache.ignite.internal.tostring.S;

/**
 * Timeout generation strategy.
 * Increases provided timeout based on exponential backoff algorithm. Max timeout equals to {@link TIMEOUT_MS_MAX}
 */
public class ExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DEFAULT_BACKOFF_COEFFICIENT = 2.0;

    /** Max timeout that strategy could generate, ms. */
    private static final int TIMEOUT_MS_MAX = 11_000;

    /** {@inheritDoc} */
    @Override
    public int nextTimeout(int currentTimeout) {
        return backoffTimeout(currentTimeout, TIMEOUT_MS_MAX);
    }

    /**
     * @param timeout Timeout.
     * @param maxTimeout Maximum timeout for backoff function.
     * @return Next exponential backoff timeout.
     */
    public static int backoffTimeout(int timeout, int maxTimeout) {
        return (int) Math.min(timeout * DEFAULT_BACKOFF_COEFFICIENT, maxTimeout);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ExponentialBackoffTimeoutStrategy.class, this);
    }
}
