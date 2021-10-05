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

package org.apache.ignite;

import java.util.Set;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.ApiStatus.Experimental;

/**
 * Ignite node interface. Main entry-point for all Ignite APIs.
 */
public interface Ignite extends AutoCloseable {
    /**
     * Returns ignite node name.
     *
     * @return Ignite node name.
     */
    String name();

    /**
     * Gets an object for manipulate Ignite tables.
     *
     * @return Ignite tables.
     */
    IgniteTables tables();

    /**
     * Returns a transaction facade.
     *
     * @return Ignite transactions.
     */
    IgniteTransactions transactions();

    /**
     * Set new baseline nodes for table assignments.
     *
     * Current implementation has significant restrictions:
     * - Only current (known on this node) tables will be updated by this call.
     * - Only alive nodes can be a part of new baseline.
     * - Potentially it can be a long operation and current
     * synchronous changePeers-based implementation can't handle this issue well.
     * - No any recovery logic supported, if setBaseline fails - it can produce random state of cluster.
     *
     * TODO: this and other issues must be fixed under IGNITE-14209
     *
     * @param baselineNodes Names of baseline nodes.
     */
    @Experimental
    void setBaseline(Set<String> baselineNodes);
}
