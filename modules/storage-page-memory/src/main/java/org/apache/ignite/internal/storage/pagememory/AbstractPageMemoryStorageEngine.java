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

package org.apache.ignite.internal.storage.pagememory;


import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.engine.StorageEngine;

/** Abstract implementation of the storage engine based on memory {@link PageMemory}. */
public abstract class AbstractPageMemoryStorageEngine implements StorageEngine {
    private final HybridClock clock;

    /** Constructor. */
    AbstractPageMemoryStorageEngine(HybridClock clock) {
        this.clock = clock;
    }

    /**
     * Creates a Global remove ID for structures based on a {@link BplusTree}, always creating monotonically increasing ones even after
     * recovery node, so that there are no errors after restoring trees.
     */
    public AtomicLong generateGlobalRemoveId() {
        return new AtomicLong(clock.nowLong());
    }
}