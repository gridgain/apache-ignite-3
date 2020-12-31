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

package org.apache.ignite.internal.replication.raft.storage;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.ConfigState;
import org.apache.ignite.internal.replication.raft.HardState;
import org.apache.ignite.internal.replication.raft.InitialState;
import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.internal.replication.raft.SnapshotTemporarilyUnavailableException;

/**
 *
 */
public class MemoryStorage implements Storage {
    /** */
    private final UUID id;

    /** */
    private HardState hardState;

    /** */
    private ConfigState confState;

    /** */
    private List<Entry> entries;

    public MemoryStorage(
        UUID id,
        HardState hardState,
        ConfigState confState,
        List<Entry> entries
    ) {
        this.id = id;
        this.hardState = hardState;
        this.confState = confState;
        this.entries = entries;
    }

    /** {@inheritDoc} */
    @Override public InitialState initialState() {
        return new InitialState(id, hardState, confState);
    }

    /** {@inheritDoc} */
    @Override public List<Entry> entries(long lo, long hi, long maxSize) {
        return entries.subList((int)lo, (int)hi);
    }

    /** {@inheritDoc} */
    @Override public long term(long i) {
        return entries.get((int)i).term();
    }

    /** {@inheritDoc} */
    @Override public long lastIndex() {
        return entries.size();
    }

    /** {@inheritDoc} */
    @Override public long firstIndex() {
        return entries.size();
    }

    public void saveHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public void saveConfigState(ConfigState confState) {
        this.confState = confState;
    }

    /** {@inheritDoc} */
    @Override public Snapshot snapshot() throws SnapshotTemporarilyUnavailableException {
        throw new SnapshotTemporarilyUnavailableException("Snapshot not implemented for memory storage");
    }
}
