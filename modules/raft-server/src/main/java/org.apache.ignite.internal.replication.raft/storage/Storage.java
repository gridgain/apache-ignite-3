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
import org.apache.ignite.internal.replication.raft.CompactionException;
import org.apache.ignite.internal.replication.raft.InitialState;
import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.internal.replication.raft.SnapshotTemporarilyUnavailableException;
import org.apache.ignite.internal.replication.raft.UnavailabilityException;

/**
 *
 */
public interface Storage {
    // TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

    // InitialState returns the saved HardState and ConfState information.
    InitialState initialState();

    /**
     * Entries returns a list of log entries in the range [lo, hi).
     *
     * @param lo Low index, inclusive.
     * @param hi High index, exclusive.
     * @param maxSize limits the total size of the log entries returned, but this method will return
     *      at least one entry if any.
     * @return
     */
    List<Entry> entries(long lo, long hi, long maxSize) throws CompactionException, UnavailabilityException;

    // Term returns the term of entry i, which must be in the range
    // [firstIndex() - 1, lastIndex()]. The term of the entry before
    // firstIndex() is retained for matching purposes even though the
    // rest of that entry may not be available.
    long term(long i) throws CompactionException, UnavailabilityException;

    // lastIndex returns the index of the last entry in the log.
    long lastIndex();

    // firstIndex returns the index of the first log entry that is
    // possibly available via entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    long firstIndex();

    /**
     * Snapshot returns the most recent snapshot.
     * If snapshot is temporarily unavailable, it should throw {@link SnapshotTemporarilyUnavailableException}
     * so raft state machine could know that Storage needs some time to prepare
     * snapshot and call Snapshot later.
     */
    Snapshot snapshot() throws SnapshotTemporarilyUnavailableException;
}
