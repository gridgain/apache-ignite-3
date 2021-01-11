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

package org.apache.ignite.internal.replication.raft;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.replication.raft.storage.Entry;

/**
 *
 */
public class RaftLog {
    public long applied() {
        return 0;
    }

    public void appliedTo(long newApplied) {

    }

    public long lastIndex() {
        return 0;
    }

    public void stableTo(long idx, long term) {

    }

    public void stableSnapshotTo(long idx) {

    }

    public boolean isUpToDate(long idx, long term) {
        return false;
    }

    public long lastTerm() {
        return 0;
    }

    public Snapshot unstableSnapshot() {
        return null;
    }

    public Snapshot snapshot() throws SnapshotTemporarilyUnavailableException {
        return null;
    }

    public long committed() {
        return 0;
    }

    public void commitTo(long idx) {

    }

    public long append(List<Entry> entries) {
        return 0;
    }

    public boolean maybeCommit(long maybeCommitIdx, long term) {
        return false;
    }
    // TODO agoncharuk: this method will return -1 instead of ErrCompacted.

    public long term(long idx) {
        return 0;
    }

    public boolean maybeAppend(long idx, long term, long commit, List<Entry> entries) {
        return false;
    }

    public boolean matchTerm(long idx, long term) {
        return false;
    }

    public long firstIndex() {
        return 0;
    }

    public long zeroTermOnErrCompacted(long term) {
        // TODO agoncharuk: provide more Java-style way to handle this method, see TODO for term(idx).
        return term > 0 ? term : 0;
    }

    public List<Entry> entries(long startIdx, long maxSize) {
        return Collections.emptyList();
    }
    // slice returns a slice of log entries from fromIdx through toIdx - 1, inclusive.

    public Entry[] slice(long fromIdx, long toIdx, long maxSize) {
        return new Entry[0];
    }

    public boolean hasPendingSnapshot() {
        return false;
    }

    public void restore(Snapshot s) {

    }

    public List<Entry> unstableEntries() {
        return null;
    }

    public List<Entry> nextEntries() {
        return null;
    }

    public boolean hasNextEntries() {
        return false;
    }

    public List<Entry> allEntries() {
        return null;
    }
}
