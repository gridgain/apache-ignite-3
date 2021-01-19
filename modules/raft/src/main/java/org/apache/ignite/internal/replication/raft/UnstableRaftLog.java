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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.slf4j.Logger;

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
public class UnstableRaftLog {
    private final Logger logger;

    private Snapshot snapshot;

    // TODO agoncharuk: we should probably replace entries with a circular buffer to avoid extra allocations.
    private List<Entry> entries;

    private long offset;

    // TODO: sanpwc Rework general structure of Tuple(Map.Entry) result.
    private static final Map.Entry<Long, Boolean> FALSE_RES = new AbstractMap.SimpleEntry<>(0L, false);

    public UnstableRaftLog(Logger logger, long offset) {
        this.logger = logger;
        this.offset = offset;
        this.entries = new ArrayList<>();
    }

    // maybeFirstIndex returns the index of the first possible entry in entries
    // if it has a snapshot.
    public Map.Entry<Long, Boolean> maybeFirstIndex() {
        if (snapshot != null)
            return new AbstractMap.SimpleEntry<>(snapshot.metadata().index() + 1, true);
        else
            return FALSE_RES;
    }

    // maybeLastIndex returns the last index if it has at least one
    // unstable entry or snapshot.
    public Map.Entry<Long, Boolean> maybeLastIndex() {
        if (!entries.isEmpty())
            return new AbstractMap.SimpleEntry<>(offset + entries.size() - 1, true);

        if (snapshot != null)
            return new AbstractMap.SimpleEntry<>(snapshot.metadata().index(), true);

        return FALSE_RES;
    }

    // maybeTerm returns the term of the entry at index i, if there
    // is any.
    public Map.Entry<Long, Boolean> maybeTerm(long idx) {
        if (idx < offset) {
            if (snapshot != null && snapshot.metadata().index() == idx)
                return new AbstractMap.SimpleEntry<>(snapshot.metadata().term(), true);

            return FALSE_RES;
        }

        Map.Entry<Long, Boolean> maybeLastIndex = maybeLastIndex();

        if (! maybeLastIndex.getValue())
            return FALSE_RES;

        if (idx > maybeLastIndex.getKey())
            return FALSE_RES;

        return new AbstractMap.SimpleEntry<>(entries.get((int) (idx - offset)).term(), true);
    }

    public void stableTo(long idx, long term) {
        Map.Entry<Long, Boolean> maybeTerm = maybeTerm(idx);
        if (!maybeTerm.getValue())
            return;

        // if i < offset, term is matched with the snapshot
        // only update the unstable entries if term is matched with
        // an unstable entry.
        if (maybeTerm.getKey() == term && idx >= offset) {
            entries = new ArrayList<>(entries.subList((int)(idx + 1 - offset), entries.size()));

            offset = idx + 1;

            shrinkEntriesArray();
        }
    }

    // shrinkEntriesArray discards the underlying array used by the entries slice
    // if most of it isn't being used. This avoids holding references to a bunch of
    // potentially large entries that aren't needed anymore. Simply clearing the
    // entries wouldn't be safe because clients might still be using them.
    public void shrinkEntriesArray() {
        if (entries.isEmpty())
            entries = new ArrayList<>();

        // TODO: sanpwc Implement.
    }

    public void stableSnapTo(long idx) {
        if (snapshot != null && snapshot.metadata().index() == idx)
            snapshot = null;
    }

    public void restore(Snapshot s) {
        offset = s.metadata().index() + 1;

        entries = new ArrayList<>();

        snapshot = s;
    }

    public void truncateAndAppend(List<Entry> entries) {
        long after = entries.get(0).index();

            if (after == offset + this.entries.size()) {
                // after is the next index in the this.entries
                // directly append
                this.entries.addAll(entries);
            }
            else if (after <= offset) {
                logger.info(
                    String.format(
                        "replace the unstable entries from index %d",
                        after
                    )
                );

                // The log is being truncated to before our current offset
                // portion, so set the offset and replace the entries
                offset = after;
                this.entries = new ArrayList<>(entries);
            } else {
                // truncate to after and copy to this.entries
                // then append
                logger.info(
                    String.format(
                        "truncate the unstable entries before index %d",
                        after
                    )
                );

                this.entries = new ArrayList<>(slice(offset,after));

                this.entries.addAll(entries);
            }
    }

    public List<Entry> slice(long low, long high) {
        mustCheckOutOfBounds(low, high);

        return entries.subList((int)(low - offset), (int)(high - offset));
    }

    // u.offset <= lo <= hi <= u.offset+len(u.entries)
    public void mustCheckOutOfBounds(long low,  long high) {
        if (low > high) {
            logger.error(
                String.format("invalid unstable.slice %d > %d",
                    low,
                    high
                )
            );
        }

        long upper = offset + entries.size();

        if (low < offset || high > upper) {
            logger.error(
                String.format(
                    "unstable.slice[%d,%d) out of bound [%d,%d]",
                    low,
                    high,
                    offset,
                    upper
                )
            );
        }
    }

    public long offset() {
        return offset;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public List<Entry> entries(){
        return entries;
    }
}
