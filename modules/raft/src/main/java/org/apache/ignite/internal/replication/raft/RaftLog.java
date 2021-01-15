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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 *
 */
// TODO: sanpwc According to current Storage logic it's not possible to get
// TODO: either ErrCompacted or ErrUnavailable, so that corresponding code was removed.
public class RaftLog {
    private final Logger logger;

    // storage contains all stable entries since the last snapshot.
    private final Storage storage;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    private final UnstableRaftLog unstable;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    private long committed;

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    private long applied;

    // maxNextEntsSize is the maximum number aggregate byte size of the messages
    // returned from calls to nextEnts.
    private final long maxNextEntriesSize;

    // newLog returns log using the given storage and default options. It
    // recovers the log to the state that it just commits and applies the
    // latest snapshot.
    public RaftLog(Logger logger, Storage storage) {
        this(logger, storage, Long.MAX_VALUE);
    }

    // newLogWithSize returns a log using the given storage and max
    // message size.
    public RaftLog(Logger logger, Storage storage, long maxNextEntriesSize) {
        if (storage == null)
            throw new UnrecoverableException("storage must not be null");

        this.logger = logger;

        this.storage = storage;

        this.maxNextEntriesSize = maxNextEntriesSize;

        this.unstable = new UnstableRaftLog(
            logger,
            storage.lastIndex() + 1);

        this.committed = storage.firstIndex() - 1;

        this.applied = storage.firstIndex() - 1;
    }

    @Override public String toString() {
        return String.format(
            "committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
            committed,
            applied,
            unstable.offset(),
            unstable.entries() != null ? unstable.entries().size() : 0
        );
    }

    public long applied() {
        return applied;
    }

    public void appliedTo(long newApplied) {
        if (newApplied == 0)
            return;

        if (committed < newApplied || newApplied < applied) {
            unrecoverable(
                "applied({}) is out of range [prevApplied(%d), committed({})]",
                newApplied,
                applied,
                committed
            );
        }

        applied = newApplied;
    }

    public long lastIndex() {
        // TODO: sanpwc Rework without Map.Entry
        Map.Entry<Long, Boolean> maybeLastIndex = unstable.maybeLastIndex();

        if (maybeLastIndex.getValue())
            return maybeLastIndex.getKey();

        try {
            return storage.lastIndex();
        }
        catch (Exception e) {
            unrecoverable("TODO(bdarnell)", e);

            return -1;
        }
    }

    public void stableTo(long idx, long term) {
        unstable.stableTo(idx, term);
    }

    public void stableSnapshotTo(long idx) {
        unstable.stableSnapTo(idx);
    }

    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    public boolean isUpToDate(long idx, long term) {
        return term > lastTerm() || (term == lastTerm() && idx >= lastIndex());
    }

    public long lastTerm() {
        try {
            return term(lastIndex());
        }
        catch (Exception e) {
            unrecoverable("unexpected error when getting the last term", e);

            return -1;
        }
    }

    public Snapshot unstableSnapshot() {
        return unstable.snapshot();
    }

    public Snapshot snapshot() throws SnapshotTemporarilyUnavailableException {
        if (unstable.snapshot() != null)
            return unstable.snapshot();

        return storage.snapshot();
    }

    public long committed() {
        return committed;
    }

    public void commitTo(long idx) {
        // never decrease commit
        if (committed < idx) {
            if (lastIndex() < idx) {
                unrecoverable(
                    "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
                    idx,
                    lastIndex()
                );
            }

            committed = idx;
        }
    }

    public long append(List<Entry> entries) {
        if (entries.size() == 0)
            return lastIndex();

        long afterIdx = entries.get(0).index() - 1;

        if (afterIdx < committed()) {
            unrecoverable(
                "after({}) is out of range [committed({})]",
                afterIdx,
                committed()
            );
        }

        unstable.truncateAndAppend(entries);

        return lastIndex();
    }

    public boolean maybeCommit(long maybeCommitIdx, long term) {
        if (maybeCommitIdx > committed && zeroTermOnErrCompacted(term(maybeCommitIdx)) == term) {
            commitTo(maybeCommitIdx);

            return true;
        }

        return false;
    }

    public long term(long idx) {
        // the valid term range is [index of dummy entry, last index]
        long dummyIndex = firstIndex() - 1;
        if (idx < dummyIndex || idx > lastIndex()) {
            // TODO: sanpwc original todo from etcd: "return an error instead?"
            return 0;
        }

        Map.Entry<Long, Boolean> maybeTerm = unstable.maybeTerm(idx);
        if (maybeTerm.getValue())
            return maybeTerm.getKey();

        try {
            return storage.term(idx);
        }
        catch (CompactionException | UnavailabilityException processedException) {
            // TODO agoncharuk: this method will return -1 instead of ErrCompacted.
            // TODO if error is not ErrCompacted or ErrUnavailable we should panic with // TODO(bdarnell)
            return -1;
        }
        catch (Exception e) {
            unrecoverable("TODO(bdarnell)");
            return -1;
        }
    }

    public boolean maybeAppend(long idx, long term, long committed, List<Entry> entries) {
        if (matchTerm(idx, term)) {
            long lastNewIdx = idx + entries.size();
            long conflictIdx = findConflict(entries);

            if (conflictIdx == 0) {
                // Do nothing
            } else if (conflictIdx <= committed()) {
                unrecoverable(
                    "entry {} conflict with committed entry [committed({})]",
                    conflictIdx,
                    committed()
                );
            }
            else {
                long off = idx + 1;
                append(entries.subList((int)(conflictIdx - off), entries.size()));
            }

            commitTo(Math.min(committed, lastNewIdx));

            return true;
        }

        return false;
    }

    // findConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    private long findConflict(List<Entry> entries) {
        for (Entry entry : entries) {
            if (!matchTerm(entry.index(), entry.term())) {
                if (entry.index() <= lastIndex()) {
                    logger.info(String.format("found conflict at index %d [existing term: %d, conflicting term: %d]",
                        entry.index(), zeroTermOnErrCompacted(term(entry.index())), entry.term()));
                }

                return entry.index();
            }
        }
        return 0;
    }

    public boolean matchTerm(long idx, long term) {
        try {
            return term(idx) == term;
        }
        catch (Exception e) {
            return false;
        }
    }

    // TODO: sanpwc Map.Entry<> rework required.
    public long firstIndex() {
        Map.Entry<Long, Boolean> maybeFirstIdx = unstable.maybeFirstIndex();

        if (maybeFirstIdx.getValue())
            return maybeFirstIdx.getKey();

        try {
            return storage.firstIndex();
        }
        catch (Exception e) {
            unrecoverable("TODO(bdarnell)", e);
            return -1;
        }
    }

    public long zeroTermOnErrCompacted(long term) {
        // TODO agoncharuk: provide more Java-style way to handle this method, see TODO for term(idx).
        return term > 0 ? term : 0;
    }

    public List<Entry> entries(long startIdx, long maxSize) throws CompactionException {
        if (startIdx > lastIndex())
            return Collections.emptyList();

        return slice(startIdx, lastIndex() + 1, maxSize);
    }

    // slice returns a slice of log entries from fromIdx through toIdx - 1, inclusive.
    public List<Entry> slice(long fromIdx, long toIdx, long maxSize) throws CompactionException {
        mustCheckOutOfBounds(fromIdx, toIdx);

        if (fromIdx == toIdx)
            return Collections.emptyList();

        List<Entry> entries = new ArrayList<>();

        if (fromIdx < unstable.offset()) {
            try {
                List<Entry> storedEntries = storage.entries(fromIdx, Math.min(toIdx, unstable.offset()), maxSize);

                // check if ents has reached the size limitation
                if (storedEntries.size() < Math.min(toIdx, unstable.offset() - fromIdx))
                    return storedEntries;

                entries = storedEntries;
            }
            catch (CompactionException compactionException) {
                throw compactionException;
            }
            catch (UnavailabilityException unavailabilityException) {
                unrecoverable(
                    "entries[{}:{}) is unavailable from storage",
                    fromIdx,
                    Math.min(toIdx, unstable.offset())
                );
            }
            catch (Error e) {
                unrecoverable("TODO(bdarnell)");
            }
        }

        if (toIdx > unstable.offset()) {
            List<Entry> unstableEntries = unstable.slice(Math.max(fromIdx, unstable.offset()), toIdx);
            if (unstableEntries.size() > 0)
                entries.addAll(unstableEntries);
            else
                entries = unstableEntries;
        }

        return Utils.limitSize(entries, maxSize);
    }

    // hasPendingSnapshot returns if there is pending snapshot waiting for applying.
    public boolean hasPendingSnapshot() {
        return unstable.snapshot() != null && !unstable.snapshot().isEmpty();
    }

    public void restore(Snapshot s) {
        logger.info(
            String.format(
                "log starts to restore snapshot [index: %d, term: %d]",
                s.metadata().index(),
                s.metadata().term()
            )
        );

        committed = s.metadata().index();

        unstable.restore(s);
    }

    public List<Entry> unstableEntries() {
        if (unstable.entries().size() == 0)
            return Collections.emptyList();
        else
            return unstable.entries();

    }

    // nextEnts returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    public List<Entry> nextEntries() {
        long off = Math.max(applied + 1, firstIndex());

        if (committed + 1 > off) {
            try {
                return slice(off, committed + 1, maxNextEntriesSize);
            }
            catch (Exception e) {
                unrecoverable("unexpected error when getting unapplied entries.", e);
            }
        }
        return Collections.emptyList();
    }

    // hasNextEnts returns if there is any available entries for execution. This
    // is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
    public boolean hasNextEntries() {
        long off = Math.max(applied + 1, firstIndex());
        return committed + 1 > off;
    }

    // allEntries returns all entries in the log.
    public List<Entry> allEntries() {
        try {
            return entries(firstIndex(), Long.MAX_VALUE);
        }
        catch (CompactionException compactionException) {
            return allEntries();
        }
        catch (Exception e) {
            unrecoverable("TODO (xiangli)", e);

            return Collections.emptyList();
        }
    }

    // TODO: sanpwc Duplicates org.apache.ignite.internal.replication.raft.RawNode.unrecoverable
    private void unrecoverable(String formatMsg, Object... args) {
        logger.error(formatMsg, args);

        throw new UnrecoverableException(MessageFormatter.arrayFormat(formatMsg, args).getMessage());
    }

    private void mustCheckOutOfBounds(long fromIdx, long toIdx) throws CompactionException {
        if (fromIdx < toIdx) {
            logger.error(
                String.format(
                    "invalid slice %d > %d",
                    fromIdx,
                    toIdx
                )
            );
        }

        long firstIdx = firstIndex();

        if (fromIdx < firstIdx)
            throw new CompactionException();

        long length = lastIndex() + 1 - firstIdx;

        if (toIdx > firstIdx + length) {
            logger.error(
                String.format(
                    "slice[%d,%d) out of bound [%d,%d]",
                    fromIdx,
                    toIdx,
                    firstIdx,
                    lastIndex()
                )
            );
        }
    }
}
