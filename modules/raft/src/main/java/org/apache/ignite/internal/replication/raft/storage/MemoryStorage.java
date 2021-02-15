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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.replication.raft.CompactionException;
import org.apache.ignite.internal.replication.raft.ConfigState;
import org.apache.ignite.internal.replication.raft.HardState;
import org.apache.ignite.internal.replication.raft.InitialState;
import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.internal.replication.raft.SnapshotTemporarilyUnavailableException;
import org.apache.ignite.internal.replication.raft.UnavailabilityException;
import org.apache.ignite.internal.replication.raft.UnrecoverableException;
import org.apache.ignite.internal.replication.raft.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

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

    /** */
    private EntryFactory entryFactory;

    /** */
    private Lock memoryStorageLock = new ReentrantLock();

    /** */
    private Logger logger = LoggerFactory.getLogger(MemoryStorage.class);

    public MemoryStorage(
        UUID id,
        HardState hardState,
        ConfigState confState,
        List<Entry> entries,
        EntryFactory entryFactory
    ) {
        this.id = id;
        this.hardState = hardState;
        this.confState = confState;
        this.entries = new ArrayList<>(entries);
        this.entryFactory = entryFactory;

        // When starting from scratch populate the list with a dummy entry at term zero.
        // TODO sanpwc: Use dummy entry instead
        this.entries.add(new TestEntry(Entry.EntryType.ENTRY_DATA, 0,0,null));
    }

    /** {@inheritDoc} */
    @Override public InitialState initialState() {
        return new InitialState(id, hardState, confState);
    }

    /** {@inheritDoc} */
    @Override public List<Entry> entries(long lo, long hi, long maxSize)
        throws CompactionException, UnavailabilityException {
        memoryStorageLock.lock();

        try {
            long off = entries.get(0).index();

            if (lo <= off)
                throw new CompactionException();

            if (hi > lastIndex() + 1) {
                unrecoverable(
                    "entries' hi(%d) is out of bound lastindex(%d)",
                    hi,
                    lastIndex()
                );
            }

            if (entries.size() == 1)
                throw new UnavailabilityException();

            return Utils.limitSize(entryFactory, entries.subList((int) (lo - off), (int) (hi - off)), maxSize);
        }
        finally {
            memoryStorageLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long term(long i) throws CompactionException, UnavailabilityException {
        memoryStorageLock.lock();

        try {
            long offset = entries.get(0).index();

            if (i < offset)
                throw new CompactionException();

            if (i - offset >= entries.size())
                throw new UnavailabilityException();

            return entries.get((int) (i - offset)).term();
        }
        finally {
            memoryStorageLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastIndex() {
        memoryStorageLock.lock();

        try {
            return entries.get(0).index() + entries.size() -1;
        }
        finally {
            memoryStorageLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long firstIndex() {
        memoryStorageLock.lock();

        try {
            return entries.get(0).index() + 1;
        }
        finally {
            memoryStorageLock.unlock();
        }
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

    public void append(List<Entry> entries) {
        if (entries.isEmpty())
            return;

        memoryStorageLock.lock();

        try {
            long fist = firstIndex();

            long last = entries.get(0).index() + entries.size() - 1;

            // shortcut if there is no new entry.
            if (last < fist)
                return;

            // truncate compacted entries
            if (fist > entries.get(0).index())
                entries = entries.subList((int)(fist - entries.get(0).index()), entries.size());

            long offset = entries.get(0).index() - this.entries.get(0).index();

            if (this.entries.size() > offset) {
                this.entries = this.entries.subList(0, (int)offset);

                this.entries.addAll(entries);
            }
            else if (this.entries.size() == offset)
                this.entries.addAll(entries);
            else {
                unrecoverable(
                    "missing log entry [last: {}, append at: {}]",
                    lastIndex(),
                    entries.get(0).index()
                );
            }
        }
        finally {
            memoryStorageLock.unlock();
        }
    }

    // TODO: sanpwc Duplicates org.apache.ignite.internal.replication.raft.RawNode.unrecoverable
    private void unrecoverable(String formatMsg, Object... args) {
        logger.error(formatMsg, args);

        throw new UnrecoverableException(MessageFormatter.arrayFormat(formatMsg, args).getMessage());
    }
}
