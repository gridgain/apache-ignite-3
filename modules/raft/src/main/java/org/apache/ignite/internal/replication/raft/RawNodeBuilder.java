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

import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.message.MessageFactory;
import org.apache.ignite.internal.replication.raft.storage.EntryFactory;
import org.apache.ignite.internal.replication.raft.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RawNodeBuilder {
    /** Raft config. */
    private RaftConfig cfg = new RaftConfig();

    /** Message factory to use. */
    private MessageFactory msgFactory;

    /** */
    private EntryFactory entryFactory;

    /**
     * {@code storage} is the storage for Raft group. Raft generates entries and states to be
     * stored in storage. Raft reads the persisted entries and states out of storage when it needs.
     * Raft reads out the previous state and configuration out of storage when restarting.
     */
    private Storage storage;

    /**
     * {@code applied} is the last applied index. It should only be set when restarting Raft.
     * Raft will not return entries to the application smaller or equal to {@code applied}.
     * If {@code applied} is unset when restarting, raft might return previous applied entries.
     * This is an application dependent configuration.
     */
    private long applied;

    /** */
    private Random rnd = new Random();

    /** */
    private Logger logger = LoggerFactory.getLogger(RawNode.class);

    public RawNodeBuilder setRaftConfig(RaftConfig cfg) {
        this.cfg = cfg;

        return this;
    }

    public RawNodeBuilder setStorage(Storage storage) {
        this.storage = storage;
        return this;
    }

    public RawNodeBuilder setMessageFactory(MessageFactory msgFactory) {
        this.msgFactory = msgFactory;

        return this;
    }

    public RawNodeBuilder setEntryFactory(EntryFactory entryFactory) {
        this.entryFactory = entryFactory;

        return this;
    }

    public RawNodeBuilder setRandom(Random rnd) {
        this.rnd = rnd;

        return this;
    }

    public RawNodeBuilder setLogger(Logger logger) {
        this.logger = logger;

        return this;
    }

    public RawNodeBuilder setApplied(long applied) {
        this.applied = applied;
        return this;
    }

    public <T> RawNode<T> build() {
        validateConfig(cfg);

        InitialState initState = storage.initialState();
        UUID id = initState.id();

        RaftLog raftLog = new RaftLog(logger, storage, entryFactory, cfg.maxCommittedSizePerReady());

        // TODO agoncharuk: move to log constructor.
        if (applied > 0)
            raftLog.appliedTo(applied);
        raftLog.commitTo(initState.hardState().committed());

        RawNode<T> r = new RawNode<T>(
            id,
            raftLog,
            cfg.maxSizePerMsg(),
            cfg.maxUncommittedEntriesSize(),
            cfg.maxInflightMsgs(),
            cfg.electionTick(),
            cfg.heartbeatTick(),
            cfg.checkQuorum(),
            cfg.preVote(),
            cfg.readOnlyOption(),
            msgFactory,
            entryFactory,
            logger,
            rnd);

        r.initFromState(initState);

        return r;
    }

    private static void validateConfig(RaftConfig cfg) {
        if (cfg.heartbeatTick() <= 0)
            throw new IllegalArgumentException("heartbeatTick must be positive");

        if (cfg.electionTick() <= cfg.heartbeatTick())
            throw new IllegalArgumentException("electionTick must be greater than heartbeatTick");

        if (cfg.maxUncommittedEntriesSize() == 0)
            cfg.maxUncommittedEntriesSize(Long.MAX_VALUE);

        if (cfg.maxCommittedSizePerReady() == 0)
            cfg.maxCommittedSizePerReady(Integer.MAX_VALUE);

        if (cfg.maxInflightMsgs() <= 0)
            throw new IllegalArgumentException("maxInflightMessages must be positive");

        if (cfg.readOnlyOption() == ReadOnlyOption.READ_ONLY_LEASE_BASED && !cfg.checkQuorum())
            throw new IllegalArgumentException("checkQuorum must be enabled when ReadOnlyOption is " +
                "READ_ONLY_LEASE_BASED");
    }
}
