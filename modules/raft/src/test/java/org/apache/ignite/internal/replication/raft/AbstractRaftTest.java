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

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.message.MessageFactory;
import org.apache.ignite.internal.replication.raft.message.TestMessageFactory;
import org.apache.ignite.internal.replication.raft.storage.EntryFactory;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.TestEntryFactory;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class AbstractRaftTest {
    protected final MessageFactory msgFactory = new TestMessageFactory();
    protected final EntryFactory entryFactory = new TestEntryFactory();
    protected final UUID[] ids = idsBySize(3);

    protected static UUID[] idsBySize(int size) {
        UUID[] ret = new UUID[size];

        for (int i = 0; i < size; i++)
            ret[i] = UUID.randomUUID();

        Arrays.sort(ret);

        return ret;
    }

    protected <T> RawNode<T> newTestRaft(UUID[] peers, int election, int heartbeat) {
        return newTestRaft(election, heartbeat, memoryStorage(peers, new HardState(1, null, 0)));
    }

    protected <T> RawNode<T> newTestRaft(int election, int heartbeat, MemoryStorage memStorage) {
        return newTestRaft(new RaftConfig()
            .electionTick(election)
            .heartbeatTick(heartbeat)
            .maxSizePerMsg(Integer.MAX_VALUE),
            memStorage);
    }

    protected <T> RawNode<T> newTestRaft(int election, int heartbeat, MemoryStorage memStorage, Random rnd) {
        RaftConfig cfg = new RaftConfig()
            .electionTick(election)
            .heartbeatTick(heartbeat)
            .maxSizePerMsg(Integer.MAX_VALUE);

        return newTestRaft(cfg, memStorage, rnd);
    }

    protected <T> RawNode<T> newTestRaft(RaftConfig cfg, MemoryStorage memStorage) {
        long seed = System.currentTimeMillis();

        LoggerFactory.getLogger(getClass().getName()).info("Using seed: {};//", seed);

        return newTestRaft(cfg, memStorage, new Random(seed));
    }

    protected <T> RawNode<T> newTestRaft(RaftConfig cfg, MemoryStorage memStorage, Random rnd) {
        return new RawNodeBuilder()
            .setMessageFactory(msgFactory)
            .setEntryFactory(entryFactory)
            .setStorage(memStorage)
            .setRaftConfig(cfg)
            .setRandom(rnd)
            .build();
    }

    protected MemoryStorage memoryStorage() {
        return memoryStorage(ids);
    }

    protected MemoryStorage memoryStorage(UUID[] peers) {
        return memoryStorage(peers, new HardState(1, null, 0));
    }

    protected MemoryStorage memoryStorage(UUID[] peers, HardState hs) {
        return memoryStorage(peers[0], peers, hs);
    }

    protected MemoryStorage memoryStorage(UUID locId, UUID[] peers, HardState hs) {
        return new MemoryStorage(
            locId,
            hs,
            ConfigState.bootstrap(Arrays.asList(peers), Collections.emptyList()),
            Collections.emptyList(),
            entryFactory
        );
    }

}
