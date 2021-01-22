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
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.replication.raft.message.MessageFactory;
import org.apache.ignite.internal.replication.raft.message.TestMessageFactory;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.EntryFactory;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.TestEntryFactory;
import org.apache.ignite.internal.replication.raft.storage.UserData;
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

    // nextEnts returns the appliable entries and updates the applied index
    protected List<Entry> nextEntries(RawNode<?> r, MemoryStorage s) {
        // Transfer all unstable entries to "stable" storage.
        s.append(r.raftLog().unstableEntries());
        r.raftLog().stableTo(r.raftLog().lastIndex(), r.raftLog().lastTerm());

        List<Entry> ents = r.raftLog().nextEntries();
        r.raftLog().appliedTo(r.raftLog().committed());

        return ents;
    }

    protected long seed() {
        long seed = System.currentTimeMillis();

        LoggerFactory.getLogger(getClass().getName()).info("Using seed: {};//", seed);

        return seed;
    }

    protected RaftConfig newTestConfig(int election, int hearbeat) {
        return new RaftConfig().electionTick(election).heartbeatTick(hearbeat).maxSizePerMsg(Integer.MAX_VALUE);
    }

    protected <T> RawNode<T> newTestRaft(UUID[] peers, int election, int heartbeat) {
        return newTestRaft(
            newTestConfig(election, heartbeat),
            memoryStorage(peers, new HardState(1, null, 0)));
    }

    protected <T> RawNode<T> newTestRaft(int election, int heartbeat, MemoryStorage memStorage) {
        return newTestRaft(
            newTestConfig(election, heartbeat),
            memStorage);
    }

    protected <T> RawNode<T> newTestRaft(int election, int heartbeat, MemoryStorage memStorage, Random rnd) {
        return newTestRaft(
            newTestConfig(election, heartbeat),
            memStorage, rnd);
    }

    protected <T> RawNode<T> newTestRaft(RaftConfig cfg, MemoryStorage memStorage) {
        return newTestRaft(cfg, memStorage, new Random(seed()));
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
        return memoryStorage(locId, peers, new UUID[0], hs);
    }

    protected MemoryStorage memoryStorage(
        UUID locId,
        UUID[] peers,
        UUID[] learners
    ) {
        return memoryStorage(locId, peers, learners, new HardState(1, null, 0));
    }

    protected MemoryStorage memoryStorage(
        UUID locId,
        UUID[] peers,
        UUID[] learners,
        HardState hs
    ) {
        return new MemoryStorage(
            locId,
            hs,
            ConfigState.bootstrap(Arrays.asList(peers), Arrays.asList(learners)),
            Collections.emptyList(),
            entryFactory
        );
    }

    protected Network newNetwork(Function<RaftConfig, RaftConfig> cfgFunc, NetworkBootstrap... bootstraps) {
        UUID[] ids = idsBySize(bootstraps.length);
        Stepper[] steppers = new Stepper[bootstraps.length];

        long seed = seed();

        for (int i = 0; i < bootstraps.length; i++) {
            NetworkBootstrap bootstrap = bootstraps[i];

            if (bootstrap == null)
                // A black hole.
                steppers[i] = Stepper.blackHole(ids[i]);
            else {
                Entry[] entries = bootstrap.entries();

                UUID vote = bootstrap.votedForIdx() < 0 ? null : ids[bootstrap.votedForIdx()];

                MemoryStorage memStorage = memoryStorage(ids[i], ids,
                    new HardState(bootstrap.term(), vote, 0));

                memStorage.append(Arrays.asList(entries));

                RaftConfig cfg = new RaftConfig().electionTick(10).heartbeatTick(1);

                if (cfgFunc != null)
                    cfg = cfgFunc.apply(cfg);

                RawNode<?> node = newTestRaft(
                    cfg,
                    memStorage,
                    new Random(seed + i));

                steppers[i] = new RawNodeStepper(node, memStorage);
            }
        }

        return new Network(new Random(seed), steppers);
    }

    protected NetworkBootstrap entries(long... terms) {
        Entry[] res = new Entry[terms.length];

        for (int i = 0; i < terms.length; i++)
            res[i] = entryFactory.newEntry(terms[i], i + 1, new UserData<>("test-" + (i + 1)));

        return new NetworkBootstrap(-1, terms.length == 0 ? 1 : terms[terms.length - 1], res);
    }

    protected NetworkBootstrap votedWithConfig(int votedForIdx, long term) {
        return new NetworkBootstrap(votedForIdx, term, new Entry[0]);
    }
}
