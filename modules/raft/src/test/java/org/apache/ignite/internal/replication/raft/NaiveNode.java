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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.TestMessageFactory;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.TestEntryFactory;

public class NaiveNode {
    private static final long TICK_INTERVAL = 100;

    private final List<Object> storage = new ArrayList<>();

    private final MemoryStorage raftStorage;

    private final BlockingQueue<Object> proposalQueue = new LinkedBlockingQueue<>();

    private final RawNode rawNode;

    public NaiveNode() {
        UUID nodeId = UUID.randomUUID();

        Random rnd = new Random(1L);

        raftStorage = new MemoryStorage(
            nodeId,
            new HardState(1, null, 0),
            ConfigState.bootstrap(Collections.singletonList(nodeId), Collections.emptyList()),
            Collections.emptyList(),
            new TestEntryFactory());

        rawNode = new RawNodeBuilder()
            .setMessageFactory(new TestMessageFactory())
            .setEntryFactory(new TestEntryFactory())
            .setStorage(raftStorage)
            .setRaftConfig(new RaftConfig()
                .electionTick(10)
                .heartbeatTick(1))
            .setRandom(rnd)
            .build();

        new Thread(this::serve).start();
    }

    // TODO sanpwc: add ability to process conf proposals, apply snapshots etc.
    private void serve() {
        long lastTick = System.currentTimeMillis();

        while (true) {
            Object userProposal = null;
            try {
                userProposal = proposalQueue.poll(TICK_INTERVAL, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                // TODO sanpwc: Panic!
                e.printStackTrace();
            }

            if (userProposal != null)
                rawNode.propose(userProposal);

            long now = System.currentTimeMillis();

            if (now - lastTick > TICK_INTERVAL) {
                rawNode.tick();
                lastTick = now;
            }

            if (rawNode.hasReady()) {
                Ready ready = rawNode.ready();

                // Persist hard state and entries.
                // TODO sanpwc: Check hardState update logic.
                if (ready.hardState() != null)
                    raftStorage.saveHardState(ready.hardState());

                if (!ready.entries().isEmpty())
                    raftStorage.append(ready.entries());

                // TODO sanpwc: In go impl there's both rc.wal.Save(rd.HardState, rd.Entries) and
                // TODO rc.raftStorage.Append(rd.Entries), see func (rc *raftNode) serveChannels()

                // Send messages.
                for (Message msg: ready.messages())
                    rawNode.send(msg);

                // TODO Should be async:
                // Apply committed entries.
                for (Entry entry: ready.committedEntries())
                    storage.add(entry.data());

                rawNode.advance(ready);
            }
        }
    }

    public void apply(Object userData) {
        proposalQueue.add(userData);
    }

    public List<Object> storage() {
        return storage;
    }

    public void becomeCandidate() {
        rawNode.becomeCandidate();
    }

    public void becomeLeader() {
        rawNode.becomeLeader();
    }
}
