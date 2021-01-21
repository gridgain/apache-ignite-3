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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.internal.replication.raft.StateType.*;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_FOLLOWER;

/**
 *
 */
public class RawNodeTest extends AbstractRaftTest {
    @Test
    public void testProgressLeader() {
        RawNode<String> r = newTestRaft(ids, 5, 1);

        r.becomeCandidate();
        r.becomeLeader();

        r.tracker().progress(ids[1]).becomeReplicate();

        // Send proposals to r1. The first 5 entries should be appended to the log.
        for (int i = 0; i < 5; i++) {
            Progress pr = r.tracker().progress(r.basicStatus().id());
            Assertions.assertEquals(Progress.ProgressState.StateReplicate, pr.state());
            Assertions.assertEquals(i + 1, pr.match());
            Assertions.assertEquals(pr.match() + 1, pr.next());

            r.propose("foo");
        }
    }

    // ensures heartbeat reset progress.paused by heartbeat response.
    @Test
    public void testProgressResumeByHeartbeatResp() {
        RawNode<String> r = newTestRaft(ids, 5, 1);

        r.becomeCandidate();
        r.becomeLeader();

        r.tracker().progress(ids[1]).probeSent(true);

        r.bcastHeartbeat();

        Assertions.assertTrue(r.tracker().progress(ids[1]).probeSent());

        r.tracker().progress(ids[1]).becomeReplicate();

        r.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], r.basicStatus().hardState().term(), null));
        Assertions.assertFalse(r.tracker().progress(ids[1]).probeSent());
    }

    @Test
    public void testProgressPaused() {
        UUID[] ids = idsBySize(2);

        RawNode r = newTestRaft(ids, 5, 1);

        r.becomeCandidate();
        r.becomeLeader();

        r.propose("somedata");
        r.propose("somedata");
        r.propose("somedata");

        Assertions.assertEquals(1, r.readMessages().size());
    }

    @Test
    public void testProgressFlowControl() {
        RaftConfig cfg = new RaftConfig().electionTick(5).heartbeatTick(1).maxInflightMsgs(3).maxSizePerMsg(2048);

        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(cfg, memoryStorage(ids));

        r.becomeCandidate();
        r.becomeLeader();

        long term = r.basicStatus().hardState().term();

        // Throw away all the messages relating to the initial election.
        r.readMessages();

        // While node 2 is in probe state, propose a bunch of entries.
        r.tracker().progress(ids[1]).becomeProbe();

        for (int i = 0; i < 10; i++)
            r.propose("a".repeat(1000));

        List<Message> ms = r.readMessages();

        // First append has two entries: the empty entry to confirm the
        // election, and the first proposal (only one proposal gets sent
        // because we're in probe state).
        Assertions.assertEquals(1, ms.size());
        Assertions.assertEquals(MessageType.MsgApp, ms.get(0).type());

        AppendEntriesRequest req = (AppendEntriesRequest)ms.get(0);

        List<Entry> entries = req.entries();
        Assertions.assertEquals(2, entries.size());
        Assertions.assertEquals(Entry.EntryType.ENTRY_DATA, entries.get(0).type());
        Assertions.assertNull(((UserData<String>)entries.get(0).data()).data());
        Assertions.assertEquals(Entry.EntryType.ENTRY_DATA, entries.get(1).type());
        Assertions.assertEquals(1000, ((UserData<String>)entries.get(1).data()).data().length());

        // When this append is acked, we change to replicate state and can
        // send multiple messages at once.
        r.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], term, entries.get(1).index(), false, 0));

        ms = r.readMessages();

        Assertions.assertEquals(3, ms.size());

        for (Message m : ms) {
            Assertions.assertEquals(MessageType.MsgApp, m.type());
            req = (AppendEntriesRequest)m;

            Assertions.assertEquals(2, req.entries().size());

            entries = req.entries();
        }

        // Ack all three of those messages together and get the last two
        // messages (containing three entries).
        r.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], term, entries.get(1).index(), false, 0));
        ms = r.readMessages();

        Assertions.assertEquals(2, ms.size());
        for (Message m : ms)
            Assertions.assertEquals(MessageType.MsgApp, m.type());

        req = (AppendEntriesRequest)ms.get(0);
        Assertions.assertEquals(2, req.entries().size());

        req = (AppendEntriesRequest)ms.get(1);

        Assertions.assertEquals(1, req.entries().size());
    }

    @Test
    public void testUncommittedEntryLimit() {
        // Use a relatively large number of entries here to prevent regression of a
        // bug which computed the size before it was fixed. This test would fail
        // with the bug, either because we'd get dropped proposals earlier than we
        // expect them, or because the final tally ends up nonzero. (At the time of
        // writing, the former).
	    final int maxEntries = 1024;

        String testData = "testdata";

        Entry entry = entryFactory.newEntry(1, 1, new UserData<>(testData));
        long maxEntrySize = maxEntries * entryFactory.payloadSize(entry);

        Assertions.assertEquals(0, entryFactory.payloadSize(entryFactory.newEntry(1, 1, new UserData<>(null))));

        RaftConfig cfg = new RaftConfig()
            .electionTick(5)
            .heartbeatTick(1)
            .maxUncommittedEntriesSize(maxEntrySize)
            .maxInflightMsgs(2 * 1024); // avoid interference

        RawNode<String> r = newTestRaft(cfg, memoryStorage());

        r.becomeCandidate();
        r.becomeLeader();

        long term = r.basicStatus().hardState().term();

        // Set the two followers to the replicate state. Commit to tail of log.
	    final int numFollowers = 2;
        r.tracker().progress(ids[1]).becomeReplicate();
        r.tracker().progress(ids[2]).becomeReplicate();

        Assertions.assertEquals(0, r.uncommittedSize());

        // Send proposals to r1. The first N entries should be appended to the log.
        List<Entry> propEnts = new ArrayList<>();

        for (int i = 0; i < maxEntries; i++) {
            ProposeReceipt receipt = r.propose(testData);

            propEnts.add(entryFactory.newEntry(term, receipt.startIndex(), new UserData<>(testData)));
        }

        // Send one more proposal to r1. It should be rejected.
        Assertions.assertThrows(ProposalDroppedException.class, () -> {
            r.propose(testData);
        });

        // Read messages and reduce the uncommitted size as if we had committed
        // these entries.
        List<Message> ms = r.readMessages();

        Assertions.assertEquals(maxEntries * numFollowers, ms.size());
        r.reduceUncommittedSize(propEnts);

        Assertions.assertEquals(0, r.uncommittedSize());

        // Send a single large proposal to r1. Should be accepted even though it
        // pushes us above the limit because we were beneath it before the proposal.
        propEnts = new ArrayList<>();
        List<String> largeData = new ArrayList<>();

        long start = r.raftLog().lastIndex();

        for (int i = 0; i < maxEntries * 2; i++) {
            largeData.add(testData);

            propEnts.add(entryFactory.newEntry(term, start + i + 1, new UserData<>(testData)));
        }

        r.propose(largeData);

        // Send one more proposal to r1. It should be rejected, again.
        Assertions.assertThrows(ProposalDroppedException.class, () -> {
            r.propose(testData);
        });

        // But we can always append an entry with no Data. This is used both for the
        // leader's first empty entry and for auto-transitioning out of joint config
        // states.
        r.propose((String)null);

        // Read messages and reduce the uncommitted size as if we had committed
        // these entries.
        ms = r.readMessages();

        Assertions.assertEquals(2 * numFollowers, ms.size());

        r.reduceUncommittedSize(propEnts);
        Assertions.assertEquals(0, r.uncommittedSize());
    }

    @Test
    public void testLeaderElection() {
        checkLeaderElection(false);
    }

    @Test
    public void testLeaderElectionPreVote() {
        checkLeaderElection(true);
    }

    public void checkLeaderElection(boolean preVote) {
        Function<RaftConfig, RaftConfig> cfgFunc = null;

        StateType candState = STATE_CANDIDATE;
        long candTerm = 2;

        if (preVote) {
            cfgFunc = (cfg) -> cfg.preVote(true);

            // In pre-vote mode, an election that fails to complete
            // leaves the node in pre-candidate state without advancing
            // the term.
            candState = STATE_PRE_CANDIDATE;
            candTerm = 1;
        }

        class TestData {
            Network n;
            StateType state;
            long expTerm;

            TestData(Network n, StateType state, long expTerm) {
                this.n = n;
                this.state = state;
                this.expTerm = expTerm;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(newNetwork(cfgFunc, entries(), entries(), entries()), STATE_LEADER, 2),
            new TestData(newNetwork(cfgFunc, entries(), entries(), null), STATE_LEADER, 2),
            new TestData(newNetwork(cfgFunc, entries(), null, null), candState, candTerm),
            new TestData(newNetwork(cfgFunc, entries(), null, null, entries()), candState, candTerm),
            new TestData(newNetwork(cfgFunc, entries(), null, null, entries(), entries()), STATE_LEADER, 2),

            // three logs further along than 0, but in the same term so rejections
            // are returned instead of the votes being ignored.
            new TestData(newNetwork(cfgFunc, entries(), entries(1), entries(1), entries(1, 1), entries()), STATE_FOLLOWER, candTerm),
            new TestData(newNetwork(cfgFunc, entries(), entries(2), entries(2), entries(2, 2), entries()), STATE_FOLLOWER, 2)
        };

        for (TestData tt : tests) {
            UUID[] ids = tt.n.ids();

            tt.n.<RawNodeStepper>action(ids[0], (s) -> s.node().campaign());

            RawNodeStepper s = tt.n.peer(ids[0]);

            Assertions.assertEquals(tt.state, s.node().basicStatus().softState().state());
            Assertions.assertEquals(tt.expTerm, s.node().basicStatus().hardState().term());
        }
    }

    protected Network newNetwork(Function<RaftConfig, RaftConfig> cfgFunc, Entry[]... startEntries) {
        UUID[] ids = idsBySize(startEntries.length);
        Stepper[] steppers = new Stepper[startEntries.length];

        long seed = System.currentTimeMillis();

        LoggerFactory.getLogger(getClass().getName()).info("Using seed: {};//", seed);

        for (int i = 0; i < startEntries.length; i++) {
            Entry[] entries = startEntries[i];

            if (entries == null)
                // A black hole.
                steppers[i] = Stepper.blackHole(ids[i]);
            else {
                MemoryStorage memStorage = memoryStorage(ids[i], ids,
                    new HardState(entries.length > 0 ? entries[entries.length - 1].term() : 1, null, 0));

                memStorage.append(Arrays.asList(entries));

                RaftConfig cfg = new RaftConfig().electionTick(10).heartbeatTick(1);

                if (cfgFunc != null)
                    cfg = cfgFunc.apply(cfg);

                RawNode<?> node = newTestRaft(
                    cfg,
                    memStorage,
                    new Random(seed + i));

                steppers[i] = new RawNodeStepper(node);
            }
        }

        return new Network(steppers);
    }

    private Entry[] entries(long... terms) {
        Entry[] res = new Entry[terms.length];

        for (int i = 0; i < terms.length; i++)
            res[i] = entryFactory.newEntry(terms[i], i + 1, new UserData<>("test-" + (i + 1)));

        return res;
    }
}
