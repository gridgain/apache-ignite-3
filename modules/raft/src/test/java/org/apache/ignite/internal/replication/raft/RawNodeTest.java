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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.replication.raft.message.HeartbeatRequest;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.message.VoteRequest;
import org.apache.ignite.internal.replication.raft.message.VoteResponse;
import org.apache.ignite.internal.replication.raft.storage.ConfChange;
import org.apache.ignite.internal.replication.raft.storage.ConfChangeJoint;
import org.apache.ignite.internal.replication.raft.storage.ConfChangeSingle;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.replication.raft.StateType.STATE_CANDIDATE;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_FOLLOWER;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_LEADER;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_PRE_CANDIDATE;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgApp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgAppResp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHeartbeat;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHeartbeatResp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgTimeoutNow;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgVoteResp;
import static org.apache.ignite.internal.replication.raft.storage.Entry.EntryType.ENTRY_CONF_CHANGE;
import static org.apache.ignite.internal.replication.raft.storage.Entry.EntryType.ENTRY_DATA;

/**
 * Intentionally skipped tests:
 * TestProposalByProxy() - we decided not to implement proposal forwarding for now.
 * TestRaftNodes - tests that TrackerConfig.voters() is sorted, but it's a TreeSet.
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

        r.sendHeartbeat();

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
        Assertions.assertEquals(MsgApp, ms.get(0).type());

        AppendEntriesRequest req = (AppendEntriesRequest)ms.get(0);

        List<Entry> entries = req.entries();
        Assertions.assertEquals(2, entries.size());
        Assertions.assertEquals(ENTRY_DATA, entries.get(0).type());
        Assertions.assertNull(entries.get(0).<UserData>data().data());
        Assertions.assertEquals(ENTRY_DATA, entries.get(1).type());
        Assertions.assertEquals(1000, ((UserData<String>)entries.get(1).data()).data().length());

        // When this append is acked, we change to replicate state and can
        // send multiple messages at once.
        r.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], term, entries.get(1).index(), false, 0));

        ms = r.readMessages();

        Assertions.assertEquals(3, ms.size());

        for (Message m : ms) {
            Assertions.assertEquals(MsgApp, m.type());
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
            Assertions.assertEquals(MsgApp, m.type());

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

            tt.n.<String>action(ids[0], (s) -> s.node().campaign());

            RawNodeStepper s = tt.n.peer(ids[0]);

            Assertions.assertEquals(tt.state, s.node().basicStatus().softState().state());
            Assertions.assertEquals(tt.expTerm, s.node().basicStatus().hardState().term());
        }
    }

    // TestLearnerElectionTimeout verfies that the leader should not start election even
    // when times out.
    @Test
    public void testLearnerElectionTimeout() {
        UUID[] ids = idsBySize(2);

        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1), memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]}));
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1), memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]}));

        n1.becomeFollower(1, null);
        n2.becomeFollower(1, null);

        // n2 is learner. Learner should not start election even when times out.
        n2.randomizedElectionTimeout(n2.electionTimeout());

        for (int i = 0; i < n2.electionTimeout(); i++)
            n2.tick();

        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
    }

    // TestLearnerPromotion verifies that the learner should not election until
    // it is promoted to a normal peer.
    @Test
    public void testLearnerPromotion() {
        UUID[] ids = idsBySize(2);

        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1), n1Storage, rnd);

        MemoryStorage n2Storage = memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1), n2Storage, rnd);

        n1.becomeFollower(1, null);
        n2.becomeFollower(1, null);

        Network nt = new Network(rnd, new RawNodeStepper<>(n1, n1Storage), new RawNodeStepper<>(n2, n2Storage));

        Assertions.assertNotEquals(STATE_LEADER, n1.basicStatus().softState().state());

        // n1 should become leader
        n1.randomizedElectionTimeout(n1.electionTimeout());
        
        for (int i = 0; i < n1.electionTimeout(); i++)
            n1.tick();

        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());

        nt.<String>action(ids[0], s -> s.node().sendHeartbeat());

        n1.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));
        n2.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        Assertions.assertFalse(n2.isLearner());

        // n2 start election, should become leader
        n2.randomizedElectionTimeout(n2.electionTimeout());

        nt.<String>action(ids[1], s -> {
            for (int i = 0; i < n2.electionTimeout(); i++)
                n2.tick();
        });

        Assertions.assertEquals(STATE_FOLLOWER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_LEADER, n2.basicStatus().softState().state());
    }

    /**
     * testLearnerCanVote checks that a learner can vote when it receives a valid Vote request.
     *
     * @see RawNode#stepInternal for why this is necessary and correct behavior.
     */
    @Test
    public void testLearnerCanVote() {
        RawNode<String> n2 = newTestRaft(
            newTestConfig(10, 1),
            memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]})
        );

        n2.becomeFollower(1, null);

        n2.step(msgFactory.newVoteRequest(ids[0], ids[1], false, 2, 11, 11, false));

        List<Message> msgs = n2.readMessages();

        Assertions.assertEquals(1, msgs.size());

        Message msg = msgs.get(0);

        Assertions.assertEquals(MessageType.MsgVoteResp, msg.type());
        Assertions.assertFalse(((VoteResponse)msg).reject());
    }

    @Test
    public void testLeaderCycle() {
        checkLeaderCycle(false);
    }

    @Test
    public void testLeaderCyclePreVote() {
        checkLeaderCycle(true);
    }

    // testLeaderCycle verifies that each node in a cluster can campaign
    // and be elected in turn. This ensures that elections (including
    // pre-vote) work when not starting from a clean slate (as they do in
    // TestLeaderElection)
    protected void checkLeaderCycle(boolean preVote) {
        Function<RaftConfig, RaftConfig> cfgFunc = preVote ? c -> c.preVote(true) : null;

        Network n = newNetwork(cfgFunc, entries(), entries(), entries());

        for (UUID campaignerID : n.ids()) {
            n.<String>action(campaignerID, s -> s.node().campaign());

            for (UUID peer : n.ids()) {
                RawNodeStepper<String> s = n.peer(peer);

                if (campaignerID.equals(s.id()))
                    Assertions.assertEquals(STATE_LEADER, s.node().basicStatus().softState().state());
                else
                    Assertions.assertEquals(STATE_FOLLOWER, s.node().basicStatus().softState().state());
            }
        }
    }

    // TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
    // newly-elected leader does *not* have the newest (i.e. highest term)
    // log entries, and must overwrite higher-term log entries with
    // lower-term ones.
    @Test
    public void testLeaderElectionOverwriteNewerLogs() {
        checkLeaderElectionOverwriteNewerLogs(false);
    }

    @Test
    public void testLeaderElectionOverwriteNewerLogsPreVote() {
        checkLeaderElectionOverwriteNewerLogs(true);
    }

    private void checkLeaderElectionOverwriteNewerLogs(boolean preVote) {
        Function<RaftConfig, RaftConfig> cfgFunc = preVote ? c -> c.preVote(true) : null;

        // This network represents the results of the following sequence of
        // events:
        // - Node 1 won the election in term 1.
        // - Node 1 replicated a log entry to node 2 but died before sending
        //   it to other nodes.
        // - Node 3 won the second election in term 2.
        // - Node 3 wrote an entry to its logs but died without sending it
        //   to any other nodes.
        //
        // At this point, nodes 1, 2, and 3 all have uncommitted entries in
        // their logs and could win an election at term 3. The winner's log
        // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
        // the case where older log entries are overwritten, so this test
        // focuses on the case where the newer entries are lost).
        Network n = newNetwork(
            cfgFunc,
            entries(1),                 // Node 1: Won first election
            entries(1),                 // Node 2: Got logs from node 1
            entries(2),                 // Node 3: Won second election
            votedWithConfig(3, 2), // Node 4: Voted but didn't get logs
            votedWithConfig(3, 2)  // Node 5: Voted but didn't get logs
        );

        {
            // Node 1 campaigns. The election fails because a quorum of nodes
            // know about the election that already happened at term 2. Node 1's
            // term is pushed ahead to 2.
            n.<String>action(n.ids()[0], s -> s.node().campaign());

            RawNode<String> node = n.<RawNodeStepper<String>>peer(n.ids()[0]).node();

            Assertions.assertEquals(STATE_FOLLOWER, node.basicStatus().softState().state());
            Assertions.assertEquals(2, node.basicStatus().hardState().term());

            // Node 1 campaigns again with a higher term. This time it succeeds.
            n.<String>action(n.ids()[0], s -> s.node().campaign());

            Assertions.assertEquals(STATE_LEADER, node.basicStatus().softState().state());
            Assertions.assertEquals(3, node.basicStatus().hardState().term());
        }

        // Now all nodes agree on a log entry with term 1 at index 1 (and
        // term 3 at index 2 which is an empty entry).
        for (UUID id : n.ids()) {
            RawNode<String> node = n.<RawNodeStepper<String>>peer(id).node();

            List<Entry> entries = node.raftLog().allEntries();

            Assertions.assertEquals(2, entries.size());

            long[] terms = new long[] {1, 3};

            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);

                Assertions.assertEquals(ENTRY_DATA, entry.type());
                Assertions.assertEquals(terms[i], entry.term());
                Assertions.assertEquals(i + 1, entry.index());
            }
        }
    }

    @Test
    public void testVoteFromAnyState() {
        checkVoteFromAnyState(false);
    }

    @Test
    public void testPreVoteFromAnyState() {
        checkVoteFromAnyState(true);
    }

    private void checkVoteFromAnyState(boolean preVote) {
        for (StateType st : StateType.values()) {
            RawNode<String> r = newTestRaft(ids, 10, 1);
            Assertions.assertEquals(1, r.basicStatus().hardState().term());

            switch (st) {
                case STATE_FOLLOWER: {
                    r.becomeFollower(r.basicStatus().hardState().term(), ids[2]);

                    break;
                }

                case STATE_PRE_CANDIDATE: {
                    r.becomePreCandidate();

                    break;
                }

                case STATE_CANDIDATE: {
                    r.becomeCandidate();

                    break;
                }

                case STATE_LEADER: {
                    r.becomeCandidate();

                    r.becomeLeader();

                    break;
                }
            }

            // Note that setting our state above may have advanced r.Term
            // past its initial value.
            long origTerm = r.basicStatus().hardState().term();
            long newTerm = origTerm + 1;

            Message msg = msgFactory.newVoteRequest(
                ids[1],
                ids[0],
                preVote,
                newTerm,
                42,
                newTerm,
                false
            );

            r.step(msg);

            List<Message> msgs = r.readMessages();

            Assertions.assertEquals(1, msgs.size());
            Assertions.assertEquals(MessageType.MsgVoteResp, msgs.get(0).type());
            VoteResponse res = (VoteResponse)msgs.get(0);
            Assertions.assertFalse(res.reject());

            BasicStatus bs = r.basicStatus();
            SoftState softState = bs.softState();
            HardState hardState = bs.hardState();

            // If this was a real vote, we reset our state and term.
            if (!preVote) {
                Assertions.assertEquals(STATE_FOLLOWER, softState.state());
                Assertions.assertEquals(newTerm, hardState.term());
                Assertions.assertEquals(ids[1], hardState.vote());
            } else {
                // In a prevote, nothing changes.
                Assertions.assertEquals(st, softState.state());
                Assertions.assertEquals(origTerm, hardState.term());
                // if st == StateFollower or StatePreCandidate, r hasn't voted yet.
                // In StateCandidate or StateLeader, it's voted for itself.
                Assertions.assertTrue(hardState.vote() == null || hardState.vote().equals(ids[0]),
                    "For state " + st + " got " + hardState.toString());
            }
        }
    }

    @Test
    public void testLogReplication() {
        class NodeAction implements Consumer<RawNodeStepper<String>> {
            int idx;
            String propose;

            NodeAction(int idx, String propose) {
                this.idx = idx;
                this.propose = propose;
            }

            @Override public void accept(RawNodeStepper<String> s) {
                if (propose != null)
                    s.node().propose(propose);
                else
                    s.node().campaign();
            }
        }

        class TestData {
            Network n;
            List<NodeAction> actions;
            long wcommmitted;

            TestData(Network n, List<NodeAction> actions, long wcommmitted) {
                this.n = n;
                this.actions = actions;
                this.wcommmitted = wcommmitted;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(
                newNetwork(null, entries(), entries()),
                Arrays.asList(new NodeAction(0, "somedata")),
                2),
            new TestData(
                newNetwork(null, entries(), entries()),
                Arrays.asList(
                    new NodeAction(0, "somedata"),
                    new NodeAction(1, null),
                    new NodeAction(1, "somedata2")),
                4)
        };

        for (TestData tt : tests) {
            tt.n.<String>action(tt.n.ids()[0], s -> s.node().campaign());

            for (NodeAction action : tt.actions)
                tt.n.<String>action(tt.n.ids()[action.idx], action);

            for (UUID peerId : tt.n.ids()) {
                RawNodeStepper<String> sm = tt.n.peer(peerId);

                Assertions.assertEquals(tt.wcommmitted, sm.node().raftLog().committed());

                List<String> committed = new ArrayList<>();

                for (Entry e : nextEntries(sm.node(), tt.n.storage(peerId))) {
                    if (e.data() instanceof UserData) {
                        UserData<String> d = e.<UserData<String>>data();

                        if (d.data() != null)
                            committed.add(d.data());
                    }
                }

                List<String> props = new ArrayList<>();

                for (NodeAction action : tt.actions) {
                    if (action.propose != null)
                        props.add(action.propose);
                }

                Assertions.assertEquals(props, committed);
            }
        }
    }

    // TestLearnerLogReplication tests that a learner can receive entries from the leader.
    @Test
    public void testLearnerLogReplication() {
        UUID[] ids = idsBySize(2);

        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1), n1Storage, rnd);

        MemoryStorage n2Storage = memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1), n2Storage, rnd);

        Network nt = new Network(rnd, new RawNodeStepper<String>(n1, n1Storage), new RawNodeStepper<String>(n2, n2Storage));

        n1.becomeFollower(1, null);
        n2.becomeFollower(1, null);

        n1.randomizedElectionTimeout(n1.electionTimeout());

        nt.<String>action(ids[0], s -> {
            for (int i = 0; i < n1.electionTimeout(); i++)
                n1.tick();
        });

        // n1 is leader and n2 is learner
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertTrue(n2.isLearner());

        long nextCommitted = n1.raftLog().committed() + 1;

        nt.<String>action(ids[0], s -> s.node().propose("somedata"));

        Assertions.assertEquals(nextCommitted, n1.raftLog().committed());
        Assertions.assertEquals(n1.raftLog().committed(), n2.raftLog().committed());

        long match = n1.tracker().progress(ids[1]).match();
        Assertions.assertEquals(n2.raftLog().committed(), match);
    }

    @Test
    public void testSingleNodeCommit() {
        Network tt = newNetwork(null, entries());
        tt.<String>action(tt.ids()[0], s -> s.node().campaign());
        tt.<String>action(tt.ids()[0], s -> s.node().propose("some data"));
        tt.<String>action(tt.ids()[0], s -> s.node().propose("some data"));

        RawNodeStepper<String> sm = tt.peer(tt.ids()[0]);

        Assertions.assertEquals(3, sm.node().raftLog().committed());
    }

    // TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
    // when leader changes, no new proposal comes in and ChangeTerm proposal is
    // filtered.
    @Test
    public void testCannotCommitWithoutNewTermEntry() {
        Network tt = newNetwork(null, entries(), entries(), entries(), entries(), entries());
        UUID[] ids = tt.ids();

        tt.<String>action(ids[0], s -> s.node().campaign());

        // 0 cannot reach 2,3,4
        tt.cut(ids[0], ids[2]);
        tt.cut(ids[0], ids[3]);
        tt.cut(ids[0], ids[4]);

        tt.<String>action(ids[0], s -> s.node().propose("some data"));
        tt.<String>action(ids[0], s -> s.node().propose("some data"));

        {
            RawNodeStepper<String> st = tt.peer(ids[0]);
            RawNode<String> sm = st.node();

            Assertions.assertEquals(1, sm.raftLog().committed());
        }

        // network recovery
        tt.recover();

        // avoid committing ChangeTerm proposal
        tt.ignore(MsgApp);

        // elect 2 as the new leader with term 2
        tt.<RawNodeStepper<String>>action(ids[1], s -> s.node().campaign());

        {
            // no log entries from previous term should be committed
            RawNodeStepper<String> st = tt.peer(ids[1]);
            RawNode<String> sm = st.node();

            Assertions.assertEquals(1, sm.raftLog().committed());
        }

        tt.recover();

        // send heartbeat; reset wait
        tt.<String>action(ids[1], s -> s.node().sendHeartbeat());
        
        // append an entry at current term
        tt.<String>action(ids[1], s -> s.node().propose("some data"));

        {
            // expect the committed to be advanced
            RawNodeStepper<String> st = tt.peer(ids[1]);
            RawNode<String> sm = st.node();

            Assertions.assertEquals(5, sm.raftLog().committed());
        }
    }

    // TestCommitWithoutNewTermEntry tests the entries could be committed
    // when leader changes, no new proposal comes in.
    @Test
    public void testCommitWithoutNewTermEntry() {
        Network tt = newNetwork(null, entries(), entries(), entries(), entries(), entries());
        UUID[] ids = tt.ids();

        tt.<String>action(ids[0], s -> s.node().campaign());

        // 0 cannot reach 2,3,4
        tt.cut(ids[0], ids[2]);
        tt.cut(ids[0], ids[3]);
        tt.cut(ids[0], ids[4]);

        tt.<String>action(ids[0], s -> s.node().propose("some data"));
        tt.<String>action(ids[0], s -> s.node().propose("some data"));

        {
            RawNodeStepper<String> sm = tt.peer(ids[0]);
            Assertions.assertEquals(1, sm.node().raftLog().committed());
        }

        // network recovery
        tt.recover();

        // elect 2 as the new leader with term 2
        // after append a ChangeTerm entry from the current term, all entries
        // should be committed
        tt.<String>action(ids[1], s -> s.node().campaign());
        {
            RawNodeStepper<String> sm = tt.peer(ids[0]);
            Assertions.assertEquals(4, sm.node().raftLog().committed());
        }
    }

    @Test
    public void testReadOnlyOptionSafe() {
        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(10, 1, aStorage);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(10, 1, bStorage);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(10, 1, cStorage);

        Network nt = new Network(
            new Random(seed()),
            new RawNodeStepper<>(a, aStorage),
            new RawNodeStepper<>(b, bStorage),
            new RawNodeStepper<>(c, cStorage));

        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        for (int i = 0; i < b.electionTimeout(); i++) {
            b.tick();
        }

        nt.<String>action(ids[0], s -> s.node().campaign());
        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());

        IgniteUuidGenerator gen = new IgniteUuidGenerator(UUID.randomUUID(), 1);

        class TestData {
            RawNode<String> sm;
            int proposals;
            long wReadIdx;
            IgniteUuid wCtx;

            TestData(RawNode<String> sm, int proposals, long wReadIdx, IgniteUuid wCtx) {
                this.sm = sm;
                this.proposals = proposals;
                this.wReadIdx = wReadIdx;
                this.wCtx = wCtx;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(a, 10, 11, gen.randomUuid()),
            new TestData(b, 10, 21, gen.randomUuid()),
            new TestData(c, 10, 31, gen.randomUuid()),
            new TestData(a, 10, 41, gen.randomUuid()),
            new TestData(b, 10, 51, gen.randomUuid()),
            new TestData(c, 10, 61, gen.randomUuid()),
        };

        for (TestData tt : tests) {
            for (int j = 0; j < tt.proposals; j++)
                nt.<String>action(ids[0], s -> s.node().propose(""));

            RawNode<String> r = tt.sm;

            try {
                nt.<String>action(r.basicStatus().id(), s -> s.node().requestReadIndex(tt.wCtx));
                Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());

                Assertions.assertNotEquals(0, r.readStates().size());
                ReadState rs = r.readStates().get(0);
                Assertions.assertEquals(tt.wReadIdx, rs.index());
                Assertions.assertEquals(tt.wCtx, rs.context());

                r.readStates().clear();
            }
            catch (NotLeaderException e) {
                Assertions.assertNotEquals(STATE_LEADER, r.basicStatus().softState().state());
            }
        }
    }

    public void testReadOnlyWithLearner() {
        UUID[] ids = idsBySize(2);
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> a = newTestRaft(10, 1, aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> b = newTestRaft(10, 1, bStorage, rnd);

        Network nt = new Network(rnd, new RawNodeStepper<>(a, aStorage), new RawNodeStepper<>(b, bStorage));
        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());
        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());

        IgniteUuidGenerator gen = new IgniteUuidGenerator(UUID.randomUUID(), 1);

        class TestData {
            RawNode<String> sm;
            int proposals;
            long wReadIdx;
            IgniteUuid wCtx;

            TestData(RawNode<String> sm, int proposals, long wReadIdx, IgniteUuid wCtx) {
                this.sm = sm;
                this.proposals = proposals;
                this.wReadIdx = wReadIdx;
                this.wCtx = wCtx;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(a, 10, 11, gen.randomUuid()),
            new TestData(b, 10, 21, gen.randomUuid()),
            new TestData(a, 10, 31, gen.randomUuid()),
            new TestData(b, 10, 41, gen.randomUuid()),
        };

        for (TestData tt : tests) {
            for (int j = 0; j < tt.proposals; j++)
                nt.<String>action(ids[0], s -> s.node().propose(""));

            RawNode<String> r = tt.sm;
            nt.<String>action(r.basicStatus().id(), s -> s.node().requestReadIndex(tt.wCtx));

            Assertions.assertNotEquals(0, r.readStates().size());
            ReadState rs = r.readStates().get(0);
            Assertions.assertEquals(tt.wReadIdx, rs.index());
            Assertions.assertEquals(tt.wCtx, rs.context());

            r.readStates().clear();
        }
    }

    @Test
    public void TestReadOnlyOptionLease() {
        RaftConfig cfg = newTestConfig(10, 1).readOnlyOption(ReadOnlyOption.READ_ONLY_LEASE_BASED).checkQuorum(true);
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(cfg, aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(cfg, bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(cfg, cStorage, rnd);

        Network nt = new Network(rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage),
            new RawNodeStepper<String>(c, cStorage));

        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());
        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());

        IgniteUuidGenerator gen = new IgniteUuidGenerator(UUID.randomUUID(), 1);

        class TestData {
            RawNode<String> sm;
            int proposals;
            long wReadIdx;
            IgniteUuid wCtx;

            TestData(RawNode<String> sm, int proposals, long wReadIdx, IgniteUuid wCtx) {
                this.sm = sm;
                this.proposals = proposals;
                this.wReadIdx = wReadIdx;
                this.wCtx = wCtx;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(a, 10, 11, gen.randomUuid()),
            new TestData(b, 10, 21, gen.randomUuid()),
            new TestData(c, 10, 31, gen.randomUuid()),
            new TestData(a, 10, 41, gen.randomUuid()),
            new TestData(b, 10, 51, gen.randomUuid()),
            new TestData(c, 10, 61, gen.randomUuid()),
        };

        for (TestData tt : tests) {
            for (int j = 0; j < tt.proposals; j++)
                nt.<String>action(ids[0], s -> s.node().propose(""));

            RawNode<String> r = tt.sm;
            try {
                nt.<String>action(r.basicStatus().id(), s -> s.node().requestReadIndex(tt.wCtx));
                Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());

                Assertions.assertNotEquals(0, r.readStates().size());
                ReadState rs = r.readStates().get(0);
                Assertions.assertEquals(tt.wReadIdx, rs.index());
                Assertions.assertEquals(tt.wCtx, rs.context());

                r.readStates().clear();
            }
            catch (NotLeaderException e) {
                Assertions.assertNotEquals(STATE_LEADER, r.basicStatus().softState().state());
            }
        }
    }

    // TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
    // when it commits at least one log entry at it term.
    @Test
    public void testReadOnlyForNewLeader() throws Exception {
        class NodeConfig {
            UUID id;
            long committed;
            long applied;
            long compactIdx;

            NodeConfig(UUID id, long committed, long applied, long compactIdx) {
                this.id = id;
                this.committed = committed;
                this.applied = applied;
                this.compactIdx = compactIdx;
            }
        }

        NodeConfig[] cfgs = new NodeConfig[] {
            new NodeConfig(ids[0], 1, 1, 0),
            new NodeConfig(ids[1], 2, 2, 2),
            new NodeConfig(ids[2], 2, 2, 2),
        };

        Random rnd = new Random(seed());

        RawNodeStepper<String>[] peers = new RawNodeStepper[cfgs.length];

        for (int i = 0, cfgsLength = cfgs.length; i < cfgsLength; i++) {
            NodeConfig c = cfgs[i];
            MemoryStorage memStorage = memoryStorage(c.id, ids);

            memStorage.append(Arrays.asList(
                entryFactory.newEntry(1, 1, new UserData<>("")),
                entryFactory.newEntry(1, 2, new UserData<>("")))
            );

            memStorage.saveHardState(new HardState(1, null, c.committed));

            if (c.compactIdx != 0)
                memStorage.compact(c.compactIdx);

            RaftConfig cfg = newTestConfig(10, 1);
            RawNode<String> raft = newTestRaft(cfg, memStorage, rnd, c.applied);

            peers[i] = new RawNodeStepper<>(raft, memStorage);
        }

        Network nt = new Network(rnd, peers);

        // Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
        nt.ignore(MsgApp);

        // Force peer a to become leader.
        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNodeStepper<String> st = nt.peer(ids[0]);
        RawNode<String> r = st.node();
        Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());

        IgniteUuidGenerator gen = new IgniteUuidGenerator(UUID.randomUUID(), 1);

        // Ensure peer a drops read only request.
        long wIdx = 4;
        IgniteUuid wCtx = gen.randomUuid();

        nt.<String>action(ids[0], s -> s.node().requestReadIndex(wCtx));

        Assertions.assertEquals(1, r.readStates().size());
        ReadState rs = r.readStates().get(0);
        Assertions.assertEquals(-1, rs.index());
        Assertions.assertEquals(wCtx, rs.context());
        r.readStates().clear();

        nt.recover();

        // Force peer a to commit a log entry at its term
        for (int i = 0; i < r.heartbeatTimeout(); i++)
            r.tick();

        nt.<String>action(ids[0], s -> s.node().propose(""));
        Assertions.assertEquals(4, r.raftLog().committed());

        long lastLogTerm = r.raftLog().zeroTermOnErrCompacted(
            r.raftLog().term(r.raftLog().committed()));

        Assertions.assertEquals(r.basicStatus().hardState().term(), lastLogTerm);

        // Ensure peer a accepts read only request after it commits a entry at its term.
        nt.<String>action(ids[0], s -> s.node().requestReadIndex(wCtx));
        Assertions.assertEquals(1, r.readStates().size());
        rs = r.readStates().get(0);
        Assertions.assertEquals(wIdx, rs.index());
        Assertions.assertEquals(wCtx, rs.context());

        r.readStates().clear();
    }

    @Test
    public void testDuelingCandidates() throws Exception {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(newTestConfig(10, 1), bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(newTestConfig(10, 1), cStorage, rnd);

        Network nt = new Network(rnd,
            new RawNodeStepper<>(a, aStorage),
            new RawNodeStepper<>(b, bStorage),
            new RawNodeStepper<>(c, cStorage)
        );

        nt.cut(ids[0], ids[2]);

        nt.<String>action(ids[0], s -> s.node().campaign());
        nt.<String>action(ids[2], s -> s.node().campaign());

        // 0 becomes leader since it receives votes from 0 and 1
        RawNodeStepper<String> st = nt.peer(ids[0]);
        Assertions.assertEquals(STATE_LEADER, st.node().basicStatus().softState().state());
        
        // 2 stays as candidate since it receives a vote from 3 and a rejection from 2
        st = nt.peer(ids[2]);
        Assertions.assertEquals(STATE_CANDIDATE, st.node().basicStatus().softState().state());

        nt.recover();

        // candidate 2 now increases its term and tries to vote again
        // we expect it to disrupt the leader 0 since it has a higher term
        // 2 will be follower again since both 0 and 1 reject its vote request since 2 does not have a long enough log
        nt.<String>action(ids[2], s -> s.node().campaign());

        for (UUID id : ids) {
            RawNodeStepper<String> peer = nt.peer(id);

            BasicStatus bs = peer.node().basicStatus();

            Assertions.assertEquals(STATE_FOLLOWER, bs.softState().state());
            Assertions.assertEquals(3, bs.hardState().term());

            if (!id.equals(ids[2])) {
                List<Entry> entries = peer.node().raftLog().unstableEntries();
                Assertions.assertEquals(1, entries.size());

                Entry entry = entries.get(0);

                Assertions.assertEquals(2, entry.term());
                Assertions.assertEquals(1, entry.index());
                Assertions.assertEquals(ENTRY_DATA, entry.type());

                Assertions.assertTrue(entry.data() instanceof UserData);
                Assertions.assertNull(entry.<UserData>data().data());

                Assertions.assertEquals(1, peer.node().raftLog().committed());
            }
            else
                Assertions.assertTrue(peer.node().raftLog().unstableEntries().isEmpty());
        }
    }

    @Test
    public void testDuelingPreCandidates() throws Exception {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1).preVote(true), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(newTestConfig(10, 1).preVote(true), bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(newTestConfig(10, 1).preVote(true), cStorage, rnd);

        Network nt = new Network(rnd,
            new RawNodeStepper<>(a, aStorage),
            new RawNodeStepper<>(b, bStorage),
            new RawNodeStepper<>(c, cStorage)
        );

        nt.cut(ids[0], ids[2]);

        nt.<String>action(ids[0], s -> s.node().campaign());
        nt.<String>action(ids[2], s -> s.node().campaign());

        // 1 becomes leader since it receives votes from 1 and 2
        RawNodeStepper<String> st = nt.peer(ids[0]);
        Assertions.assertEquals(STATE_LEADER, st.node().basicStatus().softState().state());

        // 3 campaigns then reverts to follower when its PreVote is rejected
        st = nt.peer(ids[2]);
        Assertions.assertEquals(STATE_FOLLOWER, st.node().basicStatus().softState().state());

        nt.recover();

        // Candidate 3 now increases its term and tries to vote again.
        // With PreVote, it does not disrupt the leader.
        nt.<String>action(ids[2], s -> s.node().campaign());

        for (UUID id : ids) {
            RawNodeStepper<String> peer = nt.peer(id);

            BasicStatus bs = peer.node().basicStatus();

            Assertions.assertEquals(
                id.equals(ids[0]) ? STATE_LEADER : STATE_FOLLOWER,
                bs.softState().state());
            Assertions.assertEquals(2, bs.hardState().term());

            if (!id.equals(ids[2])) {
                List<Entry> entries = peer.node().raftLog().unstableEntries();
                Assertions.assertEquals(1, entries.size());

                Entry entry = entries.get(0);

                Assertions.assertEquals(2, entry.term());
                Assertions.assertEquals(1, entry.index());
                Assertions.assertEquals(ENTRY_DATA, entry.type());

                Assertions.assertTrue(entry.data() instanceof UserData);
                Assertions.assertNull(entry.<UserData>data().data());

                Assertions.assertEquals(1, peer.node().raftLog().committed());
            }
            else
                Assertions.assertTrue(peer.node().raftLog().unstableEntries().isEmpty());
        }
    }

    @Test
    public void testCandidateConcede() {
        Network tt = newNetwork(null, entries(), entries(), entries());

        UUID[] ids = tt.ids();

        tt.isolate(tt.ids()[0]);

        tt.<String>action(ids[0], s -> s.node().campaign());
        tt.<String>action(ids[2], s -> s.node().campaign());

        // heal the partition
        tt.recover();

        // send heartbeat; reset wait
        tt.<String>action(ids[2], s -> s.node().sendHeartbeat());

        String data = "force follower";

        // send a proposal to 3 to flush out a MsgApp to 1
        tt.<String>action(ids[2], s -> s.node().propose(data));

        // send heartbeat; flush out commit
        tt.<String>action(ids[2], s -> s.node().sendHeartbeat());

        RawNodeStepper<String> a = tt.peer(ids[0]);

        Assertions.assertEquals(STATE_FOLLOWER, a.node().basicStatus().softState().state());
        Assertions.assertEquals(2, a.node().basicStatus().hardState().term());

        for (UUID id : tt.ids()) {
            RawNodeStepper<String> peer = tt.peer(id);

            RaftLog raftLog = peer.node().raftLog();

            Assertions.assertEquals(2, raftLog.committed());
            List<Entry> entries = raftLog.unstableEntries();

            Assertions.assertEquals(2, entries.size());

            {
                Entry entry = entries.get(0);
                Assertions.assertEquals(1, entry.index());
                Assertions.assertEquals(2, entry.term());
                Assertions.assertEquals(ENTRY_DATA, entry.type());
                Assertions.assertNull(entry.<UserData>data().data());
            }

            {
                Entry entry = entries.get(1);
                Assertions.assertEquals(2, entry.index());
                Assertions.assertEquals(2, entry.term());
                Assertions.assertEquals(ENTRY_DATA, entry.type());
                Assertions.assertEquals(data, entry.<UserData>data().data());
            }
        }
    }

    @Test
    public void testSingleNodeCandidate() {
        checkSingleNode(false);
    }

    @Test
    public void testSingleNodePreCandidate() {
        checkSingleNode(true);
    }

    private void checkSingleNode(boolean preVote) {
        Network tt = newNetwork(c -> c.preVote(preVote), entries());

        tt.<String>action(tt.ids()[0], s -> s.node().campaign());

        RawNodeStepper<String> st = tt.peer(tt.ids()[0]);
        Assertions.assertEquals(STATE_LEADER, st.node().basicStatus().softState().state());
    }

    @Test
    public void testOldMessages() {
        Network tt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = tt.ids();

        // make 0 leader @ term 3
        tt.<String>action(ids[1], s -> s.node().campaign());
        tt.<String>action(ids[0], s -> s.node().campaign());

        // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
        tt.send(msgFactory.newAppendEntriesRequest(
            ids[1],
            ids[2],
            2,
            3,
            2,
            Arrays.asList(entryFactory.newEntry(2, 3, new UserData<>(""))),
            0
        ));

        // commit a new entry
        tt.<String>action(ids[0], s -> s.node().propose("somedata"));

        for (UUID id : ids) {
            RawNodeStepper<String> peer = tt.peer(id);

            RaftLog raftLog = peer.node().raftLog();

            List<Entry> entries = raftLog.unstableEntries();

            Assertions.assertEquals(3, entries.size());

            long[] terms = new long[] {2, 3, 3};

            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);

                Assertions.assertEquals(i + 1, entry.index());
                Assertions.assertEquals(terms[i], entry.term());
                Assertions.assertEquals(ENTRY_DATA, entry.type());
                Assertions.assertEquals(i == entries.size() - 1 ? "somedata" : null, entry.<UserData>data().data());
            }

            Assertions.assertEquals(3, raftLog.committed());
        }
    }

    @Test
    public void testProposal() {
        class TestData {
            Network n;
            boolean success;

            TestData(Network n, boolean success) {
                this.n = n;
                this.success = success;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(newNetwork(null, entries(), entries(), entries()), true),
            new TestData(newNetwork(null, entries(), entries(), null), true),
            new TestData(newNetwork(null, entries(), null, null), false),
            new TestData(newNetwork(null, entries(), null, null, entries()), false),
            new TestData(newNetwork(null, entries(), null, null, entries(), entries()), true)
        };

        for (TestData tt : tests) {
            String data = "somedata";
            UUID[] ids = tt.n.ids();

            try {
                // promote 1 to become leader
                tt.n.action(ids[0], s -> s.node().campaign());
                tt.n.action(ids[0], s -> s.node().propose(data));

                Assertions.assertTrue(tt.success);
            }
            catch (NotLeaderException e) {
                Assertions.assertFalse(tt.success);
            }

            List<Entry> wantLog = new ArrayList<>();

            if (tt.success) {
                wantLog.add(entryFactory.newEntry(2, 1, new UserData<>(null)));
                wantLog.add(entryFactory.newEntry(2, 2, new UserData<>(data)));
            }

            for (UUID id : ids) {
                Stepper st = tt.n.peer(id);

                if (st instanceof RawNodeStepper) {
                    RawNodeStepper<String> s = (RawNodeStepper<String>)st;
                    RaftLog raftLog = s.node().raftLog();

                    Assertions.assertEquals(wantLog, raftLog.unstableEntries());
                    Assertions.assertEquals(wantLog.size(), raftLog.committed());
                }
            }

            Assertions.assertEquals(2, tt.n.<RawNodeStepper<String>>peer(ids[0]).node().basicStatus().hardState().term());
        }
    }

    @Test
    public void testCommit() {
        class TestData {
            long[] matches;
            Entry[] logs;
            long smTerm;
            long w;

            TestData(long[] matches, Entry[] logs, long smTerm, long w) {
                this.matches = matches;
                this.logs = logs;
                this.smTerm = smTerm;
                this.w = w;
            }
        };

        TestData[] tests = new TestData[] {
            // single
            new TestData(new long[] {1}, new Entry[] {entry(1, 1)}, 1, 1),
            new TestData(new long[] {1}, new Entry[] {entry(1, 1)}, 2, 0),
            new TestData(new long[] {2}, new Entry[] {entry(1, 1), entry(2, 2)}, 2, 2),
            new TestData(new long[] {1}, new Entry[] {entry(2, 1)}, 2, 1),

            // odd
            new TestData(new long[] {2, 1, 1}, new Entry[] {entry(1, 1), entry(2, 2)}, 1, 1),
            new TestData(new long[] {2, 1, 1}, new Entry[] {entry(1, 1), entry(1, 2)}, 2, 0),
            new TestData(new long[] {2, 1, 2}, new Entry[] {entry(1, 1), entry(2, 2)}, 2, 2),
            new TestData(new long[] {2, 1, 2}, new Entry[] {entry(1, 1), entry(1, 2)}, 2, 0),

            // even
            new TestData(new long[] {2, 1, 1, 1}, new Entry[] {entry(1, 1), entry(2, 2)}, 1, 1),
            new TestData(new long[] {2, 1, 1, 1}, new Entry[] {entry(1, 1), entry(1, 2)}, 2, 0),
            new TestData(new long[] {2, 1, 1, 2}, new Entry[] {entry(1, 1), entry(2, 2)}, 1, 1),
            new TestData(new long[] {2, 1, 1, 2}, new Entry[] {entry(1, 1), entry(1, 2)}, 2, 0),
            new TestData(new long[] {2, 1, 2, 2}, new Entry[] {entry(1, 1), entry(2, 2)}, 2, 2),
            new TestData(new long[] {2, 1, 2, 2}, new Entry[] {entry(1, 1), entry(1, 2)}, 2, 0),
        };

        for (TestData tt : tests) {
            UUID[] ids = idsBySize(1);

            MemoryStorage storage = memoryStorage(ids, new HardState(tt.smTerm, null, 0));
            storage.append(Arrays.asList(tt.logs));

            RawNode<String> sm = newTestRaft(newTestConfig(10, 2), storage);

            ids = Arrays.copyOf(ids, tt.matches.length);

            for (int i = 1; i < ids.length; i++)
                ids[i] = UUID.randomUUID();

            for (int j = 0; j < tt.matches.length; j++) {
                UUID id = ids[j];

                if (j > 0)
                    sm.applyConfChange(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

                Progress pr = sm.tracker().progress(id);
                pr.maybeUpdate(tt.matches[j]);
            }

            sm.maybeCommit();

            Assertions.assertEquals(tt.w, sm.raftLog().committed());
        }
    }

    @Test
    public void testPastElectionTimeout() {
        class TestData {
            int elapse;
            double wProbability;
            boolean round;

            TestData(int elapse, double wProbability, boolean round) {
                this.elapse = elapse;
                this.wProbability = wProbability;
                this.round = round;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(5, 0, false),
            new TestData(10, 0.1, true),
            new TestData(13, 0.4, true),
            new TestData(15, 0.6, true),
            new TestData(18, 0.9, true),
            new TestData(20, 1, false)
        };

        for (TestData tt : tests) {
            UUID[] ids = idsBySize(1);

            RawNode<String> sm = newTestRaft(newTestConfig(10, 1), memoryStorage(ids));

            for (int i = 0; i < tt.elapse; i++)
                sm.tickQuiesced();

            int c = 0;

            for (int j = 0; j < 10000; j++) {
                sm.resetRandomizedElectionTimeout();

                if (sm.pastElectionTimeout())
                    c++;
            }

            double got = c / 10000.;

            if (tt.round)
                got = Math.floor(got * 10 + 0.5) / 10.;

            Assertions.assertEquals(tt.wProbability, got);
        }
    }

    // TestHandleMsgApp ensures:
    // 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
    // 2. If an existing entry conflicts with a new one (same index but different terms),
    //    delete the existing entry and all that follow it; append any new entries not already in the log.
    // 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
    @Test
    public void testHandleMsgApp() {
        class TestData {
            Message m;
            long wIdx;
            long wCommit;
            boolean wReject;

            TestData(Message m, long wIdx, long wCommit, boolean wReject) {
                this.m = m;
                this.wIdx = wIdx;
                this.wCommit = wCommit;
                this.wReject = wReject;
            }
        }

        UUID[] ids = idsBySize(2);

        TestData[] tests = new TestData[] {
            // Ensure 1
            new TestData(
                msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 2, 3, Collections.emptyList(), 3),
                2, 0, true), // previous log mismatch
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 3, 3, Collections.emptyList(), 3),
                2, 0, true), // previous log does not exist

            // Ensure 2
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 1, 1, Collections.emptyList(), 1),
                2, 1, false),
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 0, 0, Arrays.asList(entry(2, 1)), 1),
                1, 1, false),
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 2, 2, Arrays.asList(entry(2, 3), entry(2, 4)), 3),
                4, 3, false),
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 2, 2, Arrays.asList(entry(2, 3)), 4),
                3, 3, false),
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 1, 1, Arrays.asList(entry(2, 2)), 4),
                2, 2, false),

            // Ensure 3
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 1, 1, Collections.emptyList(), 3),
                2, 1, false), // match entry 1, commit up to last new entry 1
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 1, 1, Arrays.asList(entry(2, 2)), 3),
                2, 2, false), // match entry 1, commit up to last new entry 2
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 2, 2, Collections.emptyList(), 3),
                2, 2, false), // match entry 2, commit up to last new entry 2
            new TestData(msgFactory.newAppendEntriesRequest(ids[0], ids[1], 2, 2, 2, Collections.emptyList(), 4),
                2, 2, false), // commit up to log.last()
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage(ids);
            storage.append(Arrays.asList(entry(1, 1), entry(2, 2)));

            RawNode<String> sm = newTestRaft(newTestConfig(10, 1), storage);
            sm.becomeFollower(2, null);

            sm.step(tt.m);

            Assertions.assertEquals(tt.wIdx, sm.raftLog().lastIndex());
            Assertions.assertEquals(tt.wCommit, sm.raftLog().committed());

            List<Message> m = sm.readMessages();
            Assertions.assertEquals(1, m.size());

            Message msg = m.get(0);
            Assertions.assertEquals(MsgAppResp, msg.type());

            AppendEntriesResponse res = (AppendEntriesResponse)msg;
            Assertions.assertEquals(tt.wReject, res.reject());
        }
    }

    // TestHandleHeartbeat ensures that the follower commits to the commit in the message.
    @Test
    public void testHandleHeartbeat() {
        long commit = 2;

        UUID[] ids = idsBySize(2);

        class TestData {
            Message m;
            long wCommit;

            TestData(Message m, long wCommit) {
                this.m = m;
                this.wCommit = wCommit;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(msgFactory.newHeartbeatRequest(ids[1], ids[0], 2, commit + 1, null), commit + 1),
            new TestData(msgFactory.newHeartbeatRequest(ids[1], ids[0], 2, commit - 1, null), commit), // do not decrease commit
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage(ids);
            storage.append(Arrays.asList(entry(1, 1), entry(2, 2), entry(3, 3)));

            RawNode<String> sm = newTestRaft(newTestConfig(5, 1), storage);

            sm.becomeFollower(2, ids[1]);

            sm.raftLog().commitTo(commit);

            sm.step(tt.m);

            Assertions.assertEquals(tt.wCommit, sm.raftLog().committed());

            List<Message> m = sm.readMessages();
            Assertions.assertEquals(1, m.size());
            Assertions.assertEquals(MsgHeartbeatResp, m.get(0).type());
        }
    }

    // TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
    @Test
    public void testHandleHeartbeatResp() {
        UUID[] ids = idsBySize(2);

        MemoryStorage storage = memoryStorage(ids);
        storage.append(Arrays.asList(entry(1, 1), entry(2, 2), entry(3, 3)));

        RawNode<String> sm = newTestRaft(newTestConfig(5, 1), storage);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.raftLog().commitTo(sm.raftLog().lastIndex());

        // A heartbeat response from a node that is behind; re-send MsgApp
        sm.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], 2, null));

        List<Message> msgs = sm.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(MsgApp, msgs.get(0).type());

        // A second heartbeat response generates another MsgApp re-send
        sm.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], 2, null));
        msgs = sm.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(MsgApp, msgs.get(0).type());

        AppendEntriesRequest req = (AppendEntriesRequest)msgs.get(0);

        // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
        sm.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], 2, req.logIndex() + req.entries().size(), false, 0));

        // Consume the message sent in response to MsgAppResp
        sm.readMessages();

        sm.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], 2, null));
        msgs = sm.readMessages();

        Assertions.assertTrue(msgs.isEmpty());
    }

    // TestRaftFreesReadOnlyMem ensures raft will free read request from
    // readOnly readIndexQueue and pendingReadIndex map.
    // related issue: https://github.com/etcd-io/etcd/issues/7571
    @Test
    public void testRaftFreesReadOnlyMem() {
        UUID[] ids = idsBySize(2);
        RawNode<String> sm = newTestRaft(ids, 5, 1);

        sm.becomeCandidate();
        sm.becomeLeader();

        sm.raftLog().commitTo(sm.raftLog().lastIndex());

        IgniteUuid ctx = new IgniteUuid(UUID.randomUUID(), 1);

        // leader starts linearizable read request.
        // more info: raft dissertation 6.4, step 2.
        sm.requestReadIndex(ctx);

        List<Message> msgs = sm.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(MsgHeartbeat, msgs.get(0).type());
        Assertions.assertEquals(ctx, ((HeartbeatRequest)msgs.get(0)).context());

        Assertions.assertEquals(1, sm.readOnly().readIndexQueue().size());
        Assertions.assertEquals(1, sm.readOnly().pendingReadIndex().size());
        Assertions.assertTrue(sm.readOnly().pendingReadIndex().containsKey(ctx));

        // heartbeat responses from majority of followers (1 in this case)
        // acknowledge the authority of the leader.
        // more info: raft dissertation 6.4, step 3.
        sm.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], 2, ctx));
        Assertions.assertTrue(sm.readOnly().readIndexQueue().isEmpty());
        Assertions.assertTrue(sm.readOnly().pendingReadIndex().isEmpty());
    }

    // TestMsgAppRespWaitReset verifies the resume behavior of a leader
    // MsgAppResp.
    @Test
    public void testMsgAppRespWaitReset() {
        RawNode<String> sm = newTestRaft(ids, 5, 1);
        sm.becomeCandidate();
        sm.becomeLeader();

        // The new leader has just emitted a new Term 4 entry; consume those messages
        // from the outgoing queue.
        sm.bcastAppend();
        sm.readMessages();

        // Node 2 acks the first entry, making it committed.
        sm.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], 2, 1, false, 0));
        Assertions.assertEquals(1, sm.raftLog().committed());

        // Also consume the MsgApp messages that update Commit on the followers.
        sm.readMessages();

        // A new command is now proposed on node 1.
        sm.propose("");

        // The command is broadcast to all nodes not in the wait state.
        // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
        List<Message> msgs = sm.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(ids[1], msgs.get(0).to());
        Assertions.assertEquals(MsgApp, msgs.get(0).type());

        AppendEntriesRequest req = (AppendEntriesRequest)msgs.get(0);
        Assertions.assertEquals(1, req.entries().size());
        Assertions.assertEquals(2, req.entries().get(0).index());

        // Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
        sm.step(msgFactory.newAppendEntriesResponse(ids[2], ids[0], 2, 1, false, 0));

        msgs = sm.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(ids[2], msgs.get(0).to());
        Assertions.assertEquals(MsgApp, msgs.get(0).type());

        req = (AppendEntriesRequest)msgs.get(0);
        Assertions.assertEquals(1, req.entries().size());
        Assertions.assertEquals(2, req.entries().get(0).index());
    }

    @Test
    public void testRecvMsgVote() {
        checkRecvMsgVote(false);
    }

    @Test
    public void testRecvMsgPreVote() {
        checkRecvMsgVote(true);
    }

    private void checkRecvMsgVote(boolean preVote) {
        class TestData {
            StateType state;
            long idx;
            long logTerm;
            UUID voteFor;
            boolean wReject;

            TestData(StateType state, long idx, long logTerm, UUID voteFor, boolean wReject) {
                this.state = state;
                this.idx = idx;
                this.logTerm = logTerm;
                this.voteFor = voteFor;
                this.wReject = wReject;
            }
        }

        UUID[] ids = idsBySize(2);

        TestData[] tests = new TestData[] {
            new TestData(STATE_FOLLOWER, 0, 0, null, true),
            new TestData(STATE_FOLLOWER, 0, 1, null, true),
            new TestData(STATE_FOLLOWER, 0, 2, null, true),
            new TestData(STATE_FOLLOWER, 0, 3, null, false),

            new TestData(STATE_FOLLOWER, 1, 0, null, true),
            new TestData(STATE_FOLLOWER, 1, 1, null, true),
            new TestData(STATE_FOLLOWER, 1, 2, null, true),
            new TestData(STATE_FOLLOWER, 1, 3, null, false),

            new TestData(STATE_FOLLOWER, 2, 0, null, true),
            new TestData(STATE_FOLLOWER, 2, 1, null, true),
            new TestData(STATE_FOLLOWER, 2, 2, null, false),
            new TestData(STATE_FOLLOWER, 2, 3, null, false),

            new TestData(STATE_FOLLOWER, 3, 0, null, true),
            new TestData(STATE_FOLLOWER, 3, 1, null, true),
            new TestData(STATE_FOLLOWER, 3, 2, null, false),
            new TestData(STATE_FOLLOWER, 3, 3, null, false),

            new TestData(STATE_FOLLOWER, 3, 2, ids[1], false),
            new TestData(STATE_FOLLOWER, 3, 2, ids[0], true),

            new TestData(STATE_LEADER, 3, 3, ids[0], true),
            new TestData(STATE_PRE_CANDIDATE, 3, 3, ids[0], true),
            new TestData(STATE_CANDIDATE, 3, 3, ids[0], true),
        };

        for (int i = 0; i < tests.length; i++) {
            TestData tt = tests[i];

            MemoryStorage memoryStorage = memoryStorage(ids);
            memoryStorage.append(Arrays.asList(entry(2, 1), entry(2, 2)));

            long lastTerm = 2;
            // raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
            // test we're only testing MsgVote responses when the campaigning node
            // has a different raft log compared to the recipient node.
            // Additionally we're verifying behaviour when the recipient node has
            // already given out its vote for its current term. We're not testing
            // what the recipient node does when receiving a message with a
            // different term number, so we simply initialize both term numbers to
            // be the same.
            long term = Math.max(lastTerm, tt.logTerm);

            memoryStorage.saveHardState(
                new HardState(
                    // Adjust the term as it will be incremented during campaign.
                    tt.state != STATE_FOLLOWER && tt.state != STATE_PRE_CANDIDATE ? term - 1 : term,
                    tt.voteFor,
                    0
                )
            );

            RawNode<String> sm = newTestRaft(newTestConfig(10, 1), memoryStorage);

            switch (tt.state) {
                case STATE_FOLLOWER:
                    sm.becomeFollower(term, null);

                    break;

                case STATE_CANDIDATE:
                    sm.becomeCandidate();

                    break;

                case STATE_PRE_CANDIDATE:
                    sm.becomePreCandidate();

                    break;

                case STATE_LEADER:
                    sm.becomeCandidate();
                    sm.becomeLeader();

                    break;
            }

            Assertions.assertEquals(term, sm.basicStatus().hardState().term(), "Failed for test case " + i);
            Assertions.assertEquals(tt.state, sm.basicStatus().softState().state(), "Failed for test case " + i);

            sm.step(msgFactory.newVoteRequest(ids[1], ids[0], preVote, term, tt.idx, tt.logTerm, false));

            List<Message> msgs = sm.readMessages();
            Assertions.assertEquals(1, msgs.size());
            Assertions.assertEquals(MsgVoteResp, msgs.get(0).type());

            VoteResponse res = (VoteResponse)msgs.get(0);
            Assertions.assertEquals(preVote, res.preVote());
            Assertions.assertEquals(tt.wReject, res.reject());
        }
    }

    @Test
    public void testStateTransition() {
        class TestData {
            StateType from;
            StateType to;
            boolean wAllow;
            long wTerm;
            UUID wLead;

            TestData(StateType from, StateType to, boolean wAllow, long wTerm, UUID wLead) {
                this.from = from;
                this.to = to;
                this.wAllow = wAllow;
                this.wTerm = wTerm;
                this.wLead = wLead;
            }
        }

        UUID[] ids = idsBySize(1);

        TestData[] tests = new TestData[] {
            new TestData(STATE_FOLLOWER, STATE_FOLLOWER, true, 3, null),
            new TestData(STATE_FOLLOWER, STATE_PRE_CANDIDATE, true, 2, null),
            new TestData(STATE_FOLLOWER, STATE_CANDIDATE, true, 3, null),
            new TestData(STATE_FOLLOWER, STATE_LEADER, false, 2, null),

            new TestData(STATE_PRE_CANDIDATE, STATE_FOLLOWER, true, 2, null),
            new TestData(STATE_PRE_CANDIDATE, STATE_PRE_CANDIDATE, true, 2, null),
            new TestData(STATE_PRE_CANDIDATE, STATE_CANDIDATE, true, 3, null),
            new TestData(STATE_PRE_CANDIDATE, STATE_LEADER, true, 2, ids[0]),

            new TestData(STATE_CANDIDATE, STATE_FOLLOWER, true, 2, null),
            new TestData(STATE_CANDIDATE, STATE_PRE_CANDIDATE, true, 2, null),
            new TestData(STATE_CANDIDATE, STATE_CANDIDATE, true, 3, null),
            new TestData(STATE_CANDIDATE, STATE_LEADER, true, 2, ids[0]),

            new TestData(STATE_LEADER, STATE_FOLLOWER, true, 3, null),
            new TestData(STATE_LEADER, STATE_PRE_CANDIDATE, false, 2, null),
            new TestData(STATE_LEADER, STATE_CANDIDATE, false, 3, null),
            new TestData(STATE_LEADER, STATE_LEADER, true, 2, ids[0]),
        };

        for (int i = 0; i < tests.length; i++) {
            TestData tt = tests[i];

            MemoryStorage memoryStorage = memoryStorage(ids);
            memoryStorage.saveHardState(new HardState(tt.from != STATE_FOLLOWER && tt.from != STATE_PRE_CANDIDATE ?
                1 :
                2, null, 0));

            RawNode<String> sm = newTestRaft(newTestConfig(10, 1), memoryStorage);

            switch (tt.from) {
                case STATE_CANDIDATE:
                    sm.becomeCandidate();

                    break;

                case STATE_PRE_CANDIDATE:
                    sm.becomePreCandidate();

                    break;

                case STATE_LEADER:
                    sm.becomeCandidate();
                    sm.becomeLeader();

                    break;

                case STATE_FOLLOWER:
                    sm.becomeFollower(2, null);

                    break;
            }

            try {
                switch (tt.to) {
                    case STATE_CANDIDATE:
                        sm.becomeCandidate();

                        break;

                    case STATE_PRE_CANDIDATE:
                        sm.becomePreCandidate();

                        break;

                    case STATE_LEADER:
                        sm.becomeLeader();

                        break;

                    case STATE_FOLLOWER:
                        sm.becomeFollower(tt.wTerm, tt.wLead);

                        break;
                }

                Assertions.assertTrue(tt.wAllow,
                    "Allowed transition " + tt.from + " -> " + tt.to + " (should be prohibited)"
                );
                Assertions.assertEquals(tt.wTerm, sm.basicStatus().hardState().term(), "Failed for test case " + i);
                Assertions.assertEquals(tt.wLead, sm.basicStatus().softState().lead(), "Failed for test case " + i);
            }
            catch (UnrecoverableException e) {
                Assertions.assertFalse(tt.wAllow,
                    "Prohibited transition " + tt.from + " -> " + tt.to + " (should be allowed)"
                );
            }
        }
    }

    @Test
    public void testAllServerStepdown() {
        class TestData {
            StateType state;
            StateType wState;
            long wTerm;
            long wIdx;

            TestData(StateType state, StateType wState, long wTerm, long wIdx) {
                this.state = state;
                this.wState = wState;
                this.wTerm = wTerm;
                this.wIdx = wIdx;
            }
        }
        
        TestData[] tests = new TestData[] {
            new TestData(STATE_FOLLOWER, STATE_FOLLOWER, 3, 0),
            new TestData(STATE_PRE_CANDIDATE, STATE_FOLLOWER, 3, 0),
            new TestData(STATE_CANDIDATE, STATE_FOLLOWER, 3, 0),
            new TestData(STATE_LEADER, STATE_FOLLOWER, 3, 1),
        };

        long tTerm = 3;

        for (TestData tt : tests) {
            RawNode<String> sm = newTestRaft(ids, 10, 1);

            switch (tt.state) {
                case STATE_FOLLOWER:
                    sm.becomeFollower(1, null);

                    break;

                case STATE_PRE_CANDIDATE:
                    sm.becomePreCandidate();

                    break;

                case STATE_CANDIDATE:
                    sm.becomeCandidate();

                    break;

                case STATE_LEADER:
                    sm.becomeCandidate();
                    sm.becomeLeader();

                    break;
            }

            for (boolean vote : new boolean[] {true, false}) {
                Message msg = vote ?
                    msgFactory.newVoteRequest(ids[1], ids[0], false, tTerm, 0, tTerm, false) :
                    msgFactory.newAppendEntriesRequest(ids[1], ids[0], tTerm, 0, tTerm, Collections.emptyList(), 0);

                sm.step(msg);

                Assertions.assertEquals(tt.wState, sm.basicStatus().softState().state());
                Assertions.assertEquals(tt.wTerm, sm.basicStatus().hardState().term());
                Assertions.assertEquals(tt.wIdx, sm.raftLog().lastIndex());
                Assertions.assertEquals(tt.wIdx, sm.raftLog().allEntries().size());

                UUID wLead = vote ? null : ids[1];
                Assertions.assertEquals(wLead, sm.basicStatus().softState().lead());
            }
        }
    }

    @Test
    public void testCandidateResetTermMsgHeartbeat() {
        checkCandidateResetTerm(MsgHeartbeat);
    }

    @Test
    public void testCandidateResetTermMsgApp() {
        checkCandidateResetTerm(MsgApp);
    }

    // testCandidateResetTerm tests when a candidate receives a
    // MsgHeartbeat or MsgApp from leader, "Step" resets the term
    // with leader's and reverts back to follower.
    private void checkCandidateResetTerm(MessageType mt) {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(10, 1, aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(10, 1, bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(10, 1, cStorage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage),
            new RawNodeStepper<String>(c, cStorage)
        );

        nt.<String>action(ids[0], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, c.basicStatus().softState().state());

        // isolate 3 and increase term in rest
        nt.isolate(ids[2]);

        nt.<String>action(ids[1], s -> s.node().campaign());
        nt.<String>action(ids[0], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());

        // trigger campaign in isolated c
        c.resetRandomizedElectionTimeout();

        for (int i = 0; i < c.randomizedElectionTimeout(); i++)
            c.tick();

        Assertions.assertEquals(STATE_CANDIDATE, c.basicStatus().softState().state());

        nt.recover();

        // leader sends to isolated candidate
        // and expects candidate to revert to follower
        if (mt == MsgHeartbeat)
            nt.<String>action(ids[0], s -> s.node().sendHeartbeat());
        else
            // As per testHandleHeartbeatResp, the heartbeat response will trigger the lader to send append request
            nt.send(msgFactory.newHeartbeatResponse(ids[2], ids[0], a.basicStatus().hardState().term(), null));

        // follower c term is reset with leader's
        Assertions.assertEquals(STATE_FOLLOWER, c.basicStatus().softState().state());
        Assertions.assertEquals(a.basicStatus().hardState().term(), c.basicStatus().hardState().term());
    }

    @Test
    public void testLeaderStepdownWhenQuorumActive() {
        RawNode<String> sm = newTestRaft(newTestConfig(5, 1).checkQuorum(true), memoryStorage(ids));

        sm.becomeCandidate();
        sm.becomeLeader();

        long term = sm.basicStatus().hardState().term();

        for (int i = 0; i < sm.electionTimeout() + 1; i++) {
            sm.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], term, null));

            sm.tick();
        }

        Assertions.assertEquals(STATE_LEADER, sm.basicStatus().softState().state());
    }

    @Test
    public void testLeaderStepdownWhenQuorumLost() {
        RawNode<String> sm = newTestRaft(newTestConfig(5, 1).checkQuorum(true), memoryStorage(ids));

        sm.becomeCandidate();
        sm.becomeLeader();

        for (int i = 0; i < sm.electionTimeout() + 1; i++)
            sm.tick();

        Assertions.assertEquals(STATE_FOLLOWER, sm.basicStatus().softState().state());
    }

    @Test
    public void testLeaderSupersedingWithCheckQuorum() {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1).checkQuorum(true), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(newTestConfig(10, 1).checkQuorum(true), bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(newTestConfig(10, 1).checkQuorum(true), cStorage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage),
            new RawNodeStepper<String>(c, cStorage)
        );

        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, c.basicStatus().softState().state());

        nt.<String>action(ids[2], s -> s.node().campaign());

        // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
        Assertions.assertEquals(STATE_CANDIDATE, c.basicStatus().softState().state());

        // Letting b's electionElapsed reach to electionTimeout
        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[2], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, c.basicStatus().softState().state());
    }

    @Test
    public void testLeaderElectionWithCheckQuorum() {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1).checkQuorum(true), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(newTestConfig(10, 1).checkQuorum(true), bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(newTestConfig(10, 1).checkQuorum(true), cStorage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage),
            new RawNodeStepper<String>(c, cStorage)
        );

        a.randomizedElectionTimeout(a.electionTimeout() + 1);
        b.randomizedElectionTimeout(b.electionTimeout() + 2);

        // Immediately after creation, votes are cast regardless of the
        // election timeout.
        nt.<String>action(ids[0], s -> s.node().campaign());
        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, c.basicStatus().softState().state());

        // need to reset randomizedElectionTimeout larger than electionTimeout again,
        // because the value might be reset to electionTimeout since the last state changes
        a.randomizedElectionTimeout(a.electionTimeout() + 1);
        b.randomizedElectionTimeout(b.electionTimeout() + 2);

        for (int i = 0; i < a.electionTimeout(); i++)
            a.tick();

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[2], s -> s.node().campaign());

        Assertions.assertEquals(STATE_FOLLOWER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(STATE_LEADER, c.basicStatus().softState().state());
    }

    // TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
    // can disrupt the leader even if the leader still "officially" holds the lease, The
    // leader is expected to step down and adopt the candidate's term
    @Test
    public void testFreeStuckCandidateWithCheckQuorum() {
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1).checkQuorum(true), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], ids);
        RawNode<String> b = newTestRaft(newTestConfig(10, 1).checkQuorum(true), bStorage, rnd);

        MemoryStorage cStorage = memoryStorage(ids[2], ids);
        RawNode<String> c = newTestRaft(newTestConfig(10, 1).checkQuorum(true), cStorage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage),
            new RawNodeStepper<String>(c, cStorage)
        );

        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());

        nt.isolate(ids[0]);

        nt.<String>action(ids[2], s -> s.node().campaign());

        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(STATE_CANDIDATE, c.basicStatus().softState().state());
        Assertions.assertEquals(b.basicStatus().hardState().term() + 1, c.basicStatus().hardState().term());

        // Vote again for safety
        nt.<String>action(ids[2], s -> s.node().campaign());

        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(STATE_CANDIDATE, c.basicStatus().softState().state());
        Assertions.assertEquals(b.basicStatus().hardState().term() + 2, c.basicStatus().hardState().term());

        nt.recover();
        nt.<String>action(ids[0], s -> s.node().sendHeartbeat());

        // Disrupt the leader so that the stuck peer is freed
        Assertions.assertEquals(STATE_FOLLOWER, a.basicStatus().softState().state());
        Assertions.assertEquals(a.basicStatus().hardState().term(), c.basicStatus().hardState().term());

        // Vote again, should become leader this time
        nt.<String>action(ids[2], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, c.basicStatus().softState().state());
    }

    @Test
    public void testNonPromotableVoterWithCheckQuorum() {
        UUID[] ids = idsBySize(2);
        Random rnd = new Random(seed());

        MemoryStorage aStorage = memoryStorage(ids[0], ids);
        RawNode<String> a = newTestRaft(newTestConfig(10, 1).checkQuorum(true), aStorage, rnd);

        MemoryStorage bStorage = memoryStorage(ids[1], new UUID[] {ids[0]});
        RawNode<String> b = newTestRaft(newTestConfig(10, 1).checkQuorum(true), bStorage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(a, aStorage),
            new RawNodeStepper<String>(b, bStorage)
        );

        b.randomizedElectionTimeout(b.electionTimeout() + 1);

        Assertions.assertFalse(b.promotable());

        for (int i = 0; i < b.electionTimeout(); i++)
            b.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, a.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, b.basicStatus().softState().state());
        Assertions.assertEquals(ids[0], b.basicStatus().softState().lead());
    }

    // TestDisruptiveFollower tests isolated follower,
    // with slow network incoming from leader, election times out
    // to become a candidate with an increased term. Then, the
    // candiate's response to late leader heartbeat forces the leader
    // to step down.
    @Test
    public void testDisruptiveFollower() {
        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], ids);
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1).checkQuorum(true), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], ids);
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1).checkQuorum(true), n2Storage);

        MemoryStorage n3Storage = memoryStorage(ids[2], ids);
        RawNode<String> n3 = newTestRaft(newTestConfig(10, 1).checkQuorum(true), n3Storage);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(n1, n1Storage),
            new RawNodeStepper<String>(n2, n2Storage),
            new RawNodeStepper<String>(n3, n3Storage)
        );

        nt.<String>action(ids[0], s -> s.node().campaign());

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateFollower
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n3.basicStatus().softState().state());

        // Server should advanceTicksForElection on restart;
        // this is to expedite campaign trigger when given larger
        // election timeouts (e.g. multi-datacenter deploy)
        // Or leader messages are being delayed while ticks elapse
        n3.randomizedElectionTimeout(n3.electionTimeout() + 2);

        for (int i = 0; i < n3.randomizedElectionTimeout() - 1; i++)
            n3.tick();

        // ideally, before last election tick elapses,
        // the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
        // from leader n1, and then resets its "electionElapsed"
        // however, last tick may elapse before receiving any
        // messages from leader, thus triggering campaign
        n3.tick();

        // n1 is still leader yet
        // while its heartbeat to candidate n3 is being delayed

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateCandidate
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_CANDIDATE, n3.basicStatus().softState().state());

        // check term
        // n1.Term == 2
        // n2.Term == 2
        // n3.Term == 3
        Assertions.assertEquals(2, n1.basicStatus().hardState().term());
        Assertions.assertEquals(2, n2.basicStatus().hardState().term());
        Assertions.assertEquals(3, n3.basicStatus().hardState().term());

        // while outgoing vote requests are still queued in n3,
        // leader heartbeat finally arrives at candidate n3
        // however, due to delayed network from leader, leader
        // heartbeat was sent with lower term than candidate's
        nt.send(msgFactory.newHeartbeatRequest(ids[0], ids[2], n1.basicStatus().hardState().term(), 0, null));

        // then candidate n3 responds with "pb.MsgAppResp" of higher term
        // and leader steps down from a message with higher term
        // this is to disrupt the current leader, so that candidate
        // with higher term can be freed with following election

        // check state
        // n1.state == StateFollower
        // n2.state == StateFollower
        // n3.state == StateCandidate
        Assertions.assertEquals(STATE_FOLLOWER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_CANDIDATE, n3.basicStatus().softState().state());

        // check term
        // n1.Term == 3
        // n2.Term == 2
        // n3.Term == 3
        Assertions.assertEquals(3, n1.basicStatus().hardState().term());
        Assertions.assertEquals(2, n2.basicStatus().hardState().term());
        Assertions.assertEquals(3, n3.basicStatus().hardState().term());
    }

    // TestDisruptiveFollowerPreVote tests isolated follower,
    // with slow network incoming from leader, election times out
    // to become a pre-candidate with less log than current leader.
    // Then pre-vote phase prevents this isolated node from forcing
    // current leader to step down, thus less disruptions.
    @Test
    public void testDisruptiveFollowerPreVote() {
        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], ids);
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1).checkQuorum(true).preVote(true), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], ids);
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1).checkQuorum(true).preVote(true), n2Storage);

        MemoryStorage n3Storage = memoryStorage(ids[2], ids);
        RawNode<String> n3 = newTestRaft(newTestConfig(10, 1).checkQuorum(true).preVote(true), n3Storage);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<String>(n1, n1Storage),
            new RawNodeStepper<String>(n2, n2Storage),
            new RawNodeStepper<String>(n3, n3Storage)
        );

        nt.<String>action(ids[0], s -> s.node().campaign());

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateFollower
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n3.basicStatus().softState().state());

        nt.isolate(ids[2]);

        nt.<String>action(ids[0], s -> s.node().propose("somedata"));
        nt.<String>action(ids[0], s -> s.node().propose("somedata"));
        nt.<String>action(ids[0], s -> s.node().propose("somedata"));

        nt.recover();

        nt.<String>action(ids[2], s -> s.node().campaign());

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StatePreCandidate
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_PRE_CANDIDATE, n3.basicStatus().softState().state());

        // check term
        // n1.Term == 2
        // n2.Term == 2
        // n3.Term == 2
        Assertions.assertEquals(2, n1.basicStatus().hardState().term());
        Assertions.assertEquals(2, n2.basicStatus().hardState().term());
        Assertions.assertEquals(2, n3.basicStatus().hardState().term());

        // delayed leader heartbeat does not force current leader to step down
        nt.send(msgFactory.newHeartbeatRequest(ids[0], ids[2], n1.basicStatus().hardState().term(), 0, null));
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
    }

    @Test
    public void testLeaderAppResp() {
        // initial progress: match = 0; next = 3
        class TestData {
            long idx;
            boolean reject;
            // progress
            long wMatch;
            long wNext;
            // message
            int wMsgNum;
            long wIdx;
            long wCommitted;

            TestData(
                long idx,
                boolean reject,
                long wMatch,
                long wNext,
                int wMsgNum,
                long wIdx,
                long wCommitted
            ) {
                this.idx = idx;
                this.reject = reject;
                this.wMatch = wMatch;
                this.wNext = wNext;
                this.wMsgNum = wMsgNum;
                this.wIdx = wIdx;
                this.wCommitted = wCommitted;
            }
        }

        TestData[] tests = new TestData[] {
            new TestData(3, true, 0, 3, 0, 0, 0),  // stale resp; no replies
            new TestData(2, true, 0, 2, 1, 1, 0),  // denied resp; leader does not commit; decrease next and send probing msg
            new TestData(2, false, 2, 4, 2, 2, 2), // accept resp; leader commits; broadcast with commit index
            new TestData(0, false, 0, 3, 0, 0, 0), // ignore heartbeat replies
        };

        for (TestData tt : tests) {
            // sm term is 2 after it becomes the leader.
            // thus the last log term must be 2 to be committed.
            MemoryStorage memoryStorage = memoryStorage(ids);
            memoryStorage.append(Arrays.asList(entry(1, 1), entry(2, 2)));

            RawNode<String> sm = newTestRaft(10, 1, memoryStorage);

            sm.becomeCandidate();
            sm.becomeLeader();
            sm.readMessages();

            sm.step(msgFactory.newAppendEntriesResponse(
                ids[1], ids[0], sm.basicStatus().hardState().term(), tt.idx, tt.reject, tt.idx));

            Progress p = sm.tracker().progress(ids[1]);

            Assertions.assertEquals(tt.wMatch, p.match());
            Assertions.assertEquals(tt.wNext, p.next());

            List<Message> msgs = sm.readMessages();
            Assertions.assertEquals(tt.wMsgNum, msgs.size());

            for (Message msg : msgs) {
                Assertions.assertEquals(MsgApp, msg.type());
                AppendEntriesRequest req = (AppendEntriesRequest)msg;

                Assertions.assertEquals(tt.wIdx, req.logIndex());
                Assertions.assertEquals(tt.wCommitted, req.committedIndex());
            }
        }
    }

    // When the leader receives a heartbeat tick, it should
    // send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries.
    @Test
    public void testBcastBeat() {
        long offset = 1000;

        // make a state machine with log.offset = 1000
        Snapshot s = new Snapshot(
            new SnapshotMetadata(
                ConfigState.bootstrap(Arrays.asList(ids), Collections.emptyList()),
                offset,
                1
            ),
            null
        );

        MemoryStorage storage = memoryStorage(ids[0], s);

        RawNode<String> sm = newTestRaft(10, 1, storage);

        sm.becomeCandidate();
        sm.becomeLeader();

        for (int i = 0; i < 10; i++)
            sm.propose("data-" + i);

        // Consume messages emitted after a propose.
        sm.readMessages();

        // slow follower
        sm.tracker().progress(ids[1]).maybeUpdate(5);

        // normal follower
        sm.tracker().progress(ids[2]).maybeUpdate(sm.raftLog().lastIndex());

        sm.sendHeartbeat();

        List<Message> msgs = sm.readMessages();
        Assertions.assertEquals(2, msgs.size());

        Map<UUID, Long> wantCommitMap = Map.of(
            ids[1], Math.min(sm.raftLog().committed(), sm.tracker().progress(ids[1]).match()),
            ids[2], Math.min(sm.raftLog().committed(), sm.tracker().progress(ids[2]).match())
        );

        for (Message m : msgs) {
            Assertions.assertEquals(MsgHeartbeat, m.type());
            HeartbeatRequest req = (HeartbeatRequest)m;

            Assertions.assertTrue(wantCommitMap.containsKey(m.to()));
            Assertions.assertEquals(wantCommitMap.get(m.to()), req.commitIndex());
        }
    }

    // tests the output of the state machine when calling sendHeartbeat.
    @Test
    public void testRecvMsgBeat() {
        for (StateType state : new StateType[] {STATE_LEADER, STATE_CANDIDATE, STATE_FOLLOWER}) {
            MemoryStorage memoryStorage = memoryStorage(ids, new HardState(2, null, 0));
            memoryStorage.append(Arrays.asList(entry(1, 1), entry(2, 2)));

            RawNode<String> sm = newTestRaft(10, 1, memoryStorage);

            switch (state) {
                case STATE_FOLLOWER:
                    sm.becomeFollower(2, null);

                    break;

                case STATE_CANDIDATE:
                    sm.becomeCandidate();

                    break;

                case STATE_LEADER:
                    sm.becomeCandidate();
                    sm.becomeLeader();

                    break;
            }

            sm.sendHeartbeat();

            List<Message> msgs = sm.readMessages();

            // candidate and follower should ignore sendHeartbeat.
            Assertions.assertEquals(state == STATE_LEADER ? 2 : 0, msgs.size());

            for (Message m : msgs)
                Assertions.assertEquals(MsgHeartbeat, m.type());
        }
    }

    @Test
    public void testLeaderIncreaseNext() {
        List<Entry> previousEnts = Arrays.asList(entry(1, 1), entry(1, 2), entry(1, 3));

        class TestData {
            Progress.ProgressState state;
            long next;
            long wNext;

            TestData(Progress.ProgressState state, long next, long wNext) {
                this.state = state;
                this.next = next;
                this.wNext = wNext;
            }
        }

        TestData[] tests = new TestData[] {
            // state replicate, optimistically increase next
            // previous entries + noop entry + propose + 1
            new TestData(Progress.ProgressState.StateReplicate, 2, previousEnts.size() + 1 + 1 + 1),
            // state probe, not optimistically increase next
            new TestData(Progress.ProgressState.StateProbe, 2, 2),
        };

        UUID[] ids = idsBySize(2);

        for (TestData tt : tests) {
            MemoryStorage memoryStorage = memoryStorage(ids);
            memoryStorage.append(previousEnts);

            RawNode<String> sm = newTestRaft(10, 1, memoryStorage);

            sm.becomeCandidate();
            sm.becomeLeader();

            Progress p = sm.tracker().progress(ids[1]);

            if (tt.state == Progress.ProgressState.StateProbe)
                p.becomeProbe();
            else
                p.becomeReplicate();

            p.maybeUpdate(tt.next - 1);

            sm.propose("somedata");

            Assertions.assertEquals(tt.wNext, p.next());
        }
    }

    @Test
    public void testSendAppendForProgressProbe() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();

        r.tracker().progress(ids[1]).becomeProbe();

        // each round is a heartbeat
        for (int i = 0; i < 3; i++) {
            if (i == 0) {
                // we expect that Raft will only send out one MsgApp on the first
                // loop. After that, the follower is paused until a heartbeat response is
                // received.
                r.propose("somedata");

                List<Message> msgs = r.readMessages();
                Assertions.assertEquals(1, msgs.size());
                Assertions.assertEquals(MsgApp, msgs.get(0).type());

                AppendEntriesRequest req = (AppendEntriesRequest)msgs.get(0);
                Assertions.assertEquals(0, req.logIndex());
            }

            Assertions.assertTrue(r.tracker().progress(ids[1]).probeSent());

            for (int j = 0; j < 10; j++) {
                r.propose("somedata");

                Assertions.assertTrue(r.readMessages().isEmpty());
            }

            // do a heartbeat
            for (int j = 0; j < r.heartbeatTimeout(); j++)
                r.tick();

            Assertions.assertTrue(r.tracker().progress(ids[1]).probeSent());

            // consume the heartbeat
            List<Message> msgs = r.readMessages();
            Assertions.assertEquals(1, msgs.size());
            Assertions.assertEquals(MsgHeartbeat, msgs.get(0).type());
        }

        // a heartbeat response will allow another message to be sent
        r.step(msgFactory.newHeartbeatResponse(ids[1], ids[0], r.basicStatus().hardState().term(), null));

        List<Message> msgs = r.readMessages();
        Assertions.assertEquals(1, msgs.size());
        Assertions.assertEquals(MsgApp, msgs.get(0).type());

        AppendEntriesRequest req = (AppendEntriesRequest)msgs.get(0);
        Assertions.assertEquals(0, req.logIndex());
        Assertions.assertTrue(r.tracker().progress(ids[1]).probeSent());
    }

    @Test
    public void testSendAppendForProgressReplicate() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();

        r.tracker().progress(ids[1]).becomeReplicate();

        for (int i = 0; i < 10; i++) {
            r.propose("somedata");

            List<Message> msgs = r.readMessages();

            Assertions.assertEquals(1, msgs.size());
        }
    }

    @Test
    public void testSendAppendForProgressSnapshot() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();

        r.tracker().progress(ids[1]).becomeSnapshot(10);

        for (int i = 0; i < 10; i++) {
            r.propose("somedata");

            List<Message> msgs = r.readMessages();
            Assertions.assertTrue(msgs.isEmpty());
        }
    }

    @Test
    public void testRecvMsgUnreachable() {
        UUID[] ids = idsBySize(2);

        List<Entry> previousEnts = Arrays.asList(entry(1, 1), entry(1, 2), entry(1, 3));

        MemoryStorage s = memoryStorage(ids);
        s.append(previousEnts);

        RawNode<String> r = newTestRaft(10, 1, s);

        r.becomeCandidate();
        r.becomeLeader();
        r.readMessages();

        // set node 2 to state replicate
        Progress p = r.tracker().progress(ids[1]);
        p.maybeUpdate(3);
        p.becomeReplicate();
        p.optimisticUpdate(5);

        r.reportUnreachable(ids[1]);

        Assertions.assertEquals(Progress.ProgressState.StateProbe, p.state());
        Assertions.assertEquals(p.match() + 1, p.next());
    }

    // TestStepConfig tests that when raft step msgProp in EntryConfChange type,
    // it appends the entry to log and sets pendingConf to be true.
    @Test
    public void testStepConfig() {
        UUID[] ids = idsBySize(2);

        // a raft that cannot make progress
        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.becomeCandidate();
        r.becomeLeader();

        long idx = r.raftLog().lastIndex();
        r.proposeConfChange(new ConfChangeSingle(UUID.randomUUID(), ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        Assertions.assertEquals(idx + 1, r.raftLog().lastIndex());
        Assertions.assertEquals(idx + 1, r.pendingConfIndex());
    }

    // TestStepIgnoreConfig tests that if raft step the second msgProp in
    // EntryConfChange type when the first one is uncommitted, the node will set
    // the proposal to noop and keep its original state.
    @Test
    public void testStepIgnoreConfig() throws Exception {
        UUID[] ids = idsBySize(2);

        // a raft that cannot make progress
        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.becomeCandidate();
        r.becomeLeader();

        r.proposeConfChange(new ConfChangeSingle(UUID.randomUUID(), ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        long idx = r.raftLog().lastIndex();

        long pendingConfIdx = r.pendingConfIndex();

        Assertions.assertThrows(InvalidConfigTransitionException.class, () -> {
            r.proposeConfChange(new ConfChangeSingle(UUID.randomUUID(), ConfChangeSingle.ConfChangeType.ConfChangeAddNode));
        });

        List<Entry> entries = r.raftLog().entries(idx + 1, Long.MAX_VALUE);
        Assertions.assertTrue(entries.isEmpty());
        Assertions.assertEquals(pendingConfIdx, r.pendingConfIndex());
    }

    // TestAddNode tests that addNode could update nodes correctly.
    @Test
    public void testAddNode() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(new UUID[] {ids[0]}, 10, 1);

        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        Set<UUID> voters = r.tracker().config().voters().ids();
        Assertions.assertEquals(2, voters.size());
        Assertions.assertTrue(voters.contains(ids[0]));
        Assertions.assertTrue(voters.contains(ids[1]));
    }

    // TestAddLearner tests that addLearner could update nodes correctly.
    public void testAddLearner() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(new UUID[] {ids[0]}, 10, 1);

        // Add new learner peer.
        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddLearnerNode));
        Assertions.assertFalse(r.isLearner());

        Set<UUID> nodes = r.tracker().config().learners();
        Assertions.assertEquals(1, nodes.size());
        Assertions.assertTrue(nodes.contains(ids[1]));
        Assertions.assertTrue(r.tracker().progress(ids[1]).isLearner());

        // Promote peer to voter.
        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));
        Assertions.assertFalse(r.tracker().progress(ids[1]).isLearner());

        // Demote r.
        r.applyConfChange(new ConfChangeSingle(ids[0], ConfChangeSingle.ConfChangeType.ConfChangeAddLearnerNode));
        Assertions.assertTrue(r.tracker().progress(ids[0]).isLearner());
        Assertions.assertTrue(r.isLearner());

        // Promote r again.
        r.applyConfChange(new ConfChangeSingle(ids[0], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));
        Assertions.assertFalse(r.tracker().progress(ids[0]).isLearner());
        Assertions.assertFalse(r.isLearner());
    }

    // TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
    // immediately when checkQuorum is set.
    @Test
    public void testAddNodeCheckQuorum() {
        UUID[] ids = idsBySize(2);

        RawNode<String> r = newTestRaft(newTestConfig(10, 1).checkQuorum(true), memoryStorage(new UUID[] {ids[0]}));

        r.becomeCandidate();
        r.becomeLeader();

        for (int i = 0; i < r.electionTimeout() - 1; i++)
            r.tick();

        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        // This tick will reach electionTimeout, which triggers a quorum check.
        r.tick();

        // Node 1 should still be the leader after a single tick.
        Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());

        // After another electionTimeout ticks without hearing from node 2,
        // node 1 should step down.
        for (int i = 0; i < r.electionTimeout(); i++)
            r.tick();

        Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());
    }

    // TestRemoveNode tests that removeNode could update nodes and
    // and removed list correctly.
    @Test
    public void testRemoveNode() {
        UUID[] ids = idsBySize(2);
        RawNode<String> r = newTestRaft(ids, 10, 1);

        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));

        Set<UUID> voters = r.tracker().config().voters().ids();
        Assertions.assertEquals(1, voters.size());
        Assertions.assertTrue(voters.contains(ids[0]));

        // Removing the remaining voter will throw an exception.
        Assertions.assertThrows(InvalidConfigTransitionException.class, () -> {
            r.applyConfChange(new ConfChangeSingle(ids[0], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));
        });
    }

    // TestRemoveLearner tests that removeNode could update nodes and
    // and removed list correctly.
    @Test
    public void testRemoveLearner() {
        UUID[] ids = idsBySize(2);

        MemoryStorage memoryStorage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});

        RawNode<String> r = newTestRaft(10, 1, memoryStorage);
        Assertions.assertTrue(r.tracker().progress(ids[1]).isLearner());

        r.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));

        Set<UUID> voters = r.tracker().config().voters().ids();
        Assertions.assertEquals(1, voters.size());
        Assertions.assertTrue(voters.contains(ids[0]));

        Set<UUID> learners = r.tracker().config().learners();
        Assertions.assertTrue(learners.isEmpty());

        // Removing the remaining voter will throw an exception.
        Assertions.assertThrows(InvalidConfigTransitionException.class, () -> {
            r.applyConfChange(new ConfChangeSingle(ids[0], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));
        });
    }

    @Test
    public void testPromotable() {
        UUID[] ids = idsBySize(3);

        for (UUID[] testIds : new UUID[][] {
            new UUID[] {ids[0]},
            ids,
            new UUID[] {},
            new UUID[] {ids[1], ids[2]}
        }) {
            RawNode<String> r = newTestRaft(5, 1, memoryStorage(ids[0], testIds));
            Assertions.assertEquals(Arrays.binarySearch(testIds, ids[0]) >= 0, r.promotable());
        }
    }

    @Test
    public void testCampaignWhileLeader() {
        checkCampaignWhileLeader(false);
    }

    @Test
    public void testPreCampaignWhileLeader() {
        checkCampaignWhileLeader(true);
    }

    private void checkCampaignWhileLeader(boolean preVote) {
        UUID[] ids = idsBySize(1);

        RawNode<String> r = newTestRaft(newTestConfig(5, 1).preVote(preVote), memoryStorage(ids));
        Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());

        // We don't call campaign() directly because it comes after the check
        // for our current state.
        r.campaign();
        Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());

        long term = r.basicStatus().hardState().term();
        r.campaign();

        Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state());
        Assertions.assertEquals(term, r.basicStatus().hardState().term());
    }

    // TestCommitAfterRemoveNode verifies that pending commands can become
    // committed when a config change reduces the quorum requirements.
    @Test
    public void testCommitAfterRemoveNode() {
        // Create a cluster with two nodes.
        UUID[] ids = idsBySize(2);

        MemoryStorage s = memoryStorage(ids);
        RawNode<String> r = newTestRaft(5, 1, s);

        r.becomeCandidate();
        r.becomeLeader();

        // Begin to remove the second node.
        ConfChange cc = new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode);

        r.proposeConfChange(cc);

        // Stabilize the log and make sure nothing is committed yet.
        List<Entry> ents = nextEntries(r, s);

        Assertions.assertTrue(ents.isEmpty());

        long ccIndex = r.raftLog().lastIndex();

        // While the config change is pending, make another proposal.
        r.propose("hello");

        // Node 2 acknowledges the config change, committing it.
        r.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], r.basicStatus().hardState().term(), ccIndex, false, 0));

        ents = nextEntries(r, s);
        Assertions.assertEquals(2, ents.size());
        Assertions.assertEquals(ENTRY_DATA, ents.get(0).type());
        Assertions.assertNull(ents.get(0).<UserData>data().data());
        Assertions.assertEquals(ENTRY_CONF_CHANGE, ents.get(1).type());

        // Apply the config change. This reduces quorum requirements so the
        // pending command can now commit.
        r.applyConfChange(cc);

        ents = nextEntries(r, s);

        Assertions.assertEquals(1, ents.size());
        Assertions.assertEquals(ENTRY_DATA, ents.get(0).type());
        Assertions.assertEquals("hello", ents.get(0).<UserData>data().data());
    }

    // TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
    // based on uncommitted entries.
    public void testNewLeaderPendingConfig() {
        UUID[] ids = idsBySize(2);

        for (boolean addEntry : new boolean[] {false, true}) {
            MemoryStorage s = memoryStorage(ids);

            if (addEntry)
                s.append(Arrays.asList(entry(1, 1)));

            RawNode<String> r = newTestRaft(ids, 10, 1);

            r.becomeCandidate();
            r.becomeLeader();

            Assertions.assertEquals(addEntry ? 1 : 0, r.pendingConfIndex());
        }
    }

    // TestLeaderTransferToUpToDateNode verifies transferring should succeed
    // if the transferee has the most up-to-date log entries when transfer starts.
    @Test
    public void testLeaderTransferToUpToDateNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();
        Assertions.assertEquals(ids[0], lead.basicStatus().softState().lead());
        Assertions.assertEquals(STATE_LEADER, lead.basicStatus().softState().state());

        // Transfer leadership to 2.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[1]));

        checkLeaderTransferState(lead, STATE_FOLLOWER, ids[1]);

        // After some log replication, transfer leadership back to 1.
        nt.<String>action(ids[1], s -> s.node().propose("somedata"));

        nt.<String>action(ids[1], s -> s.node().transferLeader(ids[0]));

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    // TestLeaderTransferToUpToDateNodeFromFollower verifies that leader transferring can only be invoked
    // on group leader.
    @Test
    public void testLeaderTransferToUpToDateNodeFromFollower() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();
        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();
        Assertions.assertEquals(ids[0], lead.basicStatus().softState().lead());
        Assertions.assertEquals(STATE_LEADER, lead.basicStatus().softState().state());

        RawNode<String> follower = nt.<RawNodeStepper<String>>peer(ids[1]).node();
        Assertions.assertEquals(ids[0], follower.basicStatus().softState().lead());
        Assertions.assertEquals(STATE_FOLLOWER, follower.basicStatus().softState().state());

        Assertions.assertThrows(NotLeaderException.class, () -> {
            nt.<String>action(ids[1], s -> s.node().transferLeader(ids[1]));
        });

        // Check status did not change.
        Assertions.assertEquals(ids[0], lead.basicStatus().softState().lead());
        Assertions.assertEquals(STATE_LEADER, lead.basicStatus().softState().state());
        Assertions.assertEquals(ids[0], follower.basicStatus().softState().lead());
        Assertions.assertEquals(STATE_FOLLOWER, follower.basicStatus().softState().state());
    }

    // TestLeaderTransferWithCheckQuorum ensures transferring leader still works
    // even the current leader is still under its leader lease
    @Test
    public void testLeaderTransferWithCheckQuorum() {
        Network nt = newNetwork(c -> c.checkQuorum(true), entries(), entries(), entries());
        UUID[] ids = nt.ids();

        for (int i = 0; i < ids.length; i++) {
            UUID id = ids[i];

            RawNode<String> node = nt.<RawNodeStepper<String>>peer(id).node();
            node.randomizedElectionTimeout(node.electionTimeout() + i + 1);
        }

        // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
        RawNode<String> f = nt.<RawNodeStepper<String>>peer(ids[1]).node();

        for (int i = 0; i < f.electionTimeout(); i++)
            f.tick();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();
        Assertions.assertEquals(STATE_LEADER, lead.basicStatus().softState().state());
        Assertions.assertEquals(ids[0], lead.basicStatus().softState().lead());

        // Transfer leadership to 2.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[1]));

        checkLeaderTransferState(lead, STATE_FOLLOWER, ids[1]);

        // After some log replication, transfer leadership back to 1.
        nt.<String>action(ids[1], s -> s.node().propose("somedata"));

        nt.<String>action(ids[1], s -> s.node().transferLeader(ids[0]));

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    @Test
    public void testLeaderTransferToSlowFollower() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        nt.isolate(ids[2]);

        nt.<String>action(ids[0], s -> s.node().propose(""));

        nt.recover();

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();
        Assertions.assertEquals(1, lead.tracker().progress(ids[2]).match());

        // Transfer leadership to 3 when node 3 is lack of log.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));

        checkLeaderTransferState(lead, STATE_FOLLOWER, ids[2]);
    }

    @Test
    public void testLeaderTransferToSelf() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        // Transfer leadership to self, there will be noop.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[0]));
        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    @Test
    public void testLeaderTransferToNonExistingNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        // Transfer leadership to non-existing node, there will be noop.
        nt.<String>action(ids[0], s -> s.node().transferLeader(UUID.randomUUID()));
        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    @Test
    public void testLeaderTransferTimeout() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        // Transfer leadership to isolated node, wait for timeout.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        for (int i = 0; i < lead.heartbeatTimeout(); i++)
            lead.tick();

        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        for (int i = 0; i < lead.electionTimeout() - lead.heartbeatTimeout(); i++)
            lead.tick();

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    @Test
    public void testLeaderTransferIgnoreProposal() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        // Transfer leadership to isolated node to let transfer pending, then send proposal.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        nt.<String>action(ids[0], s -> {
            Assertions.assertThrows(TransferLeaderException.class, () -> s.node().propose(""));
            ;
        });

        Assertions.assertEquals(1, lead.tracker().progress(ids[0]).match());
    }

    @Test
    public void testLeaderTransferReceiveHigherTermVote() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        // Transfer leadership to isolated node to let transfer pending.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        nt.<String>action(ids[1], s -> s.node().campaign());

        checkLeaderTransferState(lead, STATE_FOLLOWER, ids[1]);
    }

    @Test
    public void testLeaderTransferRemoveNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.ignore(MsgTimeoutNow);

        // The leadTransferee is removed when leadship transferring.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        lead.applyConfChange(new ConfChangeSingle(ids[2], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    @Test
    public void testLeaderTransferDemoteNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.ignore(MsgTimeoutNow);

        // The leadTransferee is demoted when leadship transferring.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        lead.applyConfChange(new ConfChangeJoint(Arrays.asList(
                new ConfChangeSingle(ids[2], ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode),
                new ConfChangeSingle(ids[2], ConfChangeSingle.ConfChangeType.ConfChangeAddLearnerNode)
        )));

        // Make the Raft group commit the LeaveJoint entry.
        lead.applyConfChange(new ConfChangeJoint(Collections.emptyList()));

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    // TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
    @Test
    public void testLeaderTransferBack() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        // Transfer leadership back to self.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[0]));

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    // TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
    // when last transfer is pending.
    @Test
    public void testLeaderTransferSecondTransferToAnotherNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        // Transfer leadership to another node.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[1]));

        checkLeaderTransferState(lead, STATE_FOLLOWER, ids[1]);
    }

    // TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
    // to the same node should not extend the timeout while the first one is pending.
    @Test
    public void testLeaderTransferSecondTransferToSameNode() {
        Network nt = newNetwork(null, entries(), entries(), entries());
        UUID[] ids = nt.ids();

        nt.<String>action(ids[0], s -> s.node().campaign());

        RawNode<String> lead = nt.<RawNodeStepper<String>>peer(ids[0]).node();

        nt.isolate(ids[2]);

        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));
        Assertions.assertEquals(ids[2], lead.basicStatus().leadTransferee());

        for (int i = 0; i < lead.heartbeatTimeout(); i++)
            lead.tick();

        // Second transfer leadership request to the same node.
        nt.<String>action(ids[0], s -> s.node().transferLeader(ids[2]));

        for (int i = 0; i < lead.electionTimeout() - lead.heartbeatTimeout(); i++)
            lead.tick();

        checkLeaderTransferState(lead, STATE_LEADER, ids[0]);
    }

    // TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
    // a node that has been removed from the group, nothing happens.
    // (previously, if the node also got votes, it would panic as it
    // transitioned to StateLeader)
    @Test
    public void testTransferNonMember() {
        UUID[] ids = idsBySize(4);

        RawNode<String> r = newTestRaft(5, 1, memoryStorage(ids[0], new UUID[] {ids[1], ids[2], ids[3]}));

        long term = r.basicStatus().hardState().term();

        r.step(msgFactory.newTimeoutNowRequest(ids[1], ids[0], term));

        r.step(msgFactory.newVoteResponse(ids[1], ids[0], false, term, false));
        r.step(msgFactory.newVoteResponse(ids[2], ids[0], false, term, false));

        Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());
    }

    // TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
    // that has been partitioned away (and fallen behind) rejoins the cluster at
    // about the same time the leader node gets partitioned away.
    // Previously the cluster would come to a standstill when run with PreVote
    // enabled.
    @Test
    public void testNodeWithSmallerTermCanCompleteElection() {
        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], ids);
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1).preVote(true), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], ids);
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1).preVote(true), n2Storage);

        MemoryStorage n3Storage = memoryStorage(ids[2], ids);
        RawNode<String> n3 = newTestRaft(newTestConfig(10, 1).preVote(true), n3Storage);

        // cause a network partition to isolate node 3
        Network nt = new Network(rnd,
            new RawNodeStepper<>(n1, n1Storage),
            new RawNodeStepper<>(n2, n2Storage),
            new RawNodeStepper<>(n3, n3Storage)
        );

        nt.cut(ids[0], ids[2]);
        nt.cut(ids[1], ids[2]);

        nt.<String>action(ids[0], s -> s.node().campaign());

        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());

        nt.<String>action(ids[2], s -> s.node().campaign());
        Assertions.assertEquals(STATE_PRE_CANDIDATE, n3.basicStatus().softState().state());

        nt.<String>action(ids[1], s -> s.node().campaign());

        // check whether the term values are expected
        // n1.Term == 3
        // n2.Term == 3
        // n3.Term == 1
        Assertions.assertEquals(3, n1.basicStatus().hardState().term());
        Assertions.assertEquals(3, n2.basicStatus().hardState().term());
        Assertions.assertEquals(1, n3.basicStatus().hardState().term());

        // check state
        // n1 == follower
        // n2 == leader
        // n3 == pre-candidate
        Assertions.assertEquals(STATE_FOLLOWER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_LEADER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_PRE_CANDIDATE, n3.basicStatus().softState().state());

        // recover the network then immediately isolate b which is currently
        // the leader, this is to emulate the crash of b.
        nt.recover();
        nt.isolate(ids[1]);

        // call for election
        nt.<String>action(ids[0], s -> s.node().campaign());
        nt.<String>action(ids[2], s -> s.node().campaign());

        // do we have a leader?
        Assertions.assertFalse(
            n1.basicStatus().softState().state() != STATE_LEADER &&
            n2.basicStatus().softState().state() != STATE_LEADER);
    }

    // TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
    // election in next round.
    @Test
    public void testPreVoteWithSplitVote() {
        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], ids);
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1).preVote(true), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], ids);
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1).preVote(true), n2Storage);

        MemoryStorage n3Storage = memoryStorage(ids[2], ids);
        RawNode<String> n3 = newTestRaft(newTestConfig(10, 1).preVote(true), n3Storage);

        // cause a network partition to isolate node 3
        Network nt = new Network(rnd,
            new RawNodeStepper<>(n1, n1Storage),
            new RawNodeStepper<>(n2, n2Storage),
            new RawNodeStepper<>(n3, n3Storage)
        );

        nt.<String>action(ids[0], s -> s.node().campaign());

        // simulate leader down. followers start split vote.
        nt.isolate(ids[0]);

        nt.multiAction(() -> {
            n2.campaign();
            n3.campaign();
        });

        // check whether the term values are expected
        // n2.Term == 3
        // n3.Term == 3
        Assertions.assertEquals(3, n2.basicStatus().hardState().term());
        Assertions.assertEquals(3, n3.basicStatus().hardState().term());

        // check state
        // n2 == candidate
        // n3 == candidate
        Assertions.assertEquals(STATE_CANDIDATE, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_CANDIDATE, n3.basicStatus().softState().state());

        // node 2 election timeout first
        nt.<String>action(ids[1], s -> s.node().campaign());

        // check whether the term values are expected
        // n2.Term == 4
        // n3.Term == 4
        Assertions.assertEquals(4, n2.basicStatus().hardState().term());
        Assertions.assertEquals(4, n3.basicStatus().hardState().term());

        // check state
        // n2 == leader
        // n3 == follower
        Assertions.assertEquals(STATE_LEADER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n3.basicStatus().softState().state());
    }

    // TestPreVoteWithCheckQuorum ensures that after a node become pre-candidate,
    // it will checkQuorum correctly.
    @Test
    public void testPreVoteWithCheckQuorum() {
        Random rnd = new Random(seed());

        MemoryStorage n1Storage = memoryStorage(ids[0], ids);
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1).preVote(true).checkQuorum(true), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], ids);
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1).preVote(true).checkQuorum(true), n2Storage);

        MemoryStorage n3Storage = memoryStorage(ids[2], ids);
        RawNode<String> n3 = newTestRaft(newTestConfig(10, 1).preVote(true).checkQuorum(true), n3Storage);

        // cause a network partition to isolate node 3
        Network nt = new Network(rnd,
            new RawNodeStepper<>(n1, n1Storage),
            new RawNodeStepper<>(n2, n2Storage),
            new RawNodeStepper<>(n3, n3Storage)
        );

        nt.<String>action(ids[0], s -> s.node().campaign());

        // isolate node 1. node 2 and node 3 have leader info
        nt.isolate(ids[0]);

        // check state
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n3.basicStatus().softState().state());

        // node 2 will ignore node 3's PreVote
        nt.<String>action(ids[2], s -> s.node().campaign());
        nt.<String>action(ids[1], s -> s.node().campaign());

        // Do we have a leader?
        Assertions.assertEquals(STATE_LEADER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n3.basicStatus().softState().state());
    }

    // TestLearnerCampaign verifies that a learner won't campaign even if it receives
    // a MsgHup or MsgTimeoutNow.
    @Test
    public void testLearnerCampaign() {
        Random rnd = new Random(seed());
        UUID[] ids = idsBySize(2);

        MemoryStorage n1Storage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n1 = newTestRaft(10, 1, n1Storage, rnd);

        MemoryStorage n2Storage = memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n2 = newTestRaft(10, 1, n2Storage, rnd);

        Network nt = new Network(
            rnd,
            new RawNodeStepper<>(n1, n1Storage),
            new RawNodeStepper<>(n2, n2Storage)
        );

        nt.<String>action(ids[1], s -> s.node().campaign());

        Assertions.assertTrue(n2.isLearner());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());

        nt.<String>action(ids[0], s -> s.node().campaign());
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(ids[0], n1.basicStatus().softState().lead());

        // NB: TransferLeader already checks that the recipient is not a learner, but
        // the check could have happened by the time the recipient becomes a learner,
        // in which case it will receive MsgTimeoutNow as in this test case and we
        // verify that it's ignored.
        nt.send(msgFactory.newTimeoutNowRequest(ids[0], ids[1], n1.basicStatus().hardState().term()));

        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());
        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
    }

    private void checkLeaderTransferState(RawNode<String> r, StateType state, UUID lead) {
        Assertions.assertEquals(state, r.basicStatus().softState().state());
        Assertions.assertEquals(lead, r.basicStatus().softState().lead());

        Assertions.assertNull(r.basicStatus().leadTransferee());
    }
}
