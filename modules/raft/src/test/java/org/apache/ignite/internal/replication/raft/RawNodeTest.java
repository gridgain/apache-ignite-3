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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.message.VoteResponse;
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
import static org.apache.ignite.internal.replication.raft.storage.Entry.EntryType.ENTRY_DATA;

/**
 * Intentionally skipped tests:
 * TestProposalByProxy() - we decided not to implement proposal forwarding for now.
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

        nt.<String>action(ids[0], s -> s.node().bcastHeartbeat());

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
        tt.<String>action(ids[1], s -> s.node().bcastHeartbeat());
        
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
        tt.<String>action(ids[2], s -> s.node().bcastHeartbeat());

        String data = "force follower";

        // send a proposal to 3 to flush out a MsgApp to 1
        tt.<String>action(ids[2], s -> s.node().propose(data));

        // send heartbeat; flush out commit
        tt.<String>action(ids[2], s -> s.node().bcastHeartbeat());

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
            UUID[] ids = new UUID[] {UUID.randomUUID()};

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
}
