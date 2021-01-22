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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.replication.raft.StateType.STATE_CANDIDATE;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_FOLLOWER;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_LEADER;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_PRE_CANDIDATE;

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

        MemoryStorage n1Storage = memoryStorage(ids[0], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n1 = newTestRaft(newTestConfig(10, 1), n1Storage);

        MemoryStorage n2Storage = memoryStorage(ids[1], new UUID[] {ids[0]}, new UUID[] {ids[1]});
        RawNode<String> n2 = newTestRaft(newTestConfig(10, 1), n2Storage);

        n1.becomeFollower(1, null);
        n2.becomeFollower(1, null);

        Network nt = new Network(new RawNodeStepper<>(n1, n1Storage), new RawNodeStepper<>(n2, n2Storage));

        Assertions.assertNotEquals(STATE_LEADER, n1.basicStatus().softState().state());

        // n1 should become leader
        n1.randomizedElectionTimeout(n1.electionTimeout());
        
        for (int i = 0; i < n1.electionTimeout(); i++)
            n1.tick();

        Assertions.assertEquals(STATE_LEADER, n1.basicStatus().softState().state());
        Assertions.assertEquals(STATE_FOLLOWER, n2.basicStatus().softState().state());

        nt.<RawNodeStepper<String>>action(ids[0], s -> s.node().bcastHeartbeat());

        n1.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));
        n2.applyConfChange(new ConfChangeSingle(ids[1], ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        Assertions.assertFalse(n2.isLearner());

        // n2 start election, should become leader
        n2.randomizedElectionTimeout(n2.electionTimeout());

        nt.<RawNodeStepper<String>>action(ids[1], s -> {
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
            n.<RawNodeStepper<?>>action(campaignerID, s -> s.node().campaign());

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
            n.<RawNodeStepper<String>>action(n.ids()[0], s -> s.node().campaign());

            RawNode<String> node = n.<RawNodeStepper<String>>peer(n.ids()[0]).node();

            Assertions.assertEquals(STATE_FOLLOWER, node.basicStatus().softState().state());
            Assertions.assertEquals(2, node.basicStatus().hardState().term());

            // Node 1 campaigns again with a higher term. This time it succeeds.
            n.<RawNodeStepper<String>>action(n.ids()[0], s -> s.node().campaign());

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

                Assertions.assertEquals(Entry.EntryType.ENTRY_DATA, entry.type());
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
            tt.n.<RawNodeStepper<String>>action(tt.n.ids()[0], s -> s.node().campaign());

            for (NodeAction action : tt.actions)
                tt.n.action(tt.n.ids()[action.idx], action);

            for (UUID peerId : tt.n.ids()) {
                RawNodeStepper<String> sm = tt.n.peer(peerId);

                Assertions.assertEquals(tt.wcommmitted, sm.node().raftLog().committed());

                List<String> committed = new ArrayList<>();

                for (Entry e : nextEntries(sm.node(), tt.n.storage(peerId))) {
                    if (e.data() instanceof UserData) {
                        UserData<String> d = (UserData<String>)e.data();

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

}
