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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.message.VoteRequest;
import org.apache.ignite.internal.replication.raft.message.VoteResponse;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.helpers.MessageFormatter;

import static org.apache.ignite.internal.replication.raft.StateType.STATE_CANDIDATE;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_FOLLOWER;
import static org.apache.ignite.internal.replication.raft.StateType.STATE_LEADER;

/**
 * This file contains tests which verify that the scenarios described
 * in the raft paper (https://raft.github.io/raft.pdf) are
 * handled by the raft implementation correctly. Each test focuses on
 * several sentences written in the paper. This could help us to prevent
 * most implementation bugs.
 *
 * Each test is composed of three parts: init, test and check.
 * Init part uses simple and understandable way to simulate the init state.
 * Test part uses Step function to generate the scenario. Check part checks
 * outgoing messages and state.
 */
public class RaftPaperTest extends AbstractRaftTest {
    /** */
    private final Comparator<Message> destinationComparator = Comparator.comparing(Message::to);

    @Test
    public void testFollowerUpdateTermFromMessage() {
        checkUpdateTermFromMessage(STATE_FOLLOWER);
    }

    @Test
    public void testCandidateUpdateTermFromMessage() {
        checkUpdateTermFromMessage(STATE_CANDIDATE);
    }

    @Test
    public void testLeaderUpdateTermFromMessage() {
        checkUpdateTermFromMessage(STATE_LEADER);
    }

    // testUpdateTermFromMessage tests that if one server’s current term is
    // smaller than the other’s, then it updates its current term to the larger
    // value. If a candidate or leader discovers that its term is out of date,
    // it immediately reverts to follower state.
    // Reference: section 5.1
    private void checkUpdateTermFromMessage(StateType state) {
        RawNode<?> r = newTestRaft(ids, 10, 1);

        switch (state) {
            case STATE_FOLLOWER:
                r.becomeFollower(1, ids[1]);

                break;

            case STATE_CANDIDATE:
                r.becomeCandidate();

                break;

            case STATE_LEADER:
                r.becomeCandidate();
                r.becomeLeader();

                break;
        }

        long newTerm = r.basicStatus().hardState().term() + 1;

        r.step(msgFactory.newAppendEntriesRequest(
            ids[1],
            r.basicStatus().id(),
            newTerm,
            4,
            2,
            Collections.emptyList(),
            0
        ));

        BasicStatus status = r.basicStatus();

        Assertions.assertEquals(newTerm, status.hardState().term());
        Assertions.assertEquals(STATE_FOLLOWER, status.softState().state());
    }

    // TestRejectStaleTermMessage tests that if a server receives a request with
    // a stale term number, it rejects the request.
    // Our implementation ignores the request instead.
    // Reference: section 5.1
    // TODO agoncharuk this test looks insufficient. There is plenty of logic in stepInternal,
    // TODO agoncharuk  need to check that the state is not updated in response to all stale messages.
    @Test
    public void testRejectStaleTermMessage() {
        AtomicBoolean called = new AtomicBoolean();

        StepFunction fakeStep = new StepFunction() {
            @Override public void accept(Message msg) {
                called.set(true);
            }
        };

        long term = 2;

        RawNode<?> r = newTestRaft(10, 1, memoryStorage(ids, new HardState(term, null, 0)));
        r.step = fakeStep;

        r.step(msgFactory.newAppendEntriesRequest(
            ids[1],
            r.basicStatus().id(),
            term - 1,
            4,
            2,
            Collections.emptyList(),
            0));

        Assertions.assertFalse(called.get());
    }

    // TestStartAsFollower tests that when servers start up, they begin as followers.
    // Reference: section 5.2
    @Test
    public void testStartAsFollowerEmptyState() {
        RawNode<?> r = newTestRaft(ids, 10, 1);

        Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());
    }

    @Test
    public void testStartAsFollowerVotedForSelf() {
        RawNode<?> r = newTestRaft(10, 1, memoryStorage(ids, new HardState(3, ids[0], 0)));

        Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());
    }

    // TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
    // it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
    // as heartbeat to all followers.
    // Reference: section 5.2
    @Test
    public void testLeaderBcastBeat() {
        // heartbeat interval
        int hi = 1;

        RawNode<Integer> r = newTestRaft(ids, 10, hi);
        r.becomeCandidate();
        r.becomeLeader();

        long term = r.basicStatus().hardState().term();

        for (int i = 0; i < 10; i++) {
             ProposeReceipt receipt = r.propose(i + 1);

            Assertions.assertEquals(term, receipt.term());
            // Node commits en empty entry when becomes a leader.
            Assertions.assertEquals(i + 2, receipt.startIndex());
            Assertions.assertEquals(i + 2, receipt.endIndex());
        }

        // Consume messages emitted after a propose.
        r.readMessages();

        for (int i = 0; i < hi; i++)
            r.tick();

        List<Message> msgs = r.readMessages();

        Collections.sort(msgs, destinationComparator);

        Assertions.assertEquals(msgFactory.newHeartbeatRequest(ids[0], ids[1], term, 0, null),
            msgs.get(0));
        Assertions.assertEquals(msgFactory.newHeartbeatRequest(ids[0], ids[2], term, 0, null),
            msgs.get(1));
    }

    @Test
    public void testFollowerStartElection() {
        checkNonleaderStartElection(STATE_FOLLOWER);
    }

    @Test
    public void testCandidateStartNewElection() {
        checkNonleaderStartElection(STATE_CANDIDATE);
    }

    // testNonleaderStartElection tests that if a follower receives no communication
    // over election timeout, it begins an election to choose a new leader. It
    // increments its current term and transitions to candidate state. It then
    // votes for itself and issues RequestVote RPCs in parallel to each of the
    // other servers in the cluster.
    // Reference: section 5.2
    // Also if a candidate fails to obtain a majority, it will time out and
    // start a new election by incrementing its term and initiating another
    // round of RequestVote RPCs.
    // Reference: section 5.2
    private void checkNonleaderStartElection(StateType state) {
        // election timeout
        int et = 10;

        RawNode<?> r = newTestRaft(ids, et, 1);

        long expTerm = 2;

        switch (state) {
            case STATE_FOLLOWER:
                r.becomeFollower(1, ids[1]);

                break;

            case STATE_CANDIDATE:
                r.becomeCandidate();

                expTerm++;

                break;
        }

        for (int i = 1; i < 2 * et; i++)
            r.tick();

        BasicStatus status = r.basicStatus();

        HardState hs = status.hardState();

        Assertions.assertEquals(expTerm, hs.term());
        Assertions.assertEquals(STATE_CANDIDATE, status.softState().state());

        Assertions.assertTrue(r.tracker().voted(ids[0]));

        List<Message> msgs = r.readMessages();
        Collections.sort(msgs, destinationComparator);

        Assertions.assertEquals(msgFactory.newVoteRequest(ids[0], ids[1], false, hs.term(), 0, 0, false),
            msgs.get(0));
        Assertions.assertEquals(msgFactory.newVoteRequest(ids[0], ids[2], false, hs.term(), 0, 0, false),
            msgs.get(1));
    }

    // TestLeaderElectionInOneRoundRPC tests all cases that may happen in
    // leader election during one round of RequestVote RPC:
    // a) it wins the election
    // b) it loses the election
    // c) it is unclear about the result
    // Reference: section 5.2
    @Test
    public void testLeaderElectionInOneRound() {
        class TestData {
            final int size;
            final Map<Integer, Boolean> votes;
            final StateType state;

            TestData(int size, Map<Integer, Boolean> votes, StateType state) {
                this.size = size;
                this.votes = votes;
                this.state = state;
            }
        };

        TestData[] tests = new TestData[] {
            // win the election when receiving votes from a majority of the servers
            new TestData(1, Collections.emptyMap(), STATE_LEADER),
            new TestData(3, Map.of(1, true, 2, true), STATE_LEADER),
            new TestData(3, Map.of(1, true), STATE_LEADER),
            new TestData(5, Map.of(1, true, 2, true, 3, true, 4, true), STATE_LEADER),
            new TestData(5, Map.of(1, true, 2, true, 3, true), STATE_LEADER),
            new TestData(5, Map.of(1, true, 2, true), STATE_LEADER),

            // return to follower state if it receives vote denial from a majority
            new TestData(3, Map.of(1, false, 2, false), STATE_FOLLOWER),
            new TestData(5, Map.of(1, false, 2, false, 3, false, 4, false), STATE_FOLLOWER),
            new TestData(5, Map.of(1, true, 2, false, 3, false, 4, false), STATE_FOLLOWER),

            // stay in candidate if it does not obtain the majority
            new TestData(3, Map.of(), STATE_CANDIDATE),
            new TestData(5, Map.of(1, true), STATE_CANDIDATE),
            new TestData(5, Map.of(1, false, 2, false), STATE_CANDIDATE),
            new TestData(5, Map.of(), STATE_CANDIDATE)
        };

        for (TestData tt : tests) {
            UUID[] ids = idsBySize(tt.size);
            RawNode<Integer> r = newTestRaft(ids, 10, 1);

            r.campaign();

            long term = r.basicStatus().hardState().term();

            for (Map.Entry<Integer, Boolean> vote : tt.votes.entrySet()) {
                int idx = vote.getKey();

                r.step(msgFactory.newVoteResponse(ids[idx], ids[0], false, term, !vote.getValue()));
            }

            Assertions.assertEquals(tt.state, r.basicStatus().softState().state());
            Assertions.assertTrue(r.basicStatus().hardState().term() != 1);
        }
    }

    // TestFollowerVote tests that each follower will vote for at most one
    // candidate in a given term, on a first-come-first-served basis.
    // Reference: section 5.2
    @Test
    public void testFollowerVote() {
        class TestData {
            final UUID vote;
            final UUID nvote;
            final boolean wreject;

            TestData(UUID vote, UUID nvote, boolean wreject) {
                this.vote = vote;
                this.nvote = nvote;
                this.wreject = wreject;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(null, ids[0], false),
            new TestData(null, ids[1], false),
            new TestData(ids[0], ids[0], false),
            new TestData(ids[1], ids[1], false),
            new TestData(ids[0], ids[1], true),
            new TestData(ids[1], ids[0], true)
        };

        for (TestData tt : tests) {
            RawNode<Integer> r = newTestRaft(10, 1, memoryStorage(ids, new HardState(1, tt.vote, 0)));

            r.step(msgFactory.newVoteRequest(tt.nvote, ids[0], false, 1, 0, 0, false));

            List<Message> msgs = r.readMessages();

            Assertions.assertEquals(1, msgs.size());
            Assertions.assertEquals(msgFactory.newVoteResponse(ids[0], tt.nvote, false, 1, tt.wreject),
                msgs.get(0));
        }
    }

    // TestCandidateFallback tests that while waiting for votes,
    // if a candidate receives an AppendEntries RPC from another server claiming
    // to be leader whose term is at least as large as the candidate's current term,
    // it recognizes the leader as legitimate and returns to follower state.
    // Reference: section 5.2
    @Test
    public void testCandidateFallback() {
        Message[] tests = new Message[] {
            msgFactory.newAppendEntriesRequest(ids[1], ids[0], 2, 0, 0, Collections.emptyList(), 0),
            msgFactory.newAppendEntriesRequest(ids[1], ids[0], 3, 0, 0, Collections.emptyList(), 0),
        };

        for (Message tt : tests) {
            RawNode<?> r = newTestRaft(ids, 10, 1);

            r.campaign();
            Assertions.assertEquals(STATE_CANDIDATE, r.basicStatus().softState().state());
            Assertions.assertEquals(2, r.basicStatus().hardState().term());

            r.step(tt);

            Assertions.assertEquals(STATE_FOLLOWER, r.basicStatus().softState().state());
            Assertions.assertEquals(tt.term(), r.basicStatus().hardState().term());
        }
    }

    @Test
    public void testFollowerElectionTimeoutRandomized() {
        checkNonleaderElectionTimeoutRandomized(STATE_FOLLOWER);
    }

    @Test
    public void TestCandidateElectionTimeoutRandomized() {
        checkNonleaderElectionTimeoutRandomized(STATE_CANDIDATE);
    }

    // testNonleaderElectionTimeoutRandomized tests that election timeout for
    // follower or candidate is randomized.
    // Reference: section 5.2
    private void checkNonleaderElectionTimeoutRandomized(StateType stateType) {
        int et = 10;
        RawNode<?> r = newTestRaft(ids, et, 1);
        Set<Integer> timeouts = new HashSet<>();

        for (int round = 0; round < 50 * et; round++) {
            switch (stateType) {
                case STATE_FOLLOWER:
                    r.becomeFollower(r.basicStatus().hardState().term() + 1, ids[1]);

                    break;

                case STATE_CANDIDATE:
                    r.becomeCandidate();

                    break;
            }

            int time = 0;

            while (r.readMessages().isEmpty()) {
                r.tick();

                time++;
            }

            timeouts.add(time);
        }

        for (int d = et + 1; d < 2 * et; d++) {
            Assertions.assertTrue(timeouts.contains(d),
                MessageFormatter.format("timeout in {} ticks should happen", d).getMessage());
        }
    }

    // TestLeaderStartReplication tests that when receiving client proposals,
    // the leader appends the proposal to its log as a new entry, then issues
    // AppendEntries RPCs in parallel to each of the other servers to replicate
    // the entry. Also, when sending an AppendEntries RPC, the leader includes
    // the index and term of the entry in its log that immediately precedes
    // the new entries.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    @Test
    public void testLeaderStartReplication() {
        MemoryStorage memStorage = memoryStorage(ids, new HardState(1, null, 0));

        RawNode<String> r = newTestRaft(10, 1, memStorage);
        r.becomeCandidate();
        r.becomeLeader();

        long term = r.basicStatus().hardState().term();

        commitNoopEntry(r, memStorage);

        long li = r.raftLog().lastIndex();

        r.propose("some data");

        Assertions.assertEquals(li + 1, r.raftLog().lastIndex());
        Assertions.assertEquals(li, r.raftLog().committed());

        List<Message> msgs = r.readMessages();
        Collections.sort(msgs, destinationComparator);

        List<Entry> wents = Arrays.asList(entry(term, li + 1, "some data"));

        Assertions.assertEquals(msgFactory.newAppendEntriesRequest(ids[0], ids[1], term, li, term, wents, li),
            msgs.get(0));
        Assertions.assertEquals(msgFactory.newAppendEntriesRequest(ids[0], ids[2], term, li, term, wents, li),
            msgs.get(1));
    }

    // TestLeaderCommitEntry tests that when the entry has been safely replicated,
    // the leader gives out the applied entries, which can be applied to its state
    // machine.
    // Also, the leader keeps track of the highest index it knows to be committed,
    // and it includes that index in future AppendEntries RPCs so that the other
    // servers eventually find out.
    // Reference: section 5.3
    @Test
    public void testLeaderCommitEntry() {
        MemoryStorage memStorage = memoryStorage();

        RawNode<String> r = newTestRaft(10, 1, memStorage);

        r.becomeCandidate();
        r.becomeLeader();

        commitNoopEntry(r, memStorage);

        long li = r.raftLog().lastIndex();

        ProposeReceipt receipt = r.propose("some data");
        Assertions.assertNotNull(receipt);

        for (Message msg : r.readMessages())
            r.step(acceptAndReply((AppendEntriesRequest)msg));

        Assertions.assertEquals(li + 1, r.raftLog().committed());

        List<Entry> entries = r.raftLog().nextEntries();
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(Entry.EntryType.ENTRY_DATA, entries.get(0).type());
        Assertions.assertEquals("some data", ((UserData<String>)entries.get(0).data()).data());

        List<Message> msgs = r.readMessages();
        Collections.sort(msgs, destinationComparator);

        int idx = 1;

        for (Message m : msgs) {
            Assertions.assertEquals(ids[idx], m.to());
            Assertions.assertEquals(MessageType.MsgApp, m.type());
            Assertions.assertEquals(li + 1, ((AppendEntriesRequest)m).committedIndex());

            idx++;
        }
    }

    // TestLeaderAcknowledgeCommit tests that a log entry is committed once the
    // leader that created the entry has replicated it on a majority of the servers.
    // Reference: section 5.3
    @Test
    public void testLeaderAcknowledgeCommit() {
        class TestData {
            final int size;
            final Set<Integer> acceptors;
            final boolean wack;

            TestData(int size, Set<Integer> acceptors, boolean wack) {
                this.size = size;
                this.acceptors = acceptors;
                this.wack = wack;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(1, Set.of(), true),
            new TestData(3, Set.of(), false),
            new TestData(3, Set.of(1), true),
            new TestData(3, Set.of(1, 2), true),
            new TestData(5, Set.of(), false),
            new TestData(5, Set.of(1), false),
            new TestData(5, Set.of(1, 2), true),
            new TestData(5, Set.of(1, 2, 3), true),
            new TestData(5, Set.of(1, 2, 3, 4), true),
        };

        for (TestData tt : tests) {
            UUID[] ids = idsBySize(tt.size);

            MemoryStorage memStorage = memoryStorage(ids, new HardState(1, null, 0));

            RawNode<String> r = newTestRaft(10, 1, memStorage);

            r.becomeCandidate();
            r.becomeLeader();

            commitNoopEntry(r, memStorage);

            long li = r.raftLog().lastIndex();

            ProposeReceipt receipt = r.propose("some data");
            Assertions.assertNotNull(receipt);

            for (Message m : r.readMessages()) {
                int idx = Arrays.binarySearch(ids, m.to());
                Assertions.assertTrue(idx > 0);

                if (tt.acceptors.contains(idx)) {
                    r.step(acceptAndReply((AppendEntriesRequest)m));
                }
            }

            boolean success = r.raftLog().committed() > li;

            Assertions.assertEquals(tt.wack, success);
        }
    }

    // TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
    // it also commits all preceding entries in the leader’s log, including
    // entries created by previous leaders.
    // Also, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    @Test
    public void testLeaderCommitPrecedingEntries() {
        Entry[][] tests = new Entry[][] {
            new Entry[] {},
            new Entry[] {entry(2, 1, "")},
            new Entry[] {entry(1, 1, ""), entry(2, 2, "")},
            new Entry[] {entry(1, 1, "")},
        };

        for (Entry[] testEntries : tests) {
            MemoryStorage storage = memoryStorage(ids, new HardState(2, null, 0));
            storage.append(Arrays.asList(testEntries));

            RawNode<String> r = newTestRaft(10, 1, storage);

            r.becomeCandidate();
            r.becomeLeader();

            ProposeReceipt receipt = r.propose("some data");
            Assertions.assertNotNull(receipt);

            for (Message m :r.readMessages())
                r.step(acceptAndReply((AppendEntriesRequest)m));

            long li = testEntries.length;

            List<Entry> wents = new ArrayList<>(Arrays.asList(testEntries));

            wents.add(entry(3, li + 1, null));
            wents.add(entry(3, li + 2, "some data"));

            List<Entry> next = r.raftLog().nextEntries();

            Assertions.assertEquals(wents, next);
        }
    }

    // TestFollowerCheckMsgApp tests that if the follower does not find an
    // entry in its log with the same index and term as the one in AppendEntries RPC,
    // then it refuses the new entries. Otherwise it replies that it accepts the
    // append entries.
    // Reference: section 5.3
    @Test
    public void testFollowerCheckMsgApp() {
        Entry[] ents = new Entry[] {
            entry(1, 1, ""),
            entry(2, 2, ""),
        };

        class TestData {
            final long term;
            final long idx;
            final long wIdx;
            final boolean wReject;
            final long wRejectHint;

            TestData(long term, long idx, long wIdx, boolean wReject, long wRejectHint) {
                this.term = term;
                this.idx = idx;
                this.wIdx = wIdx;
                this.wReject = wReject;
                this.wRejectHint = wRejectHint;
            }
        }

        TestData[] tests = new TestData[] {
            // match with committed entries
            new TestData(0, 0, 1, false, 0),
            new TestData(ents[0].term(), ents[0].index(), 1, false, 0),
            // match with uncommitted entries
            new TestData(ents[1].term(), ents[1].index(), 2, false, 0),

            // unmatch with existing entry
            new TestData(ents[0].term(), ents[1].index(), ents[1].index(), true, 2),
            // unexisting entry
            new TestData(ents[1].term() + 1, ents[1].index() + 1, ents[1].index() + 1, true, 2),
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage(ids, new HardState(2, null, 1));
            storage.append(Arrays.asList(ents));

            RawNode<String> r = newTestRaft(10, 1, storage);

            r.becomeFollower(2, ids[1]);

            r.step(msgFactory.newAppendEntriesRequest(ids[1], ids[0], 2, tt.idx, tt.term, Collections.emptyList(), 1));

            List<Message> msgs = r.readMessages();
            Assertions.assertEquals(1, msgs.size());

            Assertions.assertEquals(
                msgFactory.newAppendEntriesResponse(ids[0], ids[1], 2, tt.wIdx, tt.wReject, tt.wRejectHint),
                msgs.get(0));
        }
    }

    private Entry entry(long term, long idx, String data) {
        return entryFactory.newEntry(term, idx, new UserData<>(data));
    }

    // TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
    // the follower will delete the existing conflict entry and all that follow it,
    // and append any new entries not already in the log.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    @Test
    public void testFollowerAppendEntries() {
        class TestData {
            final long index;
            final long term;
            final Entry[] ents;
            final Entry[] wents;
            final Entry[] wunstable;

            TestData(long index, long term, Entry[] ents, Entry[] wents, Entry[] wunstable) {
                this.index = index;
                this.term = term;
                this.ents = ents;
                this.wents = wents;
                this.wunstable = wunstable;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(
                2, 2,
                new Entry[] {entry(3, 3, "")},
                new Entry[] {entry(1, 1, ""), entry(2, 2, ""), entry(3, 3, "")},
                new Entry[] {entry(3, 3, "")}
            ),
            new TestData(
                1, 1,
                new Entry[] {entry(3, 2, ""), entry(4, 3, "")},
                new Entry[] {entry(1, 1, ""), entry(3, 2, ""), entry(4, 3, "")},
                new Entry[] {entry(3, 2, ""), entry(4, 3, "")}
            ),
            new TestData(
                0, 0,
                new Entry[] {entry(1, 1, "")},
                new Entry[] {entry(1, 1, ""), entry(2, 2, "")},
                new Entry[] {}
            ),
            new TestData(
                0, 0,
                new Entry[] {entry(3, 1, "")},
                new Entry[] {entry(3, 1, "")},
                new Entry[] {entry(3, 1, "")}
            ),
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage();

            storage.append(Arrays.asList(entry(1, 1, ""), entry(2, 2, "")));

            RawNode<String> r = newTestRaft(10, 1, storage);
            r.becomeFollower(2, ids[1]);

            r.step(msgFactory.newAppendEntriesRequest(ids[1], ids[0], 2, tt.index, tt.term, Arrays.asList(tt.ents), 0));

            Assertions.assertEquals(Arrays.asList(tt.wents), r.raftLog().allEntries());
            Assertions.assertEquals(Arrays.asList(tt.wunstable), r.raftLog().unstableEntries());
        }
    }

    // TestFollowerCommitEntry tests that once a follower learns that a log entry
    // is committed, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    @Test
    public void testFollowerCommitEntry() {
        class TestData {
            final Entry[] ents;
            final long commit;

            TestData(Entry[] ents, long commit) {
                this.ents = ents;
                this.commit = commit;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(
			    new Entry[] {entry(1, 1, "some data")},
                1
            ),
            new TestData(
			    new Entry[] {
                    entry(1, 1, "some data"),
                    entry(1, 2, "some data2")
                },
                2
            ),
            new TestData(
			    new Entry[] {
                    entry(1, 1, "some data2"),
                    entry(1, 2, "some data"),
                },
                2
            ),
            new TestData(
			    new Entry[] {
                    entry(1, 1, "some data"),
                    entry(1, 2, "some data2"),
                },
                1
            ),
        };

        for (TestData tt : tests) {
            RawNode<String> r = newTestRaft(ids, 10, 1);

            r.becomeFollower(1, ids[1]);

            r.step(msgFactory.newAppendEntriesRequest(ids[1], ids[0], 1, 0, 0, Arrays.asList(tt.ents), tt.commit));

            Assertions.assertEquals(tt.commit, r.raftLog().committed());

            List<Entry> next = r.raftLog().nextEntries();

            for (int i = 0; i < tt.commit; i++)
                Assertions.assertEquals(tt.ents[i], next.get(i));
        }
    }

    // TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
    // into consistency with its own.
    // Reference: section 5.3, figure 7
    @Test
    public void testLeaderSyncFollowerLog() {
        Entry[] ents = new Entry[] {
            entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
            entry(4, 4, ""), entry(4, 5, ""),
            entry(5, 6, ""), entry(5, 7, ""),
            entry(6, 8, ""), entry(6, 9, ""), entry(6, 10, "")
        };

        long term = 8;

        Entry[][] tests = new Entry[][] {
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(4, 4, ""), entry(4, 5, ""),
                entry(5, 6, ""), entry(5, 7, ""),
                entry(6, 8, ""), entry(6, 9, "")
            },
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(4, 4, "")
            },
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(4, 4, ""), entry(4, 5, ""),
                entry(5, 6, ""), entry(5, 7, ""),
                entry(6, 8, ""), entry(6, 9, ""), entry(6, 10, ""), entry(6, 11, "")
            },
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(4, 4, ""), entry(4, 5, ""),
                entry(5, 6, ""), entry(5, 7, ""),
                entry(6, 8, ""), entry(6, 9, ""), entry(6, 10, ""),
                entry(7, 11, ""), entry(7, 12, "")
            },
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(4, 4, ""), entry(4, 5, ""), entry(4, 6, ""), entry(4, 7, "")
            },
            new Entry[] {
                entry(1, 1, ""), entry(1, 2, ""), entry(1, 3, ""),
                entry(2, 4, ""), entry(2, 5, ""), entry(2, 6, ""),
                entry(3, 7, ""), entry(3, 8, ""), entry(3, 9, ""), entry(3, 10, ""), entry(3, 11, "")
            },
        };

        Random rnd = new Random(seed());

        for (Entry[] tt : tests) {
            MemoryStorage leadStorage = memoryStorage(ids, new HardState(term, null, ents.length));

            leadStorage.append(Arrays.asList(ents));

            RawNode<String> lead = newTestRaft(10, 1, leadStorage, rnd);

            MemoryStorage followerStorage = memoryStorage(ids[1], ids, new HardState(term - 1, null, 0));

            followerStorage.append(Arrays.asList(tt));

            RawNode<String> follower = newTestRaft(10, 1, followerStorage, rnd);

            // It is necessary to have a three-node cluster.
            // The second may have more up-to-date log than the first one, so the
            // first node needs the vote from the third node to become the leader.
            Network n = new Network(
                rnd,
                new RawNodeStepper(lead, leadStorage),
                new RawNodeStepper(follower, followerStorage));

            n.<String>action(ids[0], s -> s.node().campaign());

            // The election occurs in the term after the one we loaded with
            // lead.loadState above.
            n.send(msgFactory.newVoteResponse(ids[2], ids[0], false, term + 1, false));

            n.<String>action(ids[0], s -> s.node().propose(""));

            // TODO agoncharuk here was all diff.
            Assertions.assertEquals(lead.raftLog().allEntries(), follower.raftLog().allEntries());
        }
    }

    // TestVoteRequest tests that the vote request includes information about the candidate’s log
    // and are sent to all of the other nodes.
    // Reference: section 5.4.1
    @Test
    public void testVoteRequest() {
        class TestData {
            private final Entry[] ents;
            private final long wterm;

            TestData(Entry[] ents, long wterm) {
                this.ents = ents;
                this.wterm = wterm;
            }
        };

        TestData[] tests = new TestData[] {
            new TestData(new Entry[] {entry(1, 1, "")}, 2),
            new TestData(new Entry[] {entry(1, 1, ""), entry(2, 2, "")}, 3),
        };

        for (TestData tt : tests) {
            int et = 10;

            RawNode<String> r = newTestRaft(ids, 10, 1);

            r.step(msgFactory.newAppendEntriesRequest(ids[1], ids[0], tt.wterm - 1, 0, 0, Arrays.asList(tt.ents), 0));

            r.readMessages();

            for (int i = 1; i < et * 2; i++)
                r.tick();

            List<Message> msgs = r.readMessages();
            Collections.sort(msgs, destinationComparator);

            Assertions.assertEquals(2, msgs.size());

            int i = 1;
            for (Message m : msgs) {
                Assertions.assertEquals(MessageType.MsgVote, m.type());
                Assertions.assertEquals(ids[i], m.to());
                Assertions.assertEquals(tt.wterm, m.term());

                long wIdx = tt.ents[tt.ents.length - 1].index();
                long wTerm = tt.ents[tt.ents.length - 1].term();

                VoteRequest req = (VoteRequest)m;

                Assertions.assertEquals(wIdx, req.lastIndex());
                Assertions.assertEquals(wTerm, req.lastTerm());

                i++;
            }
        }
    }

    // TestVoter tests the voter denies its vote if its own log is more up-to-date
    // than that of the candidate.
    // Reference: section 5.4.1
    @Test
    public void testVoter() {
        class TestData {
            final Entry[] ents;
            final long logTerm;
            final long idx;
            final boolean wReject;

            TestData(Entry[] ents, long logTerm, long idx, boolean wReject) {
                this.ents = ents;
                this.logTerm = logTerm;
                this.idx = idx;
                this.wReject = wReject;
            }
        };

        TestData[] tests = new TestData[] {
            // same logterm
            new TestData(new Entry[] {entry(1, 1, "")}, 1, 1, false),
            new TestData(new Entry[] {entry(1, 1, "")}, 1, 2, false),
            new TestData(new Entry[] {entry(1, 1, ""), entry(1, 2, "")}, 1, 1, true),
            // candidate higher logterm
            new TestData(new Entry[] {entry(1, 1, "")}, 2, 1, false),
            new TestData(new Entry[] {entry(1, 1, "")}, 2, 2, false),
            new TestData(new Entry[] {entry(1, 1, ""), entry(1, 2, "")}, 2, 1, false),
            // voter higher logterm
            new TestData(new Entry[] {entry(2, 1, "")}, 1, 1, true),
            new TestData(new Entry[] {entry(2, 1, "")}, 1, 2, true),
            new TestData(new Entry[] {entry(2, 1, ""), entry(1, 2, "")}, 1, 1, true),
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage();

            storage.append(Arrays.asList(tt.ents));

            RawNode<String> r = newTestRaft(10, 1, storage);

            r.step(msgFactory.newVoteRequest(ids[1], ids[0], false, 3, tt.idx, tt.logTerm, false));

            List<Message> msgs = r.readMessages();
            Assertions.assertEquals(1, msgs.size());

            Assertions.assertEquals(MessageType.MsgVoteResp, msgs.get(0).type());
            Assertions.assertEquals(tt.wReject, ((VoteResponse)msgs.get(0)).reject());
        }
    }

    // TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
    // current term are committed by counting replicas.
    // Reference: section 5.4.2
    @Test
    public void testLeaderOnlyCommitsLogFromCurrentTerm() {
        Entry[] ents = new Entry[] {entry(1, 1, ""), entry(2, 2, "")};

        class TestData {
            final long wIdx;
            final long wCommit;

            TestData(long wIdx, long wCommit) {
                this.wIdx = wIdx;
                this.wCommit = wCommit;
            }
        };

        TestData[] tests = new TestData[] {
            // do not commit log entries in previous terms
            new TestData(1, 0),
            new TestData(2, 0),
            // commit log in current term
            new TestData(3, 3),
        };

        for (TestData tt : tests) {
            MemoryStorage storage = memoryStorage(ids, new HardState(2, null, 0));

            storage.append(Arrays.asList(ents));

            RawNode<String> r = newTestRaft(10, 1, storage);

            // become leader at term 3
            r.becomeCandidate();
            r.becomeLeader();
            r.readMessages();

            // propose am entry to current term
            ProposeReceipt receipt = r.propose("");
            Assertions.assertNotNull(receipt);

            r.step(msgFactory.newAppendEntriesResponse(ids[1], ids[0], r.basicStatus().hardState().term(),
                tt.wIdx, false, 0));
            Assertions.assertEquals(tt.wCommit, r.raftLog().committed());
        }
    }

    @Test
    public void testFollowersElectionTimeoutNonconflict() {
        checkNonleadersElectionTimeoutNonconflict(STATE_FOLLOWER);
    }

    @Test
    public void TestCandidatesElectionTimeoutNonconflict() {
        checkNonleadersElectionTimeoutNonconflict(STATE_CANDIDATE);
    }

    // testNonleadersElectionTimeoutNonconflict tests that in most cases only a
    // single server(follower or candidate) will time out, which reduces the
    // likelihood of split vote in the new election.
    // Reference: section 5.2
    private void checkNonleadersElectionTimeoutNonconflict(StateType state) {
        int et = 10;
        int size = 5;

        RawNode<?>[] rs = new RawNode<?>[size];

        UUID[] ids = idsBySize(size);

        long seed = seed();

        for (int k = 0; k < ids.length; k++) {
            MemoryStorage storage = memoryStorage(ids[k], ids, new HardState(1, null, 0));

            rs[k] = newTestRaft(et, 1, storage, new Random(seed + k));
        }

        int conflicts = 0;

        for (int round = 0; round < 1000; round++) {
            for (RawNode<?> r : rs) {
                switch (state) {
                    case STATE_FOLLOWER:
                        r.becomeFollower(r.basicStatus().hardState().term() + 1, null);

                        break;

                    case STATE_CANDIDATE:
                        r.becomeCandidate();

                        break;
                }
            }

            int timeoutNum = 0;

            while (timeoutNum == 0) {
                for (RawNode<?> r : rs) {
                    Assertions.assertTrue(r.readMessages().isEmpty());

                    r.tick();

                    if (!r.readMessages().isEmpty())
                        timeoutNum++;
                }
            }

            // several rafts time out at the same tick
            if (timeoutNum > 1)
                conflicts++;
        }

        Assertions.assertFalse((float)conflicts / 1000.f > 0.3);
    }

    private void commitNoopEntry(RawNode<?> r, MemoryStorage storage) {
        Assertions.assertEquals(STATE_LEADER, r.basicStatus().softState().state(),
            "should only be used when RawNode is the leader");

        r.bcastAppend();

        // simulate the response of MsgApp
        for (Message m : r.readMessages()) {
            Assertions.assertEquals(MessageType.MsgApp, m.type());

            AppendEntriesRequest req = (AppendEntriesRequest)m;

            Assertions.assertEquals(1, req.entries().size());

            Entry entry = req.entries().get(0);
            Assertions.assertEquals(Entry.EntryType.ENTRY_DATA, entry.type(), "not a message to append noop entry");
            Assertions.assertNull(((UserData<String>)entry.data()).data(), "not a message to append noop entry");

            r.step(acceptAndReply(req));
        }

        // ignore further messages to refresh followers' commit index
        r.readMessages();
        storage.append(r.raftLog().unstableEntries());
        r.raftLog().appliedTo(r.raftLog().committed());
        r.raftLog().stableTo(r.raftLog().lastIndex(), r.raftLog().lastTerm());
    }

    private Message acceptAndReply(AppendEntriesRequest req) {
        return msgFactory.newAppendEntriesResponse(
            req.to(),
            req.from(),
            req.term(),
            req.logIndex() + req.entries().size(),
            false,
            0);
    }
}
