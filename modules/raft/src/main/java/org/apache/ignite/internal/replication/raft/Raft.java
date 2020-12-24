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
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.replication.raft.confchange.Changer;
import org.apache.ignite.internal.replication.raft.confchange.Restore;
import org.apache.ignite.internal.replication.raft.confchange.RestoreResult;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.replication.raft.message.HeartbeatRequest;
import org.apache.ignite.internal.replication.raft.message.HeartbeatResponse;
import org.apache.ignite.internal.replication.raft.message.InstallSnapshotRequest;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageFactory;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.message.VoteRequest;
import org.apache.ignite.internal.replication.raft.message.VoteResponse;
import org.apache.ignite.internal.replication.raft.storage.ConfChange;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.EntryFactory;
import org.apache.ignite.internal.replication.raft.storage.LogData;
import org.apache.ignite.internal.replication.raft.storage.Storage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.slf4j.Logger;

import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_ELECTION;
import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_PRE_ELECTION;
import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_TRANSFER;
import static org.apache.ignite.internal.replication.raft.Progress.ProgressState.StateReplicate;
import static org.apache.ignite.internal.replication.raft.Progress.ProgressState.StateSnapshot;
import static org.apache.ignite.internal.replication.raft.VoteResult.VoteWon;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgApp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgBeat;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgCheckQuorum;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHeartbeat;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHup;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgPreVote;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgPreVoteResp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgProp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgReadIndex;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgReadIndexResp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgSnap;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgTimeoutNow;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgVote;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgVoteResp;
import static org.apache.ignite.internal.replication.raft.storage.Entry.EntryType.ENTRY_CONF_CHANGE;

/**
 * TODO agoncharuk: Looks like the ETCD model heavily relies on MsgUnreachable to be fed to the Raft state machine.
 * TODO We need to change this to be able to drop any arbitrary message between nodes, as in the original Raft paper.
 * TODO See maybeSendAppend method usage.
 */
public class Raft {
    private UUID id;

    private long term;
    private UUID vote;

    private List<ReadState> readStates;

    // the log
    private RaftLog raftLog;

    private long maxMsgSize;
    private long maxUncommittedSize;

    // TODO(tbg): rename to trk.
    private Tracker prs;

    private StateType state;

    // isLearner is true if the local raft node is a learner.
    private boolean isLearner;

    private List<Message> msgs;

    // the leader id
    private UUID lead;

    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in raft thesis 3.10.
    private UUID leadTransferee;

    // Only one conf change may be pending (in the log, but not yet
    // applied) at a time. This is enforced via pendingConfIndex, which
    // is set to a value >= the log index of the latest pending
    // configuration change (if any). Config changes are only allowed to
    // be proposed if the leader's applied index is greater than this
    // value.
    private long pendingConfIndex;

    // an estimate of the size of the uncommitted tail of the Raft log. Used to
    // prevent unbounded log growth. Only maintained by the leader. Reset on
    // term changes.
    private long uncommittedSize;

    private ReadOnly readOnly;

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    private int electionElapsed;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    private int heartbeatElapsed;

    private boolean checkQuorum;
    private boolean preVote;

    private int heartbeatTimeout;
    private int electionTimeout;

    // randomizedElectionTimeout is a random number between
    // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    private int randomizedElectionTimeout;

    private boolean disableProposalForwarding;

    private Runnable tick;
    private Consumer<Message> step;

    private MessageFactory messageFactory;
    private EntryFactory<?> entryFactory;

    private Logger logger;

    public boolean hasLeader() {
        return lead != null;
    }

    public SoftState softState() {
        return new SoftState(lead, state);
    }

    public HardState hardState() {
        return new HardState(
            term,
            vote,
            raftLog.committed()
        );
    }

    public void tick() {
        tick.run();
    }

    public void advance(Ready rd) {
        reduceUncommittedSize(rd.committedEntries());

        long newApplied = rd.appliedCursor();

        // If entries were applied (or a snapshot), update our cursor for
        // the next Ready. Note that if the current HardState contains a
        // new Commit index, this does not mean that we're also applying
        // all of the new entries due to commit pagination by size.
        if (newApplied > 0) {
            long oldApplied = raftLog.applied();
            raftLog.appliedTo(newApplied);

            if (prs.config().autoLeave() &&
                oldApplied <= pendingConfIndex &&
                newApplied >= pendingConfIndex &&
                state == StateType.STATE_LEADER) {
                // If the current (and most recent, at least for this leader's term)
                // configuration should be auto-left, initiate that now. We use a
                // null data which unmarshals into an empty ConfChange and has the
                // benefit that appendEntry can never refuse it based on its size
                // (which registers as zero).
                ConfChange zeroChange = null;

                // There's no way in which this proposal should be able to be rejected.
                if (!appendEntry(Collections.<LogData>singletonList(zeroChange)))
                    throw new AssertionError("refused un-refusable auto-leaving ConfChange");

                pendingConfIndex = raftLog.lastIndex();
                logger.info("initiating automatic transition out of joint configuration {}", prs.config());
            }
        }

        if (!rd.entries().isEmpty()) {
            Entry e = rd.entries().get(rd.entries().size() - 1);

            raftLog.stableTo(e.index(), e.term());
        }

        if (!rd.snapshot().isEmpty())
            raftLog.stableSnapshotTo(rd.snapshot().metadata().index());
    }

    public void step(Message m) {
        // Handle the message term, which may result in our stepping down to a follower.

        // local message
        if (m.term() == 0) {
        }
        else if (m.term() > term) {
            if (m.type() == MsgVote || m.type() == MsgPreVote) {
                VoteRequest req = (VoteRequest)m;

                boolean force = req.campaignTransfer();

                boolean inLease = checkQuorum && lead != null && electionElapsed < electionTimeout;

                if (!force && inLease) {
                    // If a server receives a RequestVote request within the minimum election timeout
                    // of hearing from a current leader, it does not update its term or grant its vote
                    logger.info("{} [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: lease is not expired (remaining ticks: {})",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), vote,
                        req.type(), req.from(), req.lastTerm(), req.lastIndex(), term, electionTimeout - electionElapsed);

                    return;
                }
            }

            // Never change our term in response to a PreVote
            if (m.type() == MsgPreVote) {

            }
            else if (m.type() == MsgPreVoteResp && !((VoteResponse)m).reject()) {
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            }
            else {
                logger.info("{} [term: {}] received a {} message with higher term from {} [term: {}]",
                    id, term, m.type(), m.from(), m.term());

                if (m.type() == MsgApp || m.type() == MsgHeartbeat || m.type() == MsgSnap)
                    becomeFollower(m.term(), m.from());
                else
                    becomeFollower(m.term(), null);
            }
        }
        else if (m.term() < term) {
            if ((checkQuorum || preVote) && (m.type() == MsgHeartbeat || m.type() == MsgApp)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a
                // higher term, but if checkQuorum is true we may not advance the term on
                // MsgVote and must generate other messages to advance the term. The net
                // result of these two features is to minimize the disruption caused by
                // nodes that have been removed from the cluster's configuration: a
                // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                // disruptive term increases, by notifying leader of this node's activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                send(messageFactory.newAppendEntriesResponse(
                    id,
                    m.from(),
                    term,
                    0,
                    false,
                    0));
            }
            else if (m.type() == MsgPreVote) {
                VoteRequest req = (VoteRequest)m;

                // Before Pre-Vote enable, there may have candidate with higher term,
                // but less log. After update to Pre-Vote, the cluster may deadlock if
                // we drop messages with a lower term.
                logger.info("{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                    id, raftLog.lastTerm(), raftLog.lastIndex(), vote,
                    req.type(), req.from(), req.lastTerm(), req.lastIndex(), term);

                send(messageFactory.newVoteResponse(
                    id,
                    m.from(),
                    true,
                    term,
                    /*reject*/true));
            }
            else {
                // ignore other cases
                logger.info("{} [term: {}] ignored a {} message with lower term from {} [term: {}]",
                    id, term, m.type(), m.from(), m.term());
            }

            return;
        }

        switch (m.type()) {
            case MsgHup: {
                hup(preVote ? CAMPAIGN_PRE_ELECTION : CAMPAIGN_ELECTION);

                break;
            }

            case MsgVote:
            case MsgPreVote: {
                VoteRequest req = (VoteRequest)m;

                // We can vote if this is a repeat of a vote we've already cast...
                boolean canVote = m.from().equals(vote) ||
                    // ...we haven't voted and we don't think there's a leader yet in this term...
                    (vote == null && lead == null) ||
                    // ...or this is a PreVote for a future term...
                    (m.type() == MsgPreVote && m.term() > term);

                // ...and we believe the candidate is up to date.
                if (canVote && raftLog.isUpToDate(req.lastIndex(), req.lastTerm())) {
                    // Note: it turns out that that learners must be allowed to cast votes.
                    // This seems counter- intuitive but is necessary in the situation in which
                    // a learner has been promoted (i.e. is now a voter) but has not learned
                    // about this yet.
                    // For example, consider a group in which id=1 is a learner and id=2 and
                    // id=3 are voters. A configuration change promoting 1 can be committed on
                    // the quorum `{2,3}` without the config change being appended to the
                    // learner's log. If the leader (say 2) fails, there are de facto two
                    // voters remaining. Only 3 can win an election (due to its log containing
                    // all committed entries), but to do so it will need 1 to vote. But 1
                    // considers itself a learner and will continue to do so until 3 has
                    // stepped up as leader, replicates the conf change to 1, and 1 applies it.
                    // Ultimately, by receiving a request to vote, the learner realizes that
                    // the candidate believes it to be a voter, and that it should act
                    // accordingly. The candidate's config may be stale, too; but in that case
                    // it won't win the election, at least in the absence of the bug discussed
                    // in:
                    // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
                    logger.info("{} [logterm: {}, index: {}, vote: {}] cast {} for {} [logterm: {}, index: {}] at term {}",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), vote,
                        req.type(), req.from(), req.lastTerm(), req.lastIndex(), term);

                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why, consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now out of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    send(
                        messageFactory.newVoteResponse(
                            id,
                            m.from(),
                            req.preVote(),
                            m.term(),
                            /*reject*/false));

                    if (m.type() == MsgVote) {
                        // Only record real votes.
                        electionElapsed = 0;
                        vote = m.from();
                    }
                }
                else {
                    logger.info("{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                        id, raftLog.lastTerm(), raftLog.lastIndex(), vote,
                        req.type(), req.from(), req.lastTerm(), req.lastIndex(), term);

                    send(messageFactory.newVoteResponse(
                        id,
                        m.from(),
                        req.preVote(),
                        term,
                        /*reject*/true));
                }

                break;
            }

            default: {
                step.accept(m);

                break;
            }
        }
    }

    // send persists state to stable storage and then sends to its mailbox.
    // TODO agoncharuk: messages should be immutable
    private void send(Message m) {
        if (m.from() == null)
            m.from(id);

        if (m.type() == MsgVote || m.type() == MsgVoteResp || m.type() == MsgPreVote || m.type() == MsgPreVoteResp) {
            if (m.term() == 0) {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.term() is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.term() is the new term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.term() is the term the node will campaign,
                //   non-zero as we use m.term() to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.term() is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                throw new AssertionError("term should be set when sending " + m.type());
            }
        }
        else {
            if (m.term() != 0)
                throw new AssertionError("term should not be set when sending " + m.type() + " (was " + m.term() + ")");

            // do not attach term to MsgProp, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            // TODO agoncharuk: the message should be immutable.
            if (m.type() != MsgProp && m.type() != MsgReadIndex)
                m.term(term);
        }

        msgs.add(m);
    }

    // sendAppend sends an append RPC with new entries (if any) and the
    // current commit index to the given peer.
    private void sendAppend(UUID to) {
        maybeSendAppend(to, true);
    }

    private void sendTimeoutNow(UUID to) {
        send(messageFactory.newMessage(id, to, MsgTimeoutNow));
    }


    // maybeSendAppend sends an append message with new entries to the given peer,
    // if necessary. Returns true if a message was sent. The sndIfEmpty
    // argument controls whether messages with no entries will be sent
    // ("empty" messages are useful to convey updated Commit indexes, but
    // are undesirable when we're sending multiple messages in a batch).
    private boolean maybeSendAppend(UUID to, boolean sndIfEmpty) {
        Progress pr = prs.progress(to);

        if (pr.isPaused())
            return false;

        try {
            long term = raftLog.term(pr.next() - 1);
            List<Entry> ents = raftLog.entries(pr.next(), maxMsgSize);

            if (ents.isEmpty() && !sndIfEmpty)
                return false;

            AppendEntriesRequest m = messageFactory.newAppendEntriesRequest(
                id,
                to,
                this.term,
                pr.next() - 1,
                term,
                ents,
                raftLog.committed());

            if (!m.entries().isEmpty()) {
                switch (pr.state()) {
                    // optimistically increase the next when in StateReplicate
                    case StateReplicate: {
                        long last = m.entries().get(m.entries().size() - 1).index();

                        pr.optimisticUpdate(last);
                        pr.inflights().add(last);

                        break;
                    }

                    case StateProbe: {
                        pr.probeSent(true);

                        break;
                    }

                    default:
                        logger.panic("{} is sending append in unhandled state {}", id, pr.state());
                }
            }

            send(m);
        }
        catch (Exception e) { // TODO agoncharuk: Specific exception
            // send snapshot if we failed to get term or entries
            if (!pr.recentActive()) {
                logger.debug("ignore sending snapshot to {} since it is not recently active", to);

                return false;
            }

            try {
                Snapshot snapshot = raftLog.snapshot();

                if (snapshot.isEmpty())
                    throw new AssertionError("need non-empty snapshot");

                long snapIdx = snapshot.metadata().index();
                long snapTerm = snapshot.metadata().term();

                Message m = messageFactory.newInstallSnapshotRequest(
                    id,
                    to,
                    term,
                    snapIdx,
                    snapTerm,
                    snapshot);

                send(m);

                logger.debug("{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} [{}]",
                    id, raftLog.firstIndex(), raftLog.committed(), snapIdx, snapTerm, to, pr);

                pr.becomeSnapshot(snapIdx);

                logger.debug("{} paused sending replication messages to {} [{}]", id, to, pr);
            }
            catch (SnapshotTemporarilyUnavailableException ex) {
                logger.debug("{} failed to send snapshot to {} because snapshot is temporarily unavailable", id, to);

                return false;
            }
            catch (Exception ex) { // TODO agoncharuk: Specific exception should be used here, thrown from raftLog.snapshot()
                // panic
            }
        }

        return true;
    }

    // bcastAppend sends message, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    private void bcastAppend() {
        prs.foreach((id, progress) -> {
            if (this.id.equals(id))
                return;

            sendAppend(id);
        });
    }

    // bcastHeartbeat sends message, without entries to all the peers.
    private void bcastHeartbeat() {
        byte[] lastCtx = readOnly.lastPendingRequestCtx();

        bcastHeartbeatWithCtx(lastCtx);
    }

    private void bcastHeartbeatWithCtx(byte[] ctx) {
        prs.foreach((id, progress) -> {
            if (this.id.equals(id))
                return;

            sendHeartbeat(id, ctx);
        });
    }

    // sendHeartbeat sends a heartbeat message to the given peer.
    private void sendHeartbeat(UUID to, byte[] ctx) {
        // Attach the commit as min(to.matched, committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        long commit = Math.min(prs.progress(to).match(), raftLog.committed());

        Message m = messageFactory.newHeartbeatRequest(
            id,
            to,
            term,
            commit,
            ctx);

        send(m);
    }

    private void becomeFollower(long term, UUID lead) {
        step = this::stepFollower;
        reset(term);
        tick = this::tickElection;
        this.lead = lead;
        state = StateType.STATE_FOLLOWER;

        logger.debug("{} became follower at term {}", id, this.term);
    }

    private void becomeCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == StateType.STATE_LEADER)
            throw new AssertionError("invalid transition [leader -> candidate]");

        step = this::stepCandidate;
        reset(term + 1);
        tick = this::tickElection;
        vote = id;
        state = StateType.STATE_CANDIDATE;

        logger.debug("{} became candidate at term {}", id, term);
    }

    private void becomePreCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == StateType.STATE_LEADER)
            throw new AssertionError("invalid transition [leader -> pre-candidate]");

        // Becoming a pre-candidate changes our step functions and state,
        // but doesn't change anything else. In particular it does not increase
        // r.Term or change r.Vote.
        step = this::stepCandidate;
        prs.resetVotes();
        tick = this::tickElection;
        lead = null;
        state = StateType.STATE_PRE_CANDIDATE;

        logger.info("{} became pre-candidate at term {}", id, term);
    }

    private void becomeLeader() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (state == StateType.STATE_FOLLOWER)
            throw new AssertionError("invalid transition [follower -> leader]");

        step = this::stepLeader;
        reset(term);
        tick = this::tickHeartbeat;
        lead = id;
        state = StateType.STATE_LEADER;

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        prs.progress(id).becomeReplicate();

        // Conservatively set the pendingConfIndex to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        pendingConfIndex = raftLog.lastIndex();

        LogData empty = UserData.empty();

        if (!appendEntry(Collections.singletonList(empty))) {
            // This won't happen because we just called reset() above.
            logger.panic("empty entry was dropped");
        }

        // As a special case, don't count the initial empty entry towards the
        // uncommitted log quota. This is because we want to preserve the
        // behavior of allowing one entry larger than quota if the current
        // usage is zero.
        reduceUncommittedSize(empty);

        logger.info("{} became leader at term {}", id, term);
    }

    private void stepLeader(Message m) {
        // These message types do not require any progress for m.From.
        switch (m.type()) {
            case MsgBeat: {
                bcastHeartbeat();
                return;
            }

            case MsgCheckQuorum: {
                // The leader should always see itself as active. As a precaution, handle
                // the case in which the leader isn't in the configuration any more (for
                // example if it just removed itself).
                //
                // TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
                // leader steps down when removing itself. I might be missing something.
                Progress pr = prs.progress(id);

                if (pr != null)
                    pr.recentActive(true);

                if (!prs.quorumActive()) {
                    logger.warn("{} stepped down to follower since quorum is not active", id);

                    becomeFollower(term, null);
                }

                // Mark everyone (but ourselves) as inactive in preparation for the next
                // CheckQuorum.
                prs.foreach((id, progress) -> {
                    if (!this.id.equals(id))
                        progress.recentActive(false);
                });

                return;
            }

            case MsgProp: {
                if (m.entries().isEmpty())
                    logger.panic("{} stepped empty MsgProp", id);

                if (prs.progress(id) == null) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    throw new NotLeaderException();
                }

                if (leadTransferee != null) {
                    logger.debug("{} [term {}] transfer leadership to {} is in progress; dropping proposal", id, term, leadTransferee);

                    throw new TransferLeaderException(leadTransferee);
                }

                for (int i = 0; i < m.entries().size(); i++) {
                    Entry e = m.entries().get(i);

                    if (e.type() == ENTRY_CONF_CHANGE) {
                        ConfChange cc = ((ConfChangeEntry)e).confChange();

                        boolean alreadyPending = pendingConfIndex > raftLog.applied();
                        boolean alreadyJoint = prs.config().voters().isJoint();

                        boolean wantsLeaveJoint = cc.changes().isEmpty();

                        String refused = null;

                        if (alreadyPending)
                            refused = String.format("possible unapplied conf change at index %d (applied to %d)", pendingConfIndex, raftLog.applied());
                        else if (alreadyJoint && !wantsLeaveJoint)
                            refused = "must transition out of joint config first";
                        else if (!alreadyJoint && wantsLeaveJoint)
                            refused = "not in joint state; refusing empty conf change";

                        if (refused != null) {
                            logger.info("{} ignoring conf change {} at config {}: {}", id, cc, prs.config(), refused);

                            // TODO agoncharuk: since messages are immutable, need to figure out how to avoid changing the collection
                            // TODO Should not be a problem since MsgProp should not be a message anyway.
                            m.entries().set(i, messageFactory.newEntry(Entry.EntryType.ENTRY_DATA));
                        }
                        else
                            pendingConfIndex = raftLog.lastIndex() + i + 1;
                    }
                }

                if (!appendEntry(m.entries()))
                    throw new ProposalDroppedException();

                bcastAppend();

                return;
            }

            case MsgReadIndex: {
                // only one voting member (the leader) in the cluster
                if (prs.isSingleton()) {
                    Message resp = responseToReadIndexReq(m, raftLog.committed());

                    if (resp.to() != null)
                        send(resp);

                    return;
                }

                // Reject read only request when this leader has not committed any log entry at its term.
                if (!committedEntryInCurrentTerm())
                    return;

                // thinking: use an interally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
                switch (readOnly.option()) {
                    // If more than the local vote is needed, go through a full broadcast.
                    case READ_ONLY_SAFE: {
                        readOnly.addRequest(raftLog.committed(), m);

                        // The local node automatically acks the request.
                        readOnly.recvAck(id, m.entries().get(0).data());
                        bcastHeartbeatWithCtx(m.entries().get(0).data());

                        break;
                    }

                    case READ_ONLY_LEASE_BASED: {
                        Message resp = responseToReadIndexReq(m, raftLog.committed());

                        if (resp.to() != null)
                            send(resp);

                        break;
                    }
                }

                return;
            }
        }

        // All other message types require a progress for m.From (pr).
        Progress pr = prs.progress(m.from());

        if (pr == null) {
            logger.debug("{} no progress available for {}", id, m.from());

            return;
        }

        switch (m.type()) {
            case MsgAppResp: {
                AppendEntriesResponse res = (AppendEntriesResponse)m;

                pr.recentActive(true);

                if (res.reject()) {
                    logger.debug("{} received MsgAppResp(MsgApp was rejected, lastindex: {}) from {} for index {}",
                        id, res.rejectHint(), res.from(), res.logIndex());

                    if (pr.maybeDecreaseTo(res.logIndex(), res.rejectHint())) {
                        logger.debug("{} decreased progress of {} to [{}]", id, m.from(), pr);

                        if (pr.state() == StateReplicate)
                            pr.becomeProbe();

                        sendAppend(m.from());
                    }
                }
                else {
                    boolean oldPaused = pr.isPaused();

                    if (pr.maybeUpdate(res.logIndex())) {
                        if (pr.state() == Progress.ProgressState.StateProbe)
                            pr.becomeReplicate();
                        else if (pr.state() == Progress.ProgressState.StateSnapshot && pr.match() >= pr.pendingSnapshot()) {
                            // TODO(tbg): we should also enter this branch if a snapshot is
                            // received that is below pr.pendingSnapshot() but which makes it
                            // possible to use the log again.
                            logger.debug("{} recovered from needing snapshot, resumed sending replication messages to {} [{}]", id, m.from(), pr);

                            // Transition back to replicating state via probing state
                            // (which takes the snapshot into account). If we didn't
                            // move to replicating state, that would only happen with
                            // the next round of appends (but there may not be a next
                            // round for a while, exposing an inconsistent RaftStatus).
                            pr.becomeProbe();
                            pr.becomeReplicate();
                        }
                        else if (pr.state() == StateReplicate)
                            pr.inflights().freeLE(res.logIndex());

                        if (maybeCommit())
                            bcastAppend();
                        else if (oldPaused)
                            // If we were paused before, this node may be missing the
                            // latest commit index, so send it.
                            sendAppend(m.from());

                        // We've updated flow control information above, which may
                        // allow us to send multiple (size-limited) in-flight messages
                        // at once (such as when transitioning from probe to
                        // replicate, or when freeTo() covers multiple messages). If
                        // we have more entries to send, send as many messages as we
                        // can (without sending empty messages for the commit index)
                        // TODO agoncharuk: looks like this may fail(overflow) via inflights.
                        while (maybeSendAppend(m.from(), false));

                        // Transfer leadership is in progress.
                        if (m.from().equals(leadTransferee) && pr.match() == raftLog.lastIndex()) {
                            logger.info("{} sent MsgTimeoutNow to {} after received MsgAppResp", id, m.from());

                            sendTimeoutNow(m.from());
                        }
                    }
                }

                break;
            }

            case MsgHeartbeatResp: {
                HeartbeatResponse res = (HeartbeatResponse)m;

                pr.recentActive(true);
                pr.probeSent(false);

                // free one slot for the full inflights window to allow progress.
                // TODO agoncharuk: this is not clear, why can we free up an inflights slot in response to a heartbeat?
                if (pr.state() == StateReplicate && pr.inflights().full())
                    pr.inflights().freeFirstOne();

                if (pr.match() < raftLog.lastIndex())
                    sendAppend(m.from());

                if (readOnly.option() != ReadOnlyOption.READ_ONLY_SAFE || res.context() == null)
                    return;

                if (prs.config().voters().voteResult(readOnly.recvAck(res.from(), res.context())) != VoteWon)
                    return;

                List<ReadIndexStatus> rss = readOnly.advance(m);

                for (ReadIndexStatus rs : rss) {
                    Message resp = responseToReadIndexReq(rs.request(), rs.index);

                    if (resp.to() != null)
                        send(resp);
                }

                break;
            }

            case MsgSnapStatus: {
                if (pr.state() != StateSnapshot)
                    return;

                // TODO(tbg): this code is very similar to the snapshot handling in
                // MsgAppResp above. In fact, the code there is more correct than the
                // code here and should likely be updated to match (or even better, the
                // logic pulled into a newly created Progress state machine handler).
                if (!m.reject()) {
                    pr.becomeProbe();

                    logger.debug("{} snapshot succeeded, resumed sending replication messages to {} [{}]", id, m.from(), pr);
                }
                else {
                    // NB: the order here matters or we'll be probing erroneously from
                    // the snapshot index, but the snapshot never applied.
                    pr.pendingSnapshot(0);

                    pr.becomeProbe();

                    logger.debug("{} snapshot failed, resumed sending replication messages to {} [{}]", id, m.from(), pr);
                }

                // If snapshot finish, wait for the MsgAppResp from the remote node before sending
                // out the next MsgApp.
                // If snapshot failure, wait for a heartbeat interval before next try
                pr.probeSent(true);

                break;
            }

            case MsgUnreachable: {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgApp is lost.
                if (pr.state() == StateReplicate)
                    pr.becomeProbe();

                logger.debug("{} failed to send message to {} because it is unreachable [{}]", id, m.from(), pr);

                break;
            }

            case MsgTransferLeader: {
                if (pr.isLearner()) {
                    logger.debug("{} remote node {} is learner. Ignored transferring leadership", id, m.from());

                    return;
                }

                UUID leadTransferee = m.from();
                UUID lastLeadTransferee = this.leadTransferee;

                if (lastLeadTransferee != null) {
                    if (lastLeadTransferee.equals(leadTransferee)) {
                        logger.info("{} [term {}] transfer leadership to {} is in progress, ignores request to same node {}",
                            id, term, leadTransferee, leadTransferee);
                        return;
                    }

                    abortLeaderTransfer();

                    logger.info("{} [term {}] abort previous transferring leadership to {}", id, term, lastLeadTransferee);
                }

                if (id.equals(leadTransferee)) {
                    logger.debug("{} is already leader. Ignored transferring leadership to self", id);

                    return;
                }

                // Transfer leadership to third party.
                logger.info("{} [term {}] starts to transfer leadership to {}", id, term, leadTransferee);

                // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
                electionElapsed = 0;
                this.leadTransferee = leadTransferee;

                if (pr.match() == raftLog.lastIndex()) {
                    sendTimeoutNow(leadTransferee);
                    logger.info("{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log", id, leadTransferee, leadTransferee);
                }
                else
                    sendAppend(leadTransferee);

                break;
            }
        }
    }

    // stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
    // whether they respond to MsgVoteResp or MsgPreVoteResp.
    private void stepCandidate(Message m) {
        // Only handle vote responses corresponding to our candidacy (while in
        // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
        // our pre-candidate state).
        MessageType myVoteRespType = state == StateType.STATE_PRE_CANDIDATE ? MsgPreVoteResp : MsgVoteResp;

        switch (m.type()) {
            case MsgProp: {
                logger.info("{} not a leader at term {}; dropping proposal", id, term);

                throw new NotLeaderException();
            }

            case MsgApp: {
                becomeFollower(m.term(), m.from()); // always m.term() == term

                handleAppendEntries((AppendEntriesRequest)m);

                break;
            }

            case MsgHeartbeat: {
                becomeFollower(m.term(), m.from()); // always m.term() == term

                handleHeartbeat((HeartbeatRequest)m);

                break;
            }

            case MsgSnap: {
                becomeFollower(m.term(), m.from()); // always m.term() == term

                handleSnapshot((InstallSnapshotRequest)m);

                break;
            }

            case MsgPreVoteResp:
            case MsgVoteResp: {
                if (m.type() == myVoteRespType) {
                    VoteResponse res = (VoteResponse)m;

                    PollResult p = poll(res.from(), res.type(), !res.reject());

                    logger.info("{} has received {} {} votes and {} vote rejections", id, p.granted(), res.type(), p.rejected());

                    switch (p.result()) {
                        case VoteWon: {
                            if (state == StateType.STATE_PRE_CANDIDATE)
                                campaign(CAMPAIGN_ELECTION);
                            else {
                                becomeLeader();

                                bcastAppend();
                            }

                            break;
                        }

                        case VoteLost: {
                            // MsgPreVoteResp contains future term of pre-candidate
                            // m.term() > term; reuse term
                            becomeFollower(term, null);

                            break;
                        }
                    }
                }

                break;
            }

            case MsgTimeoutNow: {
                logger.debug("{} [term {} state {}] ignored MsgTimeoutNow from {}", id, term, state, m.from());

                break;
            }
        }
    }

    private void stepFollower(Message m) throws ProposalDroppedException {
        switch (m.type()) {
            case MsgProp: {
                if (lead == null) {
                    logger.info("{} no leader at term {}; dropping proposal", id, term);
                    throw new NotLeaderException();
                }
                else if (disableProposalForwarding) {
                    logger.info("{} not forwarding to leader {} at term {}; dropping proposal", id, lead, term);

                    throw new NotLeaderException();
                }

                // TODO agoncharuk: we most likely will not need this forwarding as the client should do the retries
                m.to(lead);
                send(m);

                break;
            }

            case MsgApp: {
                electionElapsed = 0;
                lead = m.from();

                handleAppendEntries((AppendEntriesRequest)m);

                break;
            }

            case MsgHeartbeat: {
                electionElapsed = 0;
                lead = m.from();

                handleHeartbeat((HeartbeatRequest)m);

                break;
            }

            case MsgSnap: {
                electionElapsed = 0;
                lead = m.from();

                handleSnapshot((InstallSnapshotRequest)m);

                break;
            }

            case MsgTransferLeader: {
                if (lead == null) {
                    logger.info("{} no leader at term {}; dropping leader transfer msg", id, term);

                    return;
                }

                m.to(lead);
                send(m);

                break;
            }

            case MsgTimeoutNow: {
                logger.info("{} [term {}] received MsgTimeoutNow from {} and starts an election to get leadership.", id, term, m.from());

                // Leadership transfers never use pre-vote even if r.preVote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
                hup(CampaignType.CAMPAIGN_TRANSFER);

                break;
            }

            case MsgReadIndex: {
                if (lead == null) {
                    logger.info("{} no leader at term {}; dropping index reading msg", id, term);

                    return;
                }

                m.to(lead);
                send(m);

                break;
            }

            case MsgReadIndexResp: {
                if (m.entries().size() != 1) {
                    logger.error("{} invalid format of MsgReadIndexResp from {}, entries count: {}",
                        id, m.from(), m.entries().size());

                    return;
                }

                readStates.add(new ReadState(m.index(), m.entries().get(0).data()));

                break;
            }
        }
    }

    private boolean appendEntry(List<LogData> es) {
        // Track the size of this uncommitted proposal.
        if (!increaseUncommittedSize(es)) {
            logger.debug(
                "{} appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
                id);

            // Drop the proposal.
            return false;
        }

        List<Entry> entries = new ArrayList<>(es.size());

        long li = raftLog.lastIndex();

        for (int i = 0; i < es.size(); i++)
            entries.add(entryFactory.newEntry(es.get(i), term, li + i + 1));

        // use latest "last" index after truncate/append
        li = raftLog.append(entries);

        prs.progress(id).maybeUpdate(li);

        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        maybeCommit();

        return true;
    }

    // maybeCommit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // bcastAppend()).
    private boolean maybeCommit() {
        long mci = prs.committed();

        return raftLog.maybeCommit(mci, term);
    }

    private void handleAppendEntries(AppendEntriesRequest req) {
        if (req.logIndex() < raftLog.committed()) {
            send(messageFactory.newAppendEntriesResponse(
                id,
                req.from(),
                term,
                raftLog.committed(),
                /*reject*/false,
                0));

            return;
        }

        boolean ok = raftLog.maybeAppend(req.logIndex(), req.logTerm(), req.committedIndex(), req.entries());
        long lastIdx = raftLog.lastIndex();

        if (ok)
            send(messageFactory.newAppendEntriesResponse(
                id,
                req.from(),
                term,
                lastIdx,
                /*reject*/false,
                0));
        else {
            logger.debug("{} [logterm: {}, index: {}] rejected MsgApp [logterm: {}, index: {}] from {}",
                id, raftLog.zeroTermOnErrCompacted(raftLog.term(req.logIndex())), req.logIndex(), req.logTerm(), req.logIndex(), req.from());

            send(messageFactory.newAppendEntriesResponse(
                id,
                req.from(),
                term,
                req.logIndex(),
                /*reject*/true,
                raftLog.lastIndex()));
        }
    }

    private void handleHeartbeat(HeartbeatRequest m) {
        raftLog.commitTo(m.commitIndex());

        send(messageFactory.newHeartbeatResponse(
            id,
            m.from(),
            term,
            m.context()));
    }

    // tickElection is run by followers and candidates after r.electionTimeout.
    private void tickElection() {
        electionElapsed++;

        if (promotable() && pastElectionTimeout()) {
            electionElapsed = 0;

            step.accept(messageFactory.newMessage(id, MsgHup));
        }
    }

    // tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
    private void tickHeartbeat() {
        heartbeatElapsed++;
        electionElapsed++;

        if (electionElapsed >= electionTimeout) {
            electionElapsed = 0;

            if (checkQuorum)
                step.accept(messageFactory.newMessage(id, MsgCheckQuorum));

            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (state == StateType.STATE_LEADER && leadTransferee != null)
                abortLeaderTransfer();
        }

        if (state != StateType.STATE_LEADER)
            return;

        if (heartbeatElapsed >= heartbeatTimeout) {
            heartbeatElapsed = 0;

            step.accept(messageFactory.newMessage(id, MsgBeat));
        }
    }

    private void reset(long term) {
        if (this.term != term) {
            this.term = term;
            vote = null;
        }

        lead = null;

        electionElapsed = 0;
        heartbeatElapsed = 0;
        resetRandomizedElectionTimeout();

        abortLeaderTransfer();

        prs.resetVotes();
        prs.visit((id, pr) ->
            new Progress(
                this.id.equals(id) ? raftLog.lastIndex() : 0,
                raftLog.lastIndex() + 1,
                new Inflights(prs.maxInflight()),
                pr.isLearner()
            ));

        pendingConfIndex = 0;
        uncommittedSize = 0;
        readOnly = new ReadOnly(readOnly.option());
    }

    private void hup(CampaignType t) {
        if (state == StateType.STATE_LEADER) {
            logger.debug("{} ignoring MsgHup because already leader", id);

            return;
        }

        if (!promotable()) {
            logger.warn("{} is unpromotable and can not campaign", id);

            return;
        }

        Entry[] ents = raftLog.slice(raftLog.applied() + 1, raftLog.committed() + 1, noLimit);

        int n = numOfPendingConf(ents);

        if (n != 0 && raftLog.committed() > raftLog.applied()) {
            logger.warn("{} cannot campaign at term {} since there are still {} pending configuration changes to apply",
                id, term, n);

            return;
        }

        logger.info("{} is starting a new election at term {}", id, term);

        campaign(t);
    }

    // campaign transitions the raft instance to candidate state. This must only be
    // called after verifying that this is a legitimate transition.
    private void campaign(CampaignType t) {
        if (!promotable()) {
            // This path should not be hit (callers are supposed to check), but
            // better safe than sorry.
            logger.warn("{} is unpromotable; campaign() should not have been called", id);
        }

        long term;
        MessageType voteMsg;

        if (t == CAMPAIGN_PRE_ELECTION) {
            becomePreCandidate();
            voteMsg = MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented r.Term.
            term = this.term + 1;
        }
        else {
            becomeCandidate();
            voteMsg = MsgVote;
            term = this.term;
        }

        // Vote for ourselves.
        PollResult p = poll(id, MessageType.voteResponseType(voteMsg), true);

        if (p.result() == VoteWon) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster). Advance to the next state.
            if (t == CAMPAIGN_PRE_ELECTION)
                campaign(CAMPAIGN_ELECTION);
            else
                becomeLeader();

            return;
        }

        Set<UUID> ids = prs.config().voters().ids();

        for (UUID rmtId : ids) {
            if (rmtId.equals(id))
                continue;

            logger.info("{} [logterm: {}, index: {}] sent {} request to {} at term {}",
                rmtId, raftLog.lastTerm(), raftLog.lastIndex(), voteMsg, rmtId, term);

            send(messageFactory.newVoteRequest(
                id,
                rmtId,
                voteMsg == MsgPreVote,
                term,
                raftLog.lastIndex(),
                raftLog.lastTerm(),
                t == CAMPAIGN_TRANSFER));
        }
    }

    public ConfigState applyConfChange(ConfChange cc) {
        Changer changer = new Changer(
            prs.config(),
            prs.progress(),
            raftLog.lastIndex(),
            prs.maxInflight()
        );

        RestoreResult t;

        if (Changer.leaveJoint(cc))
            t = changer.leaveJoint();
        else if (Changer.enterJoint(cc))
            t = changer.enterJoint(Changer.autoLeave(cc), cc.changes());
        else
            t = changer.simple(cc.changes());

        return switchToConfig(t.trackerConfig(), t.progressMap());
    }

    // switchToConfig reconfigures this node to use the provided configuration. It
    // updates the in-memory state and, when necessary, carries out additional
    // actions such as reacting to the removal of nodes or changed quorum
    // requirements.
    //
    // The inputs usually result from restoring a ConfState or applying a ConfChange.
    private ConfigState switchToConfig(TrackerConfig cfg, ProgressMap prs) {
        this.prs.reset(cfg, prs);

        logger.info("{} switched to configuration {}", id, this.prs.config());

        ConfigState cs = this.prs.configState();
        Progress pr = this.prs.progress(id);

        // Update whether the node itself is a learner, resetting to false when the
        // node is removed.
        isLearner = pr != null && pr.isLearner();

        if ((pr == null || isLearner) && state == StateType.STATE_LEADER) {
            // This node is leader and was removed or demoted. We prevent demotions
            // at the time writing but hypothetically we handle them the same way as
            // removing the leader: stepping down into the next Term.
            //
            // TODO(tbg): step down (for sanity) and ask follower with largest Match
            // to TimeoutNow (to avoid interruption). This might still drop some
            // proposals but it's better than nothing.
            //
            // TODO(tbg): test this branch. It is untested at the time of writing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there
        // are other nodes.
        if (state != StateType.STATE_LEADER || cs.voters().isEmpty())
            return cs;

        if (maybeCommit()) {
            // If the configuration change means that more entries are committed now,
            // broadcast/append to everyone in the updated config.
            bcastAppend();
        }
        else {
            // Otherwise, still probe the newly added replicas; there's no reason to
            // let them wait out a heartbeat interval (or the next incoming
            // proposal).
            this.prs.foreach((id, prog) -> {
                if (this.id.equals(id))
                    return;

                maybeSendAppend(id, false/*sendIfEmpty*/);
            });
        }

        // If the the leadTransferee was removed or demoted, abort the leadership transfer.
        if (leadTransferee != null && this.prs.config().voters().ids().contains(leadTransferee))
            abortLeaderTransfer();

        return cs;
    }

    private void handleSnapshot(InstallSnapshotRequest m) {
        long sindex = m.snapshot().metadata().index();
        long sterm = m.snapshot().metadata().term();

        if (restore(m.snapshot())) {
            logger.info("{} [commit: {}] restored snapshot [index: {}, term: {}]",
                id, raftLog.committed(), sindex, sterm);

            send(messageFactory.newAppendEntriesResponse(
                id,
                m.from(),
                term,
                raftLog.lastIndex(),
                /*reject*/false,
                0));
        }
        else {
            logger.info("{} [commit: {}] ignored snapshot [index: {}, term: {}]",
                id, raftLog.committed(), sindex, sterm);

            send(messageFactory.newAppendEntriesResponse(
                id,
                m.from(),
                term,
                raftLog.committed(),
                /*reject*/false,
                0));
        }
    }

    // restore recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine. If this method returns false, the snapshot was
    // ignored, either because it was obsolete or because of an error.
    private boolean restore(Snapshot s) {
        if (s.metadata().index() <= raftLog.committed())
            return false;

        if (state != StateType.STATE_FOLLOWER) {
            // This is defense-in-depth: if the leader somehow ended up applying a
            // snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in follower
            // state when this method is called.
            logger.warn("{} attempted to restore snapshot as leader; should never happen", id);

            becomeFollower(term + 1, null);

            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots of
        // code here and there assumes that id is in the progress tracker.
        boolean found = false;
        ConfigState cs = s.metadata().configState();

        for (Set<UUID> set : new Set[] {cs.voters(), cs.learners()}) {
            for (UUID id : set) {
                if (this.id.equals(id)) {
                    found = true;

                    break;
                }
            }
            if (found)
                break;
        }

        if (!found) {
            logger.warn(
                "{} attempted to restore snapshot but it is not in the configState {}; should never happen",
                id, cs);

            return false;
        }

        // Now go ahead and actually restore.
        if (raftLog.matchTerm(s.metadata().index(), s.metadata().term())) {
            logger.info("{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
                id, raftLog.committed(), raftLog.lastIndex(), raftLog.lastTerm(), s.metadata().index(), s.metadata().term());

            raftLog.commitTo(s.metadata().index());

            return false;
        }

        raftLog.restore(s);

        // Reset the configuration and add the (potentially updated) peers in anew.
        prs = new Tracker(prs.maxInflight());

        try {
            RestoreResult res = Restore.restore(
                new Changer(
                    prs.config(),
                    prs.progress(),
                    raftLog.lastIndex(),
                    prs.maxInflight()),
                cs);

            TrackerConfig cfg = res.trackerConfig();
            ProgressMap prs = res.progressMap();

            assertConfStatesEquivalent(logger, cs, switchToConfig(cfg, prs));

            Progress pr = this.prs.progress(id);

            pr.maybeUpdate(pr.next() - 1); // TODO(tbg): this is untested and likely unneeded

            logger.info("{} [commit: {}, lastindex: {}, lastterm: {}] restored snapshot [index: {}, term: {}]",
                id, raftLog.committed(), raftLog.lastIndex(), raftLog.lastTerm(), s.metadata().index(), s.metadata().term());

            return true;

        }
        catch (Exception e) { // TODO agoncharuk specific exception should be used here (thrown from restore)
            // This should never happen. Either there's a bug in our config change
            // handling or the client corrupted the conf change.
            throw new AssertionError(String.format("unable to restore config %s: %s", cs, e), e);
        }
    }

    private PollResult poll(UUID id, MessageType t, boolean v) {
        if (v)
            logger.info("{} received {} from {} at term {}", id, t, id, term);
        else
            logger.info("{} received {} rejection from {} at term {}", id, t, id, term);

        prs.recordVote(id, v);

        return prs.tallyVotes();
    }


    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    private boolean promotable() {
        Progress pr = prs.progress(id);

        return pr != null && !pr.isLearner() && !raftLog.hasPendingSnapshot();
    }

    // pastElectionTimeout returns true iff r.electionElapsed is greater
    // than or equal to the randomized election timeout in
    // [electiontimeout, 2 * electiontimeout - 1].
    private boolean pastElectionTimeout() {
        return electionElapsed >= randomizedElectionTimeout;
    }

    /**
     * TODO agoncharuk: this is used only in new Raft construction, move to RaftLog creation
     */
    private void loadState(HardState state) {
        if (state.committed() < raftLog.committed() || state.committed() > raftLog.lastIndex())
            logger.panic("%x state.commit %d is out of range [%d, %d]", id, state.committed(), raftLog.committed(), raftLog.lastIndex());

        raftLog.commitTo(state.committed());
        term = state.term();
        vote = state.vote();
    }

    private void abortLeaderTransfer() {
        leadTransferee = null;
    }

    // committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
    private boolean committedEntryInCurrentTerm() {
        return raftLog.zeroTermOnErrCompacted(raftLog.term(raftLog.committed())) == term;
    }

    // responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
    // itself, a blank value will be returned.
    private Message responseToReadIndexReq(Message req, long readIndex) {
        if (req.from() == null || id.equals(req.from())) {
            readStates.add(new ReadState(readIndex, req.entries().get(0).data()));

            return messageFactory.newMessage(id, MsgReadIndexResp);
        }

        return messageFactory.newMessage(id, req.from(), MsgReadIndexResp, term, readIndex, 0, req.entries());
    }

    private int numOfPendingConf(Entry[] ents) {
        int n = 0;
        for (Entry ent : ents) {
            if (ent.type() == ENTRY_CONF_CHANGE)
                n++;
        }

        return n;
    }

    // increaseUncommittedSize computes the size of the proposed entries and
    // determines whether they would push leader over its maxUncommittedSize limit.
    // If the new entries would exceed the limit, the method returns false. If not,
    // the increase in uncommitted entry size is recorded and the method returns
    // true.
    //
    // Empty payloads are never refused. This is used both for appending an empty
    // entry at a new leader's term, as well as leaving a joint configuration.
    private boolean increaseUncommittedSize(List<LogData> ents) {
        long s = 0;

        for (LogData ent : ents)
            s += payloadSize(ent);

        if (uncommittedSize > 0 && s > 0 && uncommittedSize + s > maxUncommittedSize) {
            // If the uncommitted tail of the Raft log is empty, allow any size
            // proposal. Otherwise, limit the size of the uncommitted tail of the
            // log and drop any proposal that would push the size over the limit.
            // Note the added requirement s > 0 which is used to make sure that
            // appending single empty entries to the log always succeeds, used both
            // for replicating a new leader's initial empty entry, and for
            // auto-leaving joint configurations.
            return false;
        }

        uncommittedSize += s;

        return true;
    }

    // reduceUncommittedSize accounts for the newly committed entries by decreasing
    // the uncommitted entry size limit.
    private void reduceUncommittedSize(LogData... ents) {
        if (uncommittedSize == 0)
            // Fast-path for followers, that do not track or enforce the limit.
            return;

        long s = 0;

        for (Entry ent : ents)
            s += payloadSize(ent);

        if (s > uncommittedSize) {
            // uncommittedSize may underestimate the size of the uncommitted Raft
            // log tail but will never overestimate it. Saturate at 0 instead of
            // allowing overflow.
            uncommittedSize = 0;
        }
        else
            uncommittedSize -= s;
    }

    public static class Builder {
        private RaftConfig c;
        private Storage storage;

        public Raft build() {
            c.validate();

            RaftLog raftlog = newLogWithSize(storage, logger, c.maxCommittedSizePerReady());

            InitialState initState = storage.initialState();
            ConfigState cs = initState.configState();
            HardState hs = initState.hardState();

            if (!c.peers().isEmpty() || !c.learners().isEmpty()) {
                if (!cs.voters().isEmpty() || !cs.learners().isEmpty()) {
                    // TODO(bdarnell): the peers argument is always nil except in
                    // tests; the argument should be removed and these tests should be
                    // updated to specify their nodes through a snapshot.
                    // TODO agoncharuk: why? Initializing new raft group should specify the peers?
                    // TODO agoncharuk: can we split this into two builder flows?
                    throw new IllegalStateException("cannot specify both newRaft(peers, learners) and ConfState.(Voters, Learners)");
                }
                cs.voters(c.peers());
                cs.learners(c.learners());
            }

            Raft r = new Raft(
                c.id(),
                raftlog,
                c.maxSizePerMsg(),
                c.maxUncommittedEntriesSize(),
                new Tracker(c.maxInflightMsgs()),
                c.electionTick(),
                c.heartbeatTick(),
                logger,
                c.checkQuorum(),
                c.preVote(),
                new ReadOnly(c.readOnlyOption()),
                c.disableProposalForwarding()
            );

            RestoreResult restore = Restore.restore(
                new Changer(
                    r.prs.config(),
                    r.prs.progress(),
                    raftlog.lastIndex(),
                    r.prs.maxInflight()
                ),
                cs);

            TrackerConfig cfg = restore.trackerConfig();
            ProgressMap prs = restore.progressMap();

            assertConfStatesEquivalent(logger, cs, r.switchToConfig(cfg, prs));

            if (!hs.isEmpty())
                r.loadState(hs);

            // TODO agoncharuk: move to log construction
            if (c.applied() > 0)
                raftlog.appliedTo(c.applied());

            r.becomeFollower(r.term, null);

            logger.debug("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
                r.id, strings.Join(r.prs.config().voters().ids(), ","), r.term,
                raftlog.committed(), raftlog.applied(), raftlog.lastIndex(), raftlog.lastTerm());

            return r;
        }
    }
}
