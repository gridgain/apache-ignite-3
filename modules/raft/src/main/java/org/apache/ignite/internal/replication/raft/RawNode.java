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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.apache.ignite.lang.IgniteUuid;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_ELECTION;
import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_PRE_ELECTION;
import static org.apache.ignite.internal.replication.raft.CampaignType.CAMPAIGN_TRANSFER;
import static org.apache.ignite.internal.replication.raft.Progress.ProgressState.StateReplicate;
import static org.apache.ignite.internal.replication.raft.Progress.ProgressState.StateSnapshot;
import static org.apache.ignite.internal.replication.raft.VoteResult.VoteWon;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgApp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHeartbeat;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgPreVote;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgPreVoteResp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgSnap;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgVote;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgVoteResp;
import static org.apache.ignite.internal.replication.raft.storage.Entry.EntryType.ENTRY_CONF_CHANGE;

/**
 * Followers will drop proposals, rather than forwarding them to the leader. One use case for
 * this feature is in a situation where the Raft leader is used to compute the data of a proposal,
 * for example, adding a timestamp from a hybrid logical clock to data in a monotonically increasing way.
 * Dropping the proposal instead of forwarding will prevent a follower with an inaccurate hybrid logical
 * clock from assigning the timestamp and then forwarding the data to the leader.
 */
public class RawNode<T> {
    private final UUID id;
    private final boolean checkQuorum;
    private final boolean preVote;
    private final long maxMsgSize;
    private final long maxUncommittedSize;
    private final int heartbeatTimeout;
    private final int electionTimeout;
    private final Random rnd;
    private final MessageFactory msgFactory;
    private final EntryFactory entryFactory;
    private final Logger logger;

    // ------ Hard state fields. ------
    /** Current term of raft node. */
    private long term;

    /** Current vote of raft node. {@code null} if hasn't voted in current term. */
    private UUID vote;
    // --------------------------------

    // ------ Soft state fields. ------
    /** Current node state type. */
    private StateType state;

    /** The known leader ID in current term. */
    private UUID lead;

    /**
     * leadTransferee is id of the leader transfer target when its value is not null.
     * Follow the procedure defined in raft thesis 3.10.
     */
    private UUID leadTransferee;
    // --------------------------------

    // ------ Ready fields. ----------
    /** Results of read-only requests to return. */
    private List<ReadState> readStates = new ArrayList<>();

    /** Messages to send to remote peers. */
    private List<Message> msgs = new ArrayList<>();
    // --------------------------------

    /** The log as it is viewed by the state machine. May differ from the actual durable state of the log. */
    private final RaftLog raftLog;

    // TODO(tbg): rename to trk.
    private Tracker prs;

    // isLearner is true if the local raft node is a learner.
    // TODO agoncharuk looks like this may be moved to Progress.
    private boolean isLearner;

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

    /** Aux structure to track read-only requests. */
    private ReadOnly readOnly;

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    private int electionElapsed;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    private int heartbeatElapsed;

    /**
     * randomizedElectionTimeout is a random number between [electiontimeout, 2 * electiontimeout - 1].
     * It gets reset when raft changes its state to follower or candidate, and each time a candidate times out.
     */
    private int randomizedElectionTimeout;

    private Runnable tick;
    // TODO agoncharuk settable by tests - need to get rid of this.
    StepFunction step;

    private SoftState prevSoftState;
    private HardState prevHardState;

    RawNode(
        UUID id,
        RaftLog raftLog,
        int maxSizePerMsg,
        long maxUncommittedEntriesSize,
        int maxInflightMsgs,
        int electionTick,
        int heartbeatTick,
        boolean checkQuorum,
        boolean preVote,
        ReadOnlyOption readOnlyOption,
        MessageFactory msgFactory,
        EntryFactory entryFactory,
        Logger logger,
        Random rnd
    ) {
        this.id = id;
        this.raftLog = raftLog;
        maxMsgSize = maxSizePerMsg;
        maxUncommittedSize = maxUncommittedEntriesSize;
        electionTimeout = electionTick;
        heartbeatTimeout = heartbeatTick;
        this.checkQuorum = checkQuorum;
        this.preVote = preVote;
        this.msgFactory = msgFactory;
        this.entryFactory = entryFactory;
        this.logger = logger;
        this.rnd = rnd;

        prs = new Tracker(maxInflightMsgs);
        readOnly = new ReadOnly(readOnlyOption);
    }

    // Tick advances the internal logical clock by a single tick.
    public void tick() {
        tick.run();
    }

    // TickQuiesced advances the internal logical clock by a single tick without
    // performing any other state machine processing. It allows the caller to avoid
    // periodic heartbeats and elections when all of the peers in a Raft group are
    // known to be at the same state. Expected usage is to periodically invoke Tick
    // or TickQuiesced depending on whether the group is "active" or "quiesced".
    //
    // WARNING: Be very careful about using this method as it subverts the Raft
    // state machine. You should probably be using Tick instead.
    public void tickQuiesced() {
        electionElapsed++;
    }

    // Campaign causes this RawNode to transition to candidate state.
    public boolean campaign() {
        return hup(preVote ? CAMPAIGN_PRE_ELECTION : CAMPAIGN_ELECTION);
    }

    // Proposes data be appended to the raft log.
    public ProposeReceipt propose(T data) {
        checkValidLeader();

        return leaderAppendEntries(Collections.singletonList(new UserData<>(data)));
    }

    // Proposes data batch be appended to the raft log.
    public ProposeReceipt propose(List<? extends T> data) {
        checkValidLeader();

        return leaderAppendEntries(data.stream().map(e -> new UserData<>(e)).collect(Collectors.toList()));
    }

    // ProposeConfChange proposes a config change.
    public ProposeReceipt proposeConfChange(ConfChange cc) {
        checkValidLeader();

        boolean alreadyPending = pendingConfIndex > raftLog.applied();
        boolean alreadyJoint = prs.config().voters().isJoint();

        boolean wantsLeaveJoint = cc.changes().isEmpty();

        String refused = null;

        if (alreadyPending)
            refused = String.format("possible unapplied conf change at index %d (applied to %d)", pendingConfIndex,
                raftLog.applied());
        else if (alreadyJoint && !wantsLeaveJoint)
            refused = String.format("must transition out of joint config first: %s", prs.config().voters());
        else if (!alreadyJoint && wantsLeaveJoint)
            refused = String.format("%s not in joint state, refusing empty conf change", id);

        if (refused != null) {
            logger.info("{} ignoring conf change {} at config {}: {}", id, cc, prs.config(), refused);

            throw new InvalidConfigTransitionException(refused);
        }

        ProposeReceipt receipt = leaderAppendEntries(Collections.singletonList(cc));

        assert receipt.startIndex() == receipt.endIndex() : "Invalid receipt for single-entry propose: " + receipt;

        pendingConfIndex = receipt.startIndex();

        return receipt;
    }

    /**
     * ReadIndex requests a read state. The read state will be set in ready.
     * Read State has a read index. Once the application advances further than the read
     * index, any linearizable read requests issued before the read request can be
     * processed safely. The read state will have the same rctx attached.
     */
    public void requestReadIndex(IgniteUuid reqCtx) {
        checkValidLeader();

        // only one voting member (the leader) in the cluster
        if (prs.isSingleton()) {
            responseToReadIndexReq(reqCtx, raftLog.committed());

            return;
        }

        // Reject read only request when this leader has not committed any log entry at its term.
        if (!committedEntryInCurrentTerm()) {
            responseToReadIndexReq(reqCtx, -1);

            return;
        }

        // thinking: use an interally defined context instead of the user given context.
        // We can express this in terms of the term and index instead of a user-supplied value.
        // This would allow multiple reads to piggyback on the same message.
        switch (readOnly.option()) {
            // If more than the local vote is needed, go through a full broadcast.
            case READ_ONLY_SAFE: {
                readOnly.addRequest(raftLog.committed(), reqCtx);

                // The local node automatically acks the request.
                readOnly.recvAck(id, reqCtx);
                bcastHeartbeatWithCtx(reqCtx);

                break;
            }

            case READ_ONLY_LEASE_BASED: {
                responseToReadIndexReq(reqCtx, raftLog.committed());

                break;
            }
        }

        return;

    }

    // ApplyConfChange applies a config change to the local node. The app must call
    // this when it applies a configuration change, except when it decides to reject
    // the configuration change, in which case no call must take place.
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

    // Step advances the state machine using the given message.
    public void step(Message m) {
        Progress pr = prs.progress(m.from());

        if (pr != null || !m.type().isResponse())
            stepInternal(m);
        else
            // TODO agoncharuk not sure if this is an exception: we just received a stale response message.
            throw new IllegalStateException("Peer not found: " + m.from());
    }

    // Ready returns the outstanding work that the application needs to handle. This
    // includes appending and applying entries or a snapshot, updating the HardState,
    // and sending messages. The returned Ready() *must* be handled and subsequently
    // passed back via Advance().
    public Ready ready() {
        Ready rd = readyWithoutAccept();

        acceptReady(rd);

        return rd;
    }

    // readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
    // is no obligation that the Ready must be handled.
    public Ready readyWithoutAccept() {
        SoftState softState = softState();
        HardState hardState = hardState();

        return new Ready(
            raftLog.unstableEntries(),
            raftLog.nextEntries(),
            msgs,
            softState.equals(prevSoftState) ? null : softState,
            hardState.equals(prevHardState) ? null : hardState,
            raftLog.unstableSnapshot(),
            readStates);
    }

    // acceptReady is called when the consumer of the RawNode has decided to go
    // ahead and handle a Ready. Nothing must alter the state of the RawNode between
    // this call and the prior call to readyWithoutAccept().
    public void acceptReady(Ready rd) {
        if (rd.softState() != null)
            prevSoftState = rd.softState();

        if (!rd.readStates().isEmpty())
            readStates = new ArrayList<>();

        msgs = new ArrayList<>();
    }

    // HasReady called when RawNode user need to check if any Ready pending.
    // Checking logic in this method should be consistent with Ready.containsUpdates().
    public boolean hasReady() {
        if (!softState().equals(prevSoftState))
            return true;

        HardState hardSt = hardState();

        if (!hardSt.isEmpty() && !hardSt.equals(prevHardState))
            return true;

        if (raftLog.hasPendingSnapshot())
            return true;

        if (!msgs.isEmpty() || !raftLog.unstableEntries().isEmpty() || raftLog.hasNextEntries())
            return true;

        return !readStates.isEmpty();
    }

    // Advance notifies the RawNode that the application has applied and saved progress in the
    // last Ready results.
    public void advance(Ready rd) {
        if (!rd.hardState().isEmpty())
            prevHardState = rd.hardState();

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
                // TODO agoncharuk treating null as zero change is wrong and needs to be fixed.
                ConfChange zeroChange = null;

                // There's no way in which this proposal should be able to be rejected.
                if (!appendEntries(Collections.<LogData>singletonList(zeroChange)))
                    unrecoverable("refused un-refusable auto-leaving ConfChange");

                pendingConfIndex = raftLog.lastIndex();
                logger.info("initiating automatic transition out of joint configuration {}", prs.config());
            }
        }

        if (!rd.entries().isEmpty()) {
            Entry e = rd.entries().get(rd.entries().size() - 1);

            raftLog.stableTo(e.index(), e.term());
        }

        if (rd.hasSnapshot())
            raftLog.stableSnapshotTo(rd.snapshot().metadata().index());
    }

    // Status returns the current status of the given group. This allocates, see
    // BasicStatus and WithProgress for allocation-friendlier choices.
    public Status status() {
        return new Status(basicStatus(), prs.config(), new ProgressMap(prs.progress()));
    }

    // BasicStatus returns a BasicStatus. Notably this does not contain the
    // Progress map; see WithProgress for an allocation-free way to inspect it.
    public BasicStatus basicStatus() {
        return new BasicStatus(
            id,
            hardState(),
            softState(),
            raftLog.applied(),
            leadTransferee);
    }

    // WithProgress is a helper to introspect the Progress for this node and its
    // peers.
    public void withProgress(ProgressVisitor visitor) {
        prs.foreach((id, progress) -> {
            ProgressType typ = ProgressType.ProgressTypePeer;

            if (progress.isLearner())
                typ = ProgressType.ProgressTypeLearner;

            Progress p = new Progress(progress.match(), progress.next(), null, progress.isLearner());

            visitor.accept(id, typ, p);
        });
    }

    // ReportUnreachable reports the given node is not reachable for the last send.
    public void reportUnreachable(UUID id) {
        if (state == StateType.STATE_LEADER) {
            Progress pr = prs.progress(id);

            if (pr == null) {
                logger.debug("{} no progress available for {}", this.id, id);

                return;
            }

            // During optimistic replication, if the remote becomes unreachable,
            // there is huge probability that a MsgApp is lost.
            if (pr.state() == StateReplicate)
                pr.becomeProbe();

            logger.debug("{} failed to send message to {} because it is unreachable [{}]", this.id, id, pr);
        }
    }

    // ReportSnapshot reports the status of the sent snapshot.
    public void ReportSnapshot(UUID id, SnapshotStatus status) {
        boolean rej = status == SnapshotStatus.SnapshotFailure;

        if (state == StateType.STATE_LEADER) {
            Progress pr = prs.progress(id);

            if (pr == null) {
                logger.debug("{} no progress available for {}", this.id, id);

                return;
            }

            if (pr.state() != StateSnapshot)
                return;

            // TODO(tbg): this code is very similar to the snapshot handling in
            // MsgAppResp above. In fact, the code there is more correct than the
            // code here and should likely be updated to match (or even better, the
            // logic pulled into a newly created Progress state machine handler).
            if (!rej) {
                pr.becomeProbe();

                logger.debug("{} snapshot succeeded, resumed sending replication messages to {} [{}]", this.id, id, pr);
            }
            else {
                // NB: the order here matters or we'll be probing erroneously from
                // the snapshot index, but the snapshot never applied.
                pr.pendingSnapshot(0);

                pr.becomeProbe();

                logger.debug("{} snapshot failed, resumed sending replication messages to {} [{}]", this.id, id, pr);
            }

            // If snapshot finish, wait for the MsgAppResp from the remote node before sending
            // out the next MsgApp.
            // If snapshot failure, wait for a heartbeat interval before next try
            pr.probeSent(true);
        }
    }

    // TransferLeader tries to transfer leadership to the given transferee.
    public void transferLeader(UUID leadTransferee) {
        if (state == StateType.STATE_LEADER) {
            Progress pr = prs.progress(leadTransferee);

            if (pr == null) {
                logger.debug("{} unknown remote peer {}. Ignored transferring leadership", id, leadTransferee);
            }

            if (pr.isLearner()) {
                logger.debug("{} remote node {} is learner. Ignored transferring leadership", id, leadTransferee);

                return;
            }

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
                send(msgFactory.newTimeoutNowRequest(id, leadTransferee, term));

                logger.info("{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log", id, leadTransferee, leadTransferee);
            }
            else
                sendAppend(leadTransferee);
        }
        else
            throw new NotLeaderException(lead);
    }

    private void checkValidLeader() {
        if (state != StateType.STATE_LEADER) {
            logger.info("{} not leader at term {} (known leader is {}); dropping proposal", id, term, lead);

            throw new NotLeaderException(lead);
        }
        else {
            if (prs.progress(id) == null) {
                // If we are not currently a member of the range (i.e. this node
                // was removed from the configuration while serving as leader),
                // drop any new proposals.
                throw new NotLeaderException();
            }

            if (leadTransferee != null) {
                logger.debug("{} [term {}] transfer leadership to {} is in progress; dropping request", id, term, leadTransferee);

                throw new TransferLeaderException(leadTransferee);
            }
        }
    }

    // TODO: 24.12.20 Should we rename it? Originally it's an raft.step(Message m).
    private void stepInternal(Message m) {
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
                send(msgFactory.newAppendEntriesResponse(
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

                send(msgFactory.newVoteResponse(
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
                        msgFactory.newVoteResponse(
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

                    send(msgFactory.newVoteResponse(
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

    private SoftState softState() {
        return new SoftState(lead, state);
    }

    private HardState hardState() {
        return new HardState(
            term,
            vote,
            raftLog.committed()
        );
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

    // reduceUncommittedSize accounts for the newly committed entries by decreasing
    // the uncommitted entry size limit.
    void reduceUncommittedSize(List<Entry> ents) {
        if (uncommittedSize == 0)
            // Fast-path for followers, that do not track or enforce the limit.
            return;

        long s = 0;

        for (Entry ent : ents)
            s += entryFactory.payloadSize(ent);

        if (s > uncommittedSize) {
            // uncommittedSize may underestimate the size of the uncommitted Raft
            // log tail but will never overestimate it. Saturate at 0 instead of
            // allowing overflow.
            uncommittedSize = 0;
        }
        else
            uncommittedSize -= s;
    }

    long uncommittedSize() {
        return uncommittedSize;
    }

    // increaseUncommittedSize computes the size of the proposed entries and
    // determines whether they would push leader over its maxUncommittedSize limit.
    // If the new entries would exceed the limit, the method returns false. If not,
    // the increase in uncommitted entry size is recorded and the method returns
    // true.
    //
    // Empty payloads are never refused. This is used both for appending an empty
    // entry at a new leader's term, as well as leaving a joint configuration.
    private boolean increaseUncommittedSize(List<Entry> ents) {
        long s = 0;

        for (Entry ent : ents)
            s += entryFactory.payloadSize(ent);

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

    private ProposeReceipt leaderAppendEntries(List<LogData> entries) {
        long start = raftLog.lastIndex() + 1;

        if (!appendEntries(entries))
            throw new ProposalDroppedException();

        long end = raftLog.lastIndex();

        assert end - start + 1 == entries.size() : "Produced invalid log offsets [entries=" + entries +
            ", start=" + start + ", end=" + end + ']';

        bcastAppend();

        return new ProposeReceipt(term, start, end);
    }

    private boolean appendEntries(List<LogData> es) {
        // Track the size of this uncommitted proposal.
        List<Entry> entries = new ArrayList<>(es.size());

        long li = raftLog.lastIndex();

        for (int i = 0; i < es.size(); i++)
            entries.add(entryFactory.newEntry(term, li + i + 1, es.get(i)));

        if (!increaseUncommittedSize(entries)) {
            logger.debug(
                "{} appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
                id);

            // Drop the proposal.
            return false;
        }

        // use latest "last" index after truncate/append
        li = raftLog.append(entries);

        prs.progress(id).maybeUpdate(li);

        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        maybeCommit();

        return true;
    }

    void becomeFollower(long term, UUID lead) {
        step = this::stepFollower;
        reset(term);
        tick = this::tickElection;
        this.lead = lead;
        state = StateType.STATE_FOLLOWER;

        logger.debug("{} became follower at term {}", id, this.term);
    }

    void becomeCandidate() {
        if (state == StateType.STATE_LEADER)
            unrecoverable("invalid transition [leader -> candidate]");

        step = this::stepCandidate;
        reset(term + 1);
        tick = this::tickElection;
        vote = id;
        state = StateType.STATE_CANDIDATE;

        logger.debug("{} became candidate at term {}", id, term);
    }

    private void becomePreCandidate() {
        if (state == StateType.STATE_LEADER)
            unrecoverable("invalid transition [leader -> pre-candidate]");

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

    void becomeLeader() {
        if (state == StateType.STATE_FOLLOWER)
            unrecoverable("invalid transition [follower -> leader]");

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

        if (!appendEntries(Collections.singletonList(empty))) {
            // This won't happen because we just called reset(term) above.
            unrecoverable("Empty entry was dropped in becomeLeader()");
        }

        // As a special case, don't count the initial empty entry towards the
        // uncommitted log quota. This is because we want to preserve the
        // behavior of allowing one entry larger than quota if the current
        // usage is zero.
        reduceUncommittedSize(Collections.singletonList(
            entryFactory.newEntry(term, raftLog.lastIndex(), empty)));

        logger.info("{} became leader at term {}", id, term);
    }

    // send persists state to stable storage and then sends to its mailbox.
    public void send(Message m) {
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
            unrecoverable("term should be set when sending " + m.type());
        }

        msgs.add(m);
    }

    private boolean hup(CampaignType t) {
        if (state == StateType.STATE_LEADER) {
            logger.debug("{} ignoring hup request because already leader", id);

            return false;
        }

        if (!promotable()) {
            logger.warn("{} is unpromotable and can not campaign", id);

            return false;
        }

        List<Entry> ents = null;
        try {
            ents = raftLog.slice(raftLog.applied() + 1, raftLog.committed() + 1, Long.MAX_VALUE);
        }
        catch (CompactionException compactionException) {
            unrecoverable("unexpected error getting unapplied entries ({})", compactionException);
        }

        int n = numOfPendingConf(ents);

        if (n != 0 && raftLog.committed() > raftLog.applied()) {
            logger.warn("{} cannot campaign at term {} since there are still {} pending configuration changes to apply",
                id, term, n);

            return false;
        }

        logger.info("{} is starting a new election at term {}", id, term);

        campaign(t);

        return true;
    }

    // maybeCommit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // bcastAppend()).
    private boolean maybeCommit() {
        long mci = prs.committed();

        return raftLog.maybeCommit(mci, term);
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

            AppendEntriesRequest m = msgFactory.newAppendEntriesRequest(
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
                        unrecoverable("{} is sending append in unhandled state {}", id, pr.state());
                }
            }

            send(m);
        }
        catch (CompactionException compactionException) {
            // send snapshot if we failed to get term or entries
            if (!pr.recentActive()) {
                logger.debug("ignore sending snapshot to {} since it is not recently active", to);

                return false;
            }

            try {
                Snapshot snapshot = raftLog.snapshot();

                if (snapshot.isEmpty())
                    unrecoverable("need non-empty snapshot");

                long snapIdx = snapshot.metadata().index();
                long snapTerm = snapshot.metadata().term();

                Message m = msgFactory.newInstallSnapshotRequest(
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

    // sendAppend sends an append RPC with new entries (if any) and the
    // current commit index to the given peer.
    private void sendAppend(UUID to) {
        maybeSendAppend(to, true);
    }

    // sendHeartbeat sends a heartbeat message to the given peer.
    private void sendHeartbeat(UUID to, IgniteUuid ctx) {
        // Attach the commit as min(to.matched, committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        long commit = Math.min(prs.progress(to).match(), raftLog.committed());

        Message m = msgFactory.newHeartbeatRequest(
            id,
            to,
            term,
            commit,
            ctx);

        send(m);
    }

    // bcastAppend sends message, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    void bcastAppend() {
        prs.foreach((id, progress) -> {
            if (this.id.equals(id))
                return;

            sendAppend(id);
        });
    }

    // bcastHeartbeat sends message, without entries to all the peers.
    void bcastHeartbeat() {
        assert state == StateType.STATE_LEADER;

        IgniteUuid lastCtx = readOnly.lastPendingRequestCtx();

        bcastHeartbeatWithCtx(lastCtx);
    }

    private void bcastHeartbeatWithCtx(IgniteUuid ctx) {
        prs.foreach((id, progress) -> {
            if (this.id.equals(id))
                return;

            sendHeartbeat(id, ctx);
        });
    }

    private void abortLeaderTransfer() {
        leadTransferee = null;
    }

    private void stepLeader(Message m) {
        // Require a progress for m.from().
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
                        while (maybeSendAppend(m.from(), false));

                        // Transfer leadership is in progress.
                        if (m.from().equals(leadTransferee) && pr.match() == raftLog.lastIndex()) {
                            logger.info("{} sent MsgTimeoutNow to {} after received MsgAppResp", id, m.from());

                            send(msgFactory.newTimeoutNowRequest(id, m.from(), term));
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

                List<ReadIndexStatus> rss = readOnly.advance(res.context());

                for (ReadIndexStatus rs : rss)
                    responseToReadIndexReq(rs.context(), rs.index());

                break;
            }
        }
    }

    private void stepFollower(Message m) throws ProposalDroppedException {
        switch (m.type()) {
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

            case MsgTimeoutNow: {
                logger.info("{} [term {}] received MsgTimeoutNow from {} and starts an election to get leadership.", id, term, m.from());

                // Leadership transfers never use pre-vote even if r.preVote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
                hup(CampaignType.CAMPAIGN_TRANSFER);

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

    private void handleHeartbeat(HeartbeatRequest m) {
        raftLog.commitTo(m.commitIndex());

        send(msgFactory.newHeartbeatResponse(
            id,
            m.from(),
            term,
            m.context()));
    }

    // tickHeartbeat is run by leaders to send a heartbeat after heartbeatTimeout.
    private void tickHeartbeat() {
        heartbeatElapsed++;
        electionElapsed++;

        if (electionElapsed >= electionTimeout) {
            electionElapsed = 0;

            if (checkQuorum)
                doCheckQuorum();

            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (state == StateType.STATE_LEADER && leadTransferee != null)
                abortLeaderTransfer();
        }

        if (state != StateType.STATE_LEADER)
            return;

        if (heartbeatElapsed >= heartbeatTimeout) {
            heartbeatElapsed = 0;

            bcastHeartbeat();
        }
    }

    // tickElection is run by followers and candidates after r.electionTimeout.
    private void tickElection() {
        electionElapsed++;

        if (promotable() && pastElectionTimeout()) {
            electionElapsed = 0;

            hup(preVote ? CAMPAIGN_PRE_ELECTION : CAMPAIGN_ELECTION);
        }
    }

    private void doCheckQuorum() {
        assert state == StateType.STATE_LEADER;

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

    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    private boolean promotable() {
        Progress pr = prs.progress(id);

        return pr != null && !pr.isLearner() && !raftLog.hasPendingSnapshot();
    }

    private void resetRandomizedElectionTimeout() {
        randomizedElectionTimeout = electionTimeout + rnd.nextInt(electionTimeout);
    }

    private int numOfPendingConf(List<Entry> ents) {
        int n = 0;
        for (Entry ent : ents) {
            if (ent.type() == ENTRY_CONF_CHANGE)
                n++;
        }

        return n;
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

            send(msgFactory.newVoteRequest(
                id,
                rmtId,
                voteMsg == MsgPreVote,
                term,
                raftLog.lastIndex(),
                raftLog.lastTerm(),
                t == CAMPAIGN_TRANSFER));
        }
    }

    // committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
    private boolean committedEntryInCurrentTerm() {
        return raftLog.zeroTermOnErrCompacted(raftLog.term(raftLog.committed())) == term;
    }

    // responseToReadIndexReq updates the read states with a validated read index.
    private void responseToReadIndexReq(IgniteUuid ctx, long readIndex) {
        readStates.add(new ReadState(ctx, readIndex));
    }

    private void handleAppendEntries(AppendEntriesRequest req) {
        if (req.logIndex() < raftLog.committed()) {
            send(msgFactory.newAppendEntriesResponse(
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
            send(msgFactory.newAppendEntriesResponse(
                id,
                req.from(),
                term,
                lastIdx,
                /*reject*/false,
                0));
        else {
            logger.debug("{} [logterm: {}, index: {}] rejected MsgApp [logterm: {}, index: {}] from {}",
                id, raftLog.zeroTermOnErrCompacted(raftLog.term(req.logIndex())), req.logIndex(), req.logTerm(), req.logIndex(), req.from());

            send(msgFactory.newAppendEntriesResponse(
                id,
                req.from(),
                term,
                req.logIndex(),
                /*reject*/true,
                raftLog.lastIndex()));
        }
    }

    private void handleSnapshot(InstallSnapshotRequest m) {
        long sindex = m.snapshot().metadata().index();
        long sterm = m.snapshot().metadata().term();

        if (restore(m.snapshot())) {
            logger.info("{} [commit: {}] restored snapshot [index: {}, term: {}]",
                id, raftLog.committed(), sindex, sterm);

            send(msgFactory.newAppendEntriesResponse(
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

            send(msgFactory.newAppendEntriesResponse(
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

            ConfigState updatedConfState = switchToConfig(cfg, prs);

            checkConfStatesEquivalent(cs, updatedConfState);

            Progress pr = this.prs.progress(id);

            pr.maybeUpdate(pr.next() - 1); // TODO(tbg): this is untested and likely unneeded

            logger.info("{} [commit: {}, lastindex: {}, lastterm: {}] restored snapshot [index: {}, term: {}]",
                id, raftLog.committed(), raftLog.lastIndex(), raftLog.lastTerm(), s.metadata().index(), s.metadata().term());

            return true;

        }
        // TODO agoncharuk specific exception should be used here (thrown from restore)
        catch (Exception e) {
            // This should never happen. Either there's a bug in our config change
            // handling or the client corrupted the conf change.
            throw new UnrecoverableException(String.format("unable to restore config %s: %s", cs, e), e);
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
            unrecoverable("{} state.commit {} is out of range [{}, {}]", id, state.committed(), raftLog.committed(),
                raftLog.lastIndex());

        term = state.term();
        vote = state.vote();
    }

    void initFromState(InitialState initState) {
        HardState hs = initState.hardState();
        ConfigState cs = initState.configState();

        RestoreResult restore = Restore.restore(
            new Changer(
                prs.config(),
                prs.progress(),
                raftLog.lastIndex(),
                prs.maxInflight()
            ),
            cs);

        TrackerConfig trackerCfg = restore.trackerConfig();
        ProgressMap progressMap = restore.progressMap();

        ConfigState updatedConfState = switchToConfig(trackerCfg, progressMap);

        checkConfStatesEquivalent(cs, updatedConfState);

        if (!hs.isEmpty())
            loadState(hs);

        becomeFollower(term, null);

        logger.debug("Initialized Raft {} [peers: [{}], term: {}, commit: {}, applied: {}, lastindex: {}, lastterm: {}]",
            id,
            String.join(",", prs.config().voters().ids().stream().map(Object::toString).collect(Collectors.toList())),
            term,
            raftLog.committed(),
            raftLog.applied(),
            raftLog.lastIndex(),
            raftLog.lastTerm());
    }

    private void checkConfStatesEquivalent(ConfigState cs, ConfigState updatedConfState) {
        if (!updatedConfState.equals(cs))
            unrecoverable("Config states are not equivalent after config switch " +
                "[snapshot={}, switched={}]", cs, updatedConfState);
    }

    private void unrecoverable(String formatMsg, Object... args) {
        logger.error(formatMsg, args);

        throw new UnrecoverableException(MessageFormatter.arrayFormat(formatMsg, args).getMessage());
    }

    List<Message> readMessages() {
        List<Message> ret = msgs;

        msgs = new ArrayList<>();

        return ret;
    }

    Tracker tracker() {
        return prs;
    }

    RaftLog raftLog() {
        return raftLog;
    }
}
