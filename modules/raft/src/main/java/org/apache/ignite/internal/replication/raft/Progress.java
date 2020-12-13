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

/**
 *
 */
public class Progress {
    public enum ProgressState {
        // StateProbe indicates a follower whose last index isn't known. Such a
        // follower is "probed" (i.e. an append sent periodically) to narrow down
        // its last index. In the ideal (and common) case, only one round of probing
        // is necessary as the follower will react with a hint. Followers that are
        // probed over extended periods of time are often offline.
        StateProbe,

        // StateReplicate is the state steady in which a follower eagerly receives
        // log entries to append to its log.
        StateReplicate,

        // StateSnapshot indicates a follower that needs log entries not available
        // from the leader's Raft log. Such a follower needs a full snapshot to
        // return to StateReplicate.
        StateSnapshot;
    }

    public Progress(long match, long next, Inflights inflights, boolean learner) {
        this.match = match;
        this.next = next;
        this.inflights = inflights;
        this.learner = learner;
    }

    private long match;

    private long next;

    // state defines how the leader should interact with the follower.
    //
    // When in StateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in StateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in StateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    private ProgressState state;

    // pendingSnapshot is used in StateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. Raft will not resend snapshot until the pending one
    // is reported to be failed.
    private long pendingSnapshot;

    // recentActive is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // recentActive can be reset to false after an election timeout.
    //
    // TODO(tbg): the leader should always have this set to true.
    private boolean recentActive;

    /**
     * probeSent is used while this follower is in StateProbe. When probeSent is
     * true, raft should pause sending replication message to this peer until
     * probeSent is reset.
     *
     * @see #isPaused()
     */
    private boolean probeSent;

    // Inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as maxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeLE with the index of the last
    // received entry.
    private Inflights inflights;

    // IsLearner is true if this progress is tracked for a learner.
    private boolean learner;

    public long next() {
        return next;
    }

    public long match() {
        return match;
    }

    public ProgressState state() {
        return state;
    }

    // becomeProbe() transitions into StateProbe. next is reset to match + 1 or,
    // optionally and if larger, the index of the pending snapshot.
    public void becomeProbe() {
        // If the original state is StateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (state == ProgressState.StateSnapshot) {
            long pendingSnapshot = this.pendingSnapshot;

            resetState(ProgressState.StateProbe);

            next = Math.max(match + 1, pendingSnapshot + 1);
        }
        else {
            resetState(ProgressState.StateProbe);

            next = match + 1;
        }
    }

    // BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
    public void becomeReplicate() {
        resetState(ProgressState.StateReplicate);

        next = match + 1;
    }

    // BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
    // snapshot index.
    public void becomeSnapshot(long snapshotIdx) {
        resetState(ProgressState.StateSnapshot);

        pendingSnapshot = snapshotIdx;
    }

    // ResetState moves the Progress into the specified State, resetting ProbeSent,
    // PendingSnapshot, and Inflights.
    private void resetState(ProgressState state) {
        probeSent = false;
        pendingSnapshot = 0;
        this.state = state;
        inflights.reset();
    }

    public Inflights inflights() {
        return inflights;
    }

    public void probeSent(boolean sent) {
        probeSent = sent;
    }

    public void recentActive(boolean active) {
        recentActive = active;
    }

    public boolean recentActive() {
        return recentActive;
    }

    public long pendingSnapshot() {
        return pendingSnapshot;
    }

    public boolean maybeUpdate(long n) {
        boolean updated = false;

        if (match < n) {
            match = n;

            updated = true;

            probeSent = false;
        }

        next = Math.max(next, n + 1);

        return updated;
    }

    // maybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
    // arguments are the index the follower rejected to append to its log, and its
    // last index.
    //
    // Rejections can happen spuriously as messages are sent out of order or
    // duplicated. In such cases, the rejection pertains to an index that the
    // Progress already knows were previously acknowledged, and false is returned
    // without changing the Progress.
    //
    // If the rejection is genuine, Next is lowered sensibly, and the Progress is
    // cleared for sending log entries.
    public boolean maybeDecreaseTo(long rejected, long last) {
        if (state == ProgressState.StateReplicate) {
            // The rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if (rejected <= match)
                return false;

            // Directly decrease next to match + 1.
            //
            // TODO(tbg): why not use last if it's larger?
            next = match + 1;

            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1. This
        // is because non-replicating followers are probed one entry at a time.
        if (next - 1 != rejected)
            return false;

        next = Math.max(Math.min(rejected, last + 1), 1);

        probeSent = false;

        return true;
    }


    public boolean isLearner() {
        return learner;
    }

    // OptimisticUpdate signals that appends all the way up to and including index n
    // are in-flight. As a result, Next is increased to n+1.
    public void optimisticUpdate(long n) {
        next = n + 1;
    }

    // isPaused returns whether sending log entries to this node has been throttled.
    // This is done when a node has rejected recent MsgApps, is currently waiting
    // for a snapshot, or has reached the MaxInflightMsgs limit. In normal
    // operation, this is false. A throttled node will be contacted less frequently
    // until it has reached a state in which it's able to accept a steady stream of
    // log entries again.
    public boolean isPaused() {
        switch (state) {
            case StateProbe:
                return probeSent;

            case StateReplicate:
                return inflights.full();

            case StateSnapshot:
                return true;

            default:
                throw new AssertionError("Unexpected state");
        }
    }
}
