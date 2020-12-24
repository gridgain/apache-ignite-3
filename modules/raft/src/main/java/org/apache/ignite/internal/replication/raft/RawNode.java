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

import java.util.UUID;

import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.storage.ConfChange;

import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgHup;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgProp;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgReadIndex;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgSnapStatus;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgTransferLeader;
import static org.apache.ignite.internal.replication.raft.message.MessageType.MsgUnreachable;

/**
 *
 */
public class RawNode {
    private Raft raft;
    private SoftState prevSoftState;
    private HardState prevHardSt;

    // Tick advances the internal logical clock by a single tick.
    public void tick() {
        raft.tick();
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
        raft.electionElapsed++;
    }

    // Campaign causes this RawNode to transition to candidate state.
    public void campaign() {
        return raft.step(MsgHup);
    }

    // Propose proposes data be appended to the raft log.
    public void propose(byte[] data) {
        raft.step(MsgProp, data);
    }

    // ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
    // details.
    public void proposeConfChange(ConfChange cc) {
        Message m = confChangeToMsg(cc);

        raft.step(m);
    }

    // ApplyConfChange applies a config change to the local node. The app must call
    // this when it applies a configuration change, except when it decides to reject
    // the configuration change, in which case no call must take place.
    public ConfigState applyConfChange(ConfChange cc) {
        return raft.applyConfChange(cc);
    }

    // Step advances the state machine using the given message.
    public void step(Message m) {
        // ignore unexpected local messages receiving over network
        if (isLocalMsg(m.type()))
            throw new IllegalArgumentException("Message is local: " + m);

        // TODO agoncharuk: this validation should happen inside raft state machine.
        Progress pr = raft.progress(m.from());

        if (pr != null || !isResponseMsg(m.type()))
            raft.step(m);
        else
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
        return new Ready(raft, prevSoftState, prevHardSt);
    }

    // acceptReady is called when the consumer of the RawNode has decided to go
    // ahead and handle a Ready. Nothing must alter the state of the RawNode between
    // this call and the prior call to Ready().
    public void acceptReady(Ready rd) {
        if (rd.softState() != null)
            prevSoftState = rd.softState();

        if (!rd.readStates().isEmpty())
            raft.readStates = null;

        raft.msgs = null;
    }

    // HasReady called when RawNode user need to check if any Ready pending.
    // Checking logic in this method should be consistent with Ready.containsUpdates().
    public boolean hasReady() {
        if (!raft.softState().equals(prevSoftState))
            return true;

        HardState hardSt = raft.hardState();

        if (!hardSt.isEmpty() && !hardSt.equals(prevHardSt))
            return true;

        if (raft.raftLog.hasPendingSnapshot())
            return true;

        if (!raft.msgs.isEmpty() || !raft.raftLog.unstableEntries().isEmpty() || raft.raftLog.hasNextEnts())
            return true;

        return !raft.readStates.isEmpty();
    }

    // Advance notifies the RawNode that the application has applied and saved progress in the
    // last Ready results.
    public void advance(Ready rd) {
        if (!rd.hardState().isEmpty())
            prevHardSt = rd.hardState();

        raft.advance(rd);
    }

    // Status returns the current status of the given group. This allocates, see
    // BasicStatus and WithProgress for allocation-friendlier choices.
    public Status status() {
        return raft.getStatus();
    }

    // BasicStatus returns a BasicStatus. Notably this does not contain the
    // Progress map; see WithProgress for an allocation-free way to inspect it.
    public BasicStatus basicStatus() {
        return raft.getBasicStatus();
    }

    // ProgressType indicates the type of replica a Progress corresponds to.
    enum ProgressType {
        // ProgressTypePeer accompanies a Progress for a regular peer replica.
        ProgressTypePeer,
        // ProgressTypeLearner accompanies a Progress for a learner replica.
        ProgressTypeLearner
    }

    // WithProgress is a helper to introspect the Progress for this node and its
    // peers.
    public void withProgress(ProgressVisotor visitor) {
        raft.prs.foreach((id, progress) -> {
            ProgressType typ = ProgressType.ProgressTypePeer;

            if (progress.isLearner())
                typ = ProgressType.ProgressTypeLearner;

            Progress p = new Progress(progress.match(), progress.next(), null, progress.isLearner());

            visitor.accept(id, typ, p);
        });
    }

    // ReportUnreachable reports the given node is not reachable for the last send.
    public void reportUnreachable(UUID id) {
        raft.step(MsgUnreachable, id);
    }

    // ReportSnapshot reports the status of the sent snapshot.
    public void ReportSnapshot(UUID id, SnapshotStatus status) {
        boolean rej = status == SnapshotFailure;

        raft.step(MsgSnapStatus, id, rej);
    }

    // TransferLeader tries to transfer leadership to the given transferee.
    public void TransferLeader(UUID transferee) {
        raft.step(MsgTransferLeader, transferee);
    }

    // ReadIndex requests a read state. The read state will be set in ready.
    // Read State has a read index. Once the application advances further than the read
    // index, any linearizable read requests issued before the read request can be
    // processed safely. The read state will have the same rctx attached.
    public void readIndex(byte[] rctx) {
        raft.step(MsgReadIndex, rctx);
    }
}
