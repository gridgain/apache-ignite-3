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

package org.apache.ignite.internal.replication.raft.quorum;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.VoteResult;

import static org.apache.ignite.internal.replication.raft.VoteResult.VoteLost;
import static org.apache.ignite.internal.replication.raft.VoteResult.VotePending;

/**
 * JointConfig is a configuration of two groups of(possibly overlapping)
 * majority configurations. Decisions require the support of both majorities.
 */
public class JointConfig {
    private MajorityConfig incoming;
    private MajorityConfig outgoing;

    public JointConfig(MajorityConfig incoming, MajorityConfig outgoing) {
        this.incoming = incoming;
        this.outgoing = outgoing;
    }

    public static JointConfig of(Set<UUID> incoming, Set<UUID> outgoing) {
        return new JointConfig(
            new MajorityConfig(incoming == null ? Collections.emptySet() : incoming),
            outgoing == null || outgoing.isEmpty() ? null : new MajorityConfig(outgoing)
        );
    }

    public Set<UUID> ids() {
        if (outgoing == null)
            return incoming.ids();

        // Sorted set is requred to make logs stable for data-driven tests.
        Set<UUID> res = new TreeSet<>(incoming.ids());
        res.addAll(outgoing.ids());

        return Collections.unmodifiableSet(res);
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending, lost, or won. A joint quorum
    // requires both majority quorums to vote in favor.
    public VoteResult voteResult(Map<UUID, Boolean> votes) {
        VoteResult resMain = incoming.voteResult(votes);

        if (outgoing == null)
            return resMain;

        VoteResult resTrans = outgoing.voteResult(votes);

        if (resMain == resTrans) {
            // If they agree, return the agreed state.
            return resMain;
        }

        if (resMain == VoteLost || resTrans == VoteLost) {
            // If either config has lost, loss is the only possible outcome.
            return VoteLost;
        }

        // One side won, the other one is pending, so the whole outcome is.
        return VotePending;
    }

    // CommittedIndex returns the largest committed index for the given joint
    // quorum. An index is jointly committed if it is committed in both constituent
    // majorities.
    public long committedIndex(AckedIndexer l) {
        long idx0 = incoming.committedIndex(l);

        long idx1 = outgoing == null ? Long.MAX_VALUE : outgoing.committedIndex(l);

        return Math.min(idx0, idx1);
    }


    public Set<UUID> incoming() {
        return incoming == null ? null : incoming.ids();
    }

    public Set<UUID> outgoing() {
        return outgoing == null ? null : outgoing.ids();
    }

    public boolean isSingleton() {
        return outgoing == null && incoming.ids().size() == 1;
    }

    public boolean isJoint() {
        return outgoing != null;
    }
}
