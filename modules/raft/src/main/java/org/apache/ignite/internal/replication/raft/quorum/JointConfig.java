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
    private MajorityConfig main;
    private MajorityConfig transition;

    public JointConfig(MajorityConfig main, MajorityConfig transition) {
        this.main = main;
        this.transition = transition;
    }

    public Set<UUID> ids() {
        if (transition == null)
            return main.ids();

        // Sorted set is requred to make logs stable for data-driven tests.
        Set<UUID> res = new TreeSet<>(main.ids());
        res.addAll(transition.ids());

        return Collections.unmodifiableSet(res);
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending, lost, or won. A joint quorum
    // requires both majority quorums to vote in favor.
    public VoteResult voteResult(Map<UUID, Boolean> votes) {
        VoteResult resMain = main.voteResult(votes);

        if (transition == null)
            return resMain;

        VoteResult resTrans = transition.voteResult(votes);

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

    public boolean isSingleton() {
        return transition == null && main.ids().size() == 1;
    }

    public boolean isJoint() {
        return transition != null;
    }
}
