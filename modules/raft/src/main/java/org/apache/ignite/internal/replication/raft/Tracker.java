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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.apache.ignite.internal.replication.raft.quorum.JointConfig;
import org.apache.ignite.internal.replication.raft.quorum.MajorityConfig;

import static org.apache.ignite.internal.replication.raft.VoteResult.VoteWon;

/**
 *
 */
public class Tracker {
    private TrackerConfig cfg;
    private ProgressMap progress;
    private Map<UUID, Boolean> votes;
    private int maxInflight;

    public Tracker(int maxInflight) {
        this.maxInflight = maxInflight;

        cfg = new TrackerConfig(
            new JointConfig(new MajorityConfig(Collections.emptySet()), null), // Voters
            null, // Learners
            null  // Learners next
        );

        votes = new HashMap<>();
        progress = new ProgressMap();
    }

    public ConfigState configState() {
        return new ConfigState(
            cfg.voters().main().ids(), // TODO agoncharuk: arrays should be sorted
            cfg.voters().transition().ids(), // VotersOutgoing TODO agoncharuk: NPE here
            cfg.learners(),
            cfg.learnersNext(),
            cfg.autoLeave()
        );
    }

    public void foreach(BiConsumer<UUID, Progress> consumer) {
        // TODO agoncharuk.
    }

    public void visit(BiFunction<UUID, Progress, Progress> updater) {

    }

    public Progress progress(UUID id) {
        // TODO
        return null;
    }

    // QuorumActive returns true if the quorum is active from the view of the local
    // raft state machine. Otherwise, it returns false.
    public boolean quorumActive() {
        Map<UUID, Boolean> votes = new HashMap<>();

        foreach((id, progress) -> {
            if (progress.isLearner())
                return;

            votes.put(id, progress.recentActive());
        });

        return cfg.voters().voteResult(votes) == VoteWon;
    }


    public TrackerConfig config() {
        return cfg;
    }

    public int maxInflight() {
        return maxInflight;
    }

    public void resetVotes() {
        votes.clear();
    }

    public void reset(TrackerConfig cfg, ProgressMap progress) {
        this.cfg = cfg;
        this.progress = progress;
    }

    public long committed() {
        return 0;
    }

    // IsSingleton returns true if (and only if) there is only one voting member
    // (i.e. the leader) in the current configuration.
    public boolean isSingleton() {
        return cfg.voters().isSingleton();
    }
}
