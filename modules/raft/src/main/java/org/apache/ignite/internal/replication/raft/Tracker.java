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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.apache.ignite.internal.replication.raft.quorum.JointConfig;

import static org.apache.ignite.internal.replication.raft.VoteResult.VoteWon;

/**
 *
 */
public class Tracker {
    private TrackerConfig cfg;
    private ProgressMap progress;
    private Map<UUID, Boolean> votes;
    private final int maxInflight;

    public Tracker(int maxInflight) {
        this.maxInflight = maxInflight;

        cfg = new TrackerConfig(
            // Voters
            JointConfig.of(null, null),
            null, // Learners
            null,  // Learners next,
            false
        );

        votes = new HashMap<>();
        progress = new ProgressMap();
    }

    public ConfigState configState() {
        return new ConfigState(
            cfg.voters().incoming(), // TODO agoncharuk: arrays should be sorted
            cfg.voters().outgoing(), // VotersOutgoing TODO agoncharuk: NPE here
            cfg.learners(),
            cfg.learnersNext(),
            cfg.autoLeave()
        );
    }

    // RecordVote records that the node with the given id voted for this Raft
    // instance if v == true (and declined it otherwise).
    public void recordVote(UUID id, boolean v) {
        if (!votes.containsKey(id))
            votes.put(id, v);
    }

    // TallyVotes returns the number of granted and rejected Votes, and whether the
    // election outcome is known.
    public PollResult tallyVotes() {
        int granted = 0, rejected = 0;

        // Make sure to populate granted/rejected correctly even if the Votes slice
        // contains members no longer part of the configuration. This doesn't really
        // matter in the way the numbers are used (they're informational), but might
        // as well get it right.
        for (Map.Entry<UUID, Progress> entry : progress.entrySet()) {
            UUID id = entry.getKey();
            Progress pr = entry.getValue();

            if (pr.isLearner())
                continue;

            Boolean voted = votes.get(id);

            if (voted == null)
                continue;

            if (voted) {
                granted++;
            } else {
                rejected++;
            }
        }

        VoteResult result = config().voters().voteResult(votes);

        return new PollResult(granted, rejected, result);
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

    public ProgressMap progress() {
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
        return cfg.voters().committedIndex(progress);
    }

    // IsSingleton returns true if (and only if) there is only one voting member
    // (i.e. the leader) in the current configuration.
    public boolean isSingleton() {
        return cfg.voters().isSingleton();
    }
}
