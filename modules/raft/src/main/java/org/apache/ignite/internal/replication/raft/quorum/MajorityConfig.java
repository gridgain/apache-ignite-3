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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.VoteResult;

import static org.apache.ignite.internal.replication.raft.VoteResult.VoteLost;
import static org.apache.ignite.internal.replication.raft.VoteResult.VotePending;
import static org.apache.ignite.internal.replication.raft.VoteResult.VoteWon;

/**
 *
 */
public class MajorityConfig {
    private final Set<UUID> ids;

    public MajorityConfig(Set<UUID> ids) {
        // Sorted set is requred to make logs stable for data-driven tests.
        this.ids = Collections.unmodifiableSet(new TreeSet<>(ids));
    }

    public Set<UUID> ids() {
        return ids;
    }

    // CommittedIndex computes the committed index from those supplied via the
    // provided AckedIndexer (for the active config).
    long committedIndex(AckedIndexer l) {
        int n = ids.size();

        if (n == 0) {
            // This plays well with joint quorums which, when one half is the zero
            // MajorityConfig, should behave like the other half.
            // TODO agoncharuk: this likely should be removed.
            return Long.MAX_VALUE;
        }

        long[] srt = new long[n];

        // Fill the slice with the indexes observed. Any unused slots will be
        // left as zero; these correspond to voters that may report in, but
        // haven't yet. We fill from the right (since the zeroes will end up on
        // the left after sorting below anyway).
        int i = n - 1;

        for (UUID id : ids) {
            long idx = l.ackedIndex(id);

            if (idx > 0) {
                srt[i] = idx;
                i--;
            }
        }

        // Sort by index.
        Arrays.sort(srt);

        // The smallest index into the array for which the value is acked by a
        // quorum. In other words, from the end of the slice, move n/2+1 to the
        // left (accounting for zero-indexing).
        int pos = n - (n / 2 + 1);
        return srt[pos];
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending (i.e. neither a quorum of
    // yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    // quorum of no has been reached).
    public VoteResult voteResult(Map<UUID, Boolean> votes) {
        if (ids.isEmpty()) {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            // TODO agoncharuk: this will likely be removed.
            return VoteWon;
        }

        int missing = 0, yes = 0, no = 0;

        for (UUID id : ids) {
            Boolean res = votes.get(id);

            if (res == null) {
                missing++;
                continue;
            }

            if (res)
                yes++;
            else
                no++;
        }

        int q = ids.size() / 2 + 1;

        if (yes >= q)
            return VoteWon;
        if (yes + missing >= q)
            return VotePending;

        return VoteLost;
    }
}
