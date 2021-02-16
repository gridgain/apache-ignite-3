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

package org.apache.ignite.internal.replication.raft.message;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class TestMessageFactory implements MessageFactory {
    /** {@inheritDoc} */
    @Override public VoteRequest newVoteRequest(
        UUID from,
        UUID to,
        boolean preVote,
        long term,
        long lastIdx,
        long lastTerm,
        boolean campaignTransfer
    ) {
        return new TestVoteRequest(
            from,
            to,
            preVote,
            term,
            lastIdx,
            lastTerm,
            campaignTransfer);
    }

    /** {@inheritDoc} */
    @Override public VoteResponse newVoteResponse(
        UUID from,
        UUID to,
        boolean preVote,
        long term,
        boolean reject
    ) {
        return new TestVoteResponse(
            from,
            to,
            preVote,
            term,
            reject);
    }

    /** {@inheritDoc} */
    @Override public HeartbeatRequest newHeartbeatRequest(
        UUID from,
        UUID to,
        long term,
        long commitIdx,
        IgniteUuid ctx
    ) {
        return new TestHeartbeatRequest(
            from,
            to,
            term,
            commitIdx,
            ctx);
    }

    /** {@inheritDoc} */
    @Override public HeartbeatResponse newHeartbeatResponse(
        UUID from,
        UUID to,
        long term,
        IgniteUuid ctx
    ) {
        return new TestHeartbeatResponse(
            from,
            to,
            term,
            ctx);
    }

    /** {@inheritDoc} */
    @Override public AppendEntriesRequest newAppendEntriesRequest(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        long logTerm,
        List<Entry> entries,
        long committedIdx
    ) {
        return new TestAppendEntriesRequest(
            from,
            to,
            term,
            logIdx,
            logTerm,
            entries,
            committedIdx);
    }

    /** {@inheritDoc} */
    @Override public AppendEntriesResponse newAppendEntriesResponse(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        boolean reject,
        long rejectHint
    ) {
        return new TestAppendEntriesResponse(
            from,
            to,
            term,
            logIdx,
            reject,
            rejectHint);
    }

    /** {@inheritDoc} */
    @Override public InstallSnapshotRequest newInstallSnapshotRequest(
        UUID from,
        UUID to,
        long term,
        Snapshot snapshot
    ) {
        return new TestInstallSnapshotRequest(from, to, term, snapshot);
    }

    /** {@inheritDoc} */
    @Override public TimeoutNowRequest newTimeoutNowRequest(UUID from, UUID to, long term) {
        return new TestTimeoutNowRequest(from, to, term);
    }
}
