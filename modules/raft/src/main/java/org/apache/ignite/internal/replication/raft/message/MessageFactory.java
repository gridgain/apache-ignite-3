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
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public interface MessageFactory {
    public VoteRequest newVoteRequest(
        UUID from,
        UUID to,
        boolean preVote,
        long term,
        long lastIdx,
        long lastTerm,
        boolean campaignTransfer
    );

    public VoteResponse newVoteResponse(
        UUID from,
        UUID to,
        boolean preVote,
        long term,
        boolean reject
    );

    AppendEntriesRequest newAppendEntriesRequest(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        long logTerm,
        List<Entry> entries,
        long committedIdx
    );

    AppendEntriesResponse newAppendEntriesResponse(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        boolean reject,
        long rejectHint
    );

    HeartbeatRequest newHeartbeatRequest(
        UUID from,
        UUID to,
        long term,
        long commitIdx,
        IgniteUuid ctx
    );

    HeartbeatResponse newHeartbeatResponse(
        UUID from,
        UUID to,
        long term,
        IgniteUuid ctx
    );

    InstallSnapshotRequest newInstallSnapshotRequest(
        UUID from,
        UUID to,
        long term,
        long snapIdx,
        long snapTerm,
        Snapshot snapshot);

    TimeoutNowRequest newTimeoutNowRequest(
        UUID from,
        UUID to,
        long term
    );
}
