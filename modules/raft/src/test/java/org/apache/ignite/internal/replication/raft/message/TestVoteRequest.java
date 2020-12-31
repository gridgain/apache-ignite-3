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

import java.util.UUID;

/**
 *
 */
public class TestVoteRequest extends TestBaseMessage implements VoteRequest {
    /** */
    private final long lastIdx;

    /** */
    private final long lastTerm;

    /** */
    private final boolean campaignTransfer;

    public TestVoteRequest(
        UUID from,
        UUID to,
        boolean preVote,
        long term,
        long lastIdx,
        long lastTerm,
        boolean campaignTransfer
    ) {
        super(preVote ? MessageType.MsgPreVote : MessageType.MsgVote, from, to, term);
        this.lastIdx = lastIdx;
        this.lastTerm = lastTerm;
        this.campaignTransfer = campaignTransfer;
    }

    /** {@inheritDoc} */
    @Override public long lastIndex() {
        return lastIdx;
    }

    /** {@inheritDoc} */
    @Override public long lastTerm() {
        return lastTerm;
    }

    /** {@inheritDoc} */
    @Override public boolean campaignTransfer() {
        return campaignTransfer;
    }
}
