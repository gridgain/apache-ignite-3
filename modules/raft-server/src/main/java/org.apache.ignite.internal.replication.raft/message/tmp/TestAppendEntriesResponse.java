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

package org.apache.ignite.internal.replication.raft.message.tmp;

import org.apache.ignite.internal.replication.raft.message.AppendEntriesResponse;
import org.apache.ignite.internal.replication.raft.message.MessageType;

import java.util.UUID;

/**
 *
 */
public class TestAppendEntriesResponse extends TestBaseMessage implements AppendEntriesResponse {
    /** */
    private final long logIdx;

    /** */
    private final boolean reject;

    /** */
    private final long rejectHint;

    public TestAppendEntriesResponse(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        boolean reject,
        long rejectHint
    ) {
        super(MessageType.MsgAppResp, from, to, term);

        this.logIdx = logIdx;
        this.reject = reject;
        this.rejectHint = rejectHint;
    }

    /** {@inheritDoc} */
    @Override public long logIndex() {
        return logIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean reject() {
        return reject;
    }

    /** {@inheritDoc} */
    @Override public long rejectHint() {
        return rejectHint;
    }
}
