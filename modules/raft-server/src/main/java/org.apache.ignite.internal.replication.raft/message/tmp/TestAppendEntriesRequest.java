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

import org.apache.ignite.internal.replication.raft.message.AppendEntriesRequest;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.storage.Entry;

import java.util.List;
import java.util.UUID;

/**
 *
 */
public class TestAppendEntriesRequest extends TestBaseMessage implements AppendEntriesRequest {
    /** */
    private final long logIdx;

    /** */
    private final long logTerm;

    /** */
    private final List<Entry> entries;

    /** */
    private final long committedIdx;

    public TestAppendEntriesRequest(
        UUID from,
        UUID to,
        long term,
        long logIdx,
        long logTerm,
        List<Entry> entries,
        long committedIdx
    ) {
        super(MessageType.MsgApp, from, to, term);

        this.logIdx = logIdx;
        this.logTerm = logTerm;
        this.entries = entries;
        this.committedIdx = committedIdx;
    }

    /** {@inheritDoc} */
    @Override public long logIndex() {
        return logIdx;
    }

    /** {@inheritDoc} */
    @Override public long logTerm() {
        return logTerm;
    }

    /** {@inheritDoc} */
    @Override public List<Entry> entries() {
        return entries;
    }

    /** {@inheritDoc} */
    @Override public long committedIndex() {
        return committedIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        if (!super.equals(o))
            return false;

        TestAppendEntriesRequest that = (TestAppendEntriesRequest)o;

        return logIdx == that.logIdx &&
            logTerm == that.logTerm &&
            committedIdx == that.committedIdx &&
            entries.equals(that.entries);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + (int)(logIdx ^ (logIdx >>> 32));
        result = 31 * result + (int)(logTerm ^ (logTerm >>> 32));
        result = 31 * result + entries.hashCode();
        result = 31 * result + (int)(committedIdx ^ (committedIdx >>> 32));

        return result;
    }
}
