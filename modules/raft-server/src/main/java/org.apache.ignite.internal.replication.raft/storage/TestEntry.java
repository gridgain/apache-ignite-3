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

package org.apache.ignite.internal.replication.raft.storage;

/**
 *
 */
// TODO sanpwc: tmp - use dummy entry instead.
public class TestEntry implements Entry {
    private final EntryType type;
    private final long term;
    private final long idx;
    private final LogData data;

    public TestEntry(
        EntryType type,
        long term,
        long idx,
        LogData data
    ) {
        this.type = type;
        this.term = term;
        this.idx = idx;
        this.data = data;
    }
    /** {@inheritDoc} */
    @Override public EntryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public long index() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public LogData data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = type.hashCode();

        result = 31 * result + (int)(term ^ (term >>> 32));
        result = 31 * result + (int)(idx ^ (idx >>> 32));
        result = 31 * result + (data != null ? data.hashCode() : 0);

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TestEntry entry = (TestEntry)o;

        if (term != entry.term)
            return false;

        if (idx != entry.idx)
            return false;

        if (type != entry.type)
            return false;

        return data != null ? data.equals(entry.data) : entry.data == null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestEntry[type=" + type + ", idx=" + idx + ", term=" + term + ", data=" + data + ']';
    }
}
