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
public class TestEntryFactory implements EntryFactory {
    /** {@inheritDoc} */
    @Override public Entry newEntry(long term, long idx, LogData data) {
        // We now consider null data as an empty configuration change. This will be fixed after tests are added and
        // we can refactor the code.
        if (data == null || data instanceof ConfChange)
            return new TestEntry(Entry.EntryType.ENTRY_CONF_CHANGE, term, idx, data);
        else if (data instanceof UserData)
            return new TestEntry(Entry.EntryType.ENTRY_DATA, term, idx, data);
        else
            throw new IllegalArgumentException("Unsupported LogData type: " + data);
    }

    /** {@inheritDoc} */
    @Override public long payloadSize(LogData data) {
        return 10;
    }
}
