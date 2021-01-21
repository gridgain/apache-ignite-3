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

import java.util.Arrays;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.EntryFactory;
import org.apache.ignite.internal.replication.raft.storage.TestEntryFactory;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class UtilsTest {
    /** */
    private EntryFactory entryFactory = new TestEntryFactory();

    @Test
    public void testLimitSize() {
        Entry[] ents = new Entry[] {
            entryFactory.newEntry(4, 4, new UserData<>("somedata")),
            entryFactory.newEntry(5, 5, new UserData<>("somedata")),
            entryFactory.newEntry(6, 6, new UserData<>("somedata"))
        };

        for (int i = 1; i < ents.length; i++)
            Assertions.assertEquals(entryFactory.payloadSize(ents[0]), entryFactory.payloadSize(ents[1]));

        class TestData {
            long maxSize;
            Entry[] wentries;

            TestData(long maxSize, Entry[] wentries) {
                this.maxSize = maxSize;
                this.wentries = wentries;
            }
        }

        long size = entryFactory.payloadSize(ents[0]);

        TestData[] tests = new TestData[] {
            new TestData(Long.MAX_VALUE, ents),
            // even if maxsize is zero, the first entry should be returned
            new TestData(0, new Entry[] {ents[0]}),
            // limit to 2
            new TestData(2 * size, new Entry[] {ents[0], ents[1]}),
            // limit to 2
            new TestData(2 * size + size / 2, new Entry[] {ents[0], ents[1]}),
            new TestData(3 * size - 1, new Entry[] {ents[0], ents[1]}),
            // all
            new TestData(3 * size, ents),
        };

        for (TestData tt : tests)
            Assertions.assertEquals(Arrays.asList(tt.wentries),
                Utils.limitSize(entryFactory, Arrays.asList(ents), tt.maxSize));
    }
}
