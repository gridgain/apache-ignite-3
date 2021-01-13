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

import java.util.List;
import org.apache.ignite.internal.replication.raft.storage.Entry;

public class Utils {

    // TODO: sanpwc Is it valid to use static for such methods according to new ignite-3.0 guidelines?
    public static List<Entry> limitSize(List<Entry> entries, long maxSize) {
        if (entries.size() == 0)
            return entries;

        long totalSize = 0;

        int limitIdx = 0;

        for (Entry entry: entries) {
            totalSize += entry.size();

            if (totalSize > maxSize)
                break;

            limitIdx++;
        }

        return entries.subList(0, limitIdx);
    }
}
