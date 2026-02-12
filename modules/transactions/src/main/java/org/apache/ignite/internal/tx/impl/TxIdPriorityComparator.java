/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.impl;

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxPriority;

/**
 * Comparator for transaction IDs based on their associated priorities and the IDs themselves. The IDs with higher priorities are sorted
 * first. If the priorities are equal, the IDs are sorted by their natural order.
 */
public class TxIdPriorityComparator implements Comparator<UUID> {
    private static final Comparator<TxPriority> TX_PRIORITY_COMPARATOR = TxPriority::compareTo;

    @Override
    public int compare(UUID o1, UUID o2) {
        TxPriority priority1 = TransactionIds.priority(o1);
        TxPriority priority2 = TransactionIds.priority(o2);

        int res = TX_PRIORITY_COMPARATOR.compare(priority1, priority2);

        if (res == 0) {
            long ts1 = o1.getMostSignificantBits();
            long ts2 = o2.getMostSignificantBits();

            res = Long.compare(ts1, ts2);

            if (res == 0) {
                int rc1 = TransactionIds.retryCnt(o1);
                int rc2 = TransactionIds.retryCnt(o2);

                return Integer.compare(rc1, rc2);
            }

            return res;
        } else {
            return res * -1; // Reverse order.
        }
    }
}
