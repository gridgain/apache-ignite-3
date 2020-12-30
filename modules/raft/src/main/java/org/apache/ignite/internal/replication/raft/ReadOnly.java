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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class ReadOnly {
    private ReadOnlyOption option;
    // TODO agoncharuk use LinkedHashMap here instead of map, queue pair
    private Map<IgniteUuid, ReadIndexStatus> pendingReadIndex;
    private List<IgniteUuid> readIndexQueue;

    public ReadOnly(ReadOnlyOption option) {
        this.option = option;

        pendingReadIndex = new HashMap<>();
        readIndexQueue = new ArrayList<>();
    }

    public ReadOnlyOption option() {
        return option;
    }

    // addRequest adds a read only request into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    public void addRequest(long index, IgniteUuid ctx) {
        if (pendingReadIndex.containsKey(ctx))
            return;

        pendingReadIndex.put(ctx, new ReadIndexStatus(index, ctx));

        readIndexQueue.add(ctx);
    }

    // recvAck notifies the readonly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request
    // context.
    public Map<UUID, Boolean> recvAck(UUID id, IgniteUuid ctx) {
        ReadIndexStatus rs = pendingReadIndex.get(ctx);

        if (rs == null)
            return Collections.emptyMap();

        rs.recvAck(id);

        return rs.acks();
    }

    // lastPendingRequestCtx returns the context of the last pending read only
    // request in readonly struct.
    public IgniteUuid lastPendingRequestCtx() {
        if (readIndexQueue.isEmpty())
            return null;

        return readIndexQueue.get(readIndexQueue.size() - 1);
    }

    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given ctx.
    public List<ReadIndexStatus> advance(IgniteUuid ctx) {
        int i = 0;
        boolean found = false;

        List<ReadIndexStatus> rss = new ArrayList<>();

        for (IgniteUuid okctx : readIndexQueue) {
            i++;

            ReadIndexStatus rs = pendingReadIndex.get(okctx);

            if (rs == null)
                throw new AssertionError("cannot find corresponding read state from pending map");

            rss.add(rs);

            if (okctx.equals(ctx)) {
                found = true;

                break;
            }
        }

        if (found) {
            readIndexQueue = readIndexQueue.subList(i, readIndexQueue.size());

            for (ReadIndexStatus rs : rss)
                pendingReadIndex.remove(rs.context());

            return rss;
        }

        return null;
    }
}
