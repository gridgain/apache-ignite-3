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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class ReadIndexStatus {
    private final long idx;
    private final IgniteUuid ctx;

    // NB: this never records 'false', but it's more convenient to use this
    // instead of a Set<UUID> due to the API of VoteResult. If
    // this becomes performance sensitive enough (doubtful), VoteResult
    // can change to an API that is closer to that of committedIndex.
    private Map<UUID, Boolean> acks = new HashMap<>();

    public ReadIndexStatus(long idx, IgniteUuid ctx) {
        this.idx = idx;
        this.ctx = ctx;
    }

    public Map<UUID, Boolean> acks() {
        return Collections.unmodifiableMap(acks);
    }

    public void recvAck(UUID id) {
        acks.put(id, true);
    }

    /**
     * @return
     */
    public long index() {
        return idx;
    }

    /**
     * @return
     */
    public IgniteUuid context() {
        return ctx;
    }
}
