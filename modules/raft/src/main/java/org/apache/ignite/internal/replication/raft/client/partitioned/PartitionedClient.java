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

package org.apache.ignite.internal.replication.raft.client.partitioned;

import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.ignite.internal.replication.raft.client.AbstractClient;
import org.apache.ignite.internal.replication.raft.client.RoutingTable;
import org.apache.ignite.internal.replication.raft.client.ServerGroupId;
import org.apache.ignite.internal.replication.raft.common.paritioned.GetPartitionedOperation;
import org.apache.ignite.internal.replication.raft.common.paritioned.PutPartitionedOperation;
import org.apache.ignite.lang.IgniteUuidGenerator;

public class PartitionedClient extends AbstractClient {

    public PartitionedClient(RoutingTable routingTbl) {
        super(routingTbl);
    }

    private final IgniteUuidGenerator igniteUuidGenerator = new IgniteUuidGenerator(UUID.randomUUID(), 1L);

    public Future put(Object key, Object val) {
        return propose(
            new ServerGroupId(ServerGroupId.ServerType.PARTITIONED, key.hashCode() % 2),
            new PutPartitionedOperation(igniteUuidGenerator.randomUuid(), key, val));
    }

    public Future get(Object key) {
        return propose(
            new ServerGroupId(ServerGroupId.ServerType.PARTITIONED, key.hashCode() % 2),
            new GetPartitionedOperation(igniteUuidGenerator.randomUuid(), key));
    }
}
