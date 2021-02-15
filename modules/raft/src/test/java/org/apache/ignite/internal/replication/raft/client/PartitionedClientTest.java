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

package org.apache.ignite.internal.replication.raft.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.replication.raft.ConfigState;
import org.apache.ignite.internal.replication.raft.HardState;
import org.apache.ignite.internal.replication.raft.RaftConfig;
import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.internal.replication.raft.RawNodeBuilder;
import org.apache.ignite.internal.replication.raft.ReadOnlyOption;
import org.apache.ignite.internal.replication.raft.client.partitioned.PartitionedClient;
import org.apache.ignite.internal.replication.raft.common.NaiveNetworkMock;
import org.apache.ignite.internal.replication.raft.message.TestMessageFactory;
import org.apache.ignite.internal.replication.raft.server.Server;
import org.apache.ignite.internal.replication.raft.server.partitioned.PartitionedServer;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.TestEntryFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionedClientTest {

    @Test
    public void testPartitionedClient() throws Exception {
        RoutingTable routingTbl = new RoutingTable(Map.of(
            new ServerGroupId(ServerGroupId.ServerType.PARTITIONED, 0), preparePartitionedGroup(),
            new ServerGroupId(ServerGroupId.ServerType.PARTITIONED, 1), preparePartitionedGroup()
        ));

        Thread.sleep(2000);

        PartitionedClient partitionedClient = new PartitionedClient(routingTbl);

        partitionedClient.put(1, "A").get();

        Assertions.assertEquals("A", partitionedClient.get(1).get());
    }

    private static List<Server> preparePartitionedGroup() {
        UUID[] ids = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        NaiveNetworkMock netMock = new NaiveNetworkMock();

        List<Server> srvs = Arrays.stream(ids).map(
            id -> preparePartitionedServer(id, netMock, ids)).collect(Collectors.toList());

        netMock.addNodes(srvs.stream().map((Function<Server, RawNode>)Server::rawNode).collect(Collectors.toList()));

        srvs.get(0).rawNode().becomeCandidate();

        srvs.get(0).rawNode().becomeLeader();

        return srvs;
    }

    private static PartitionedServer preparePartitionedServer(UUID nodeId, NaiveNetworkMock netMock, UUID... peerIds) {
        Random rnd = new Random(1L);

        MemoryStorage raftStorage = new MemoryStorage(
            nodeId,
            new HardState(1, null, 0),
            ConfigState.bootstrap(
                Arrays.asList(peerIds),
                Collections.emptyList()),
            Collections.emptyList(),
            new TestEntryFactory());

        return new PartitionedServer(
            raftStorage,
            new RawNodeBuilder()
                .setMessageFactory(new TestMessageFactory())
                .setEntryFactory(new TestEntryFactory())
                .setStorage(raftStorage)
                .setRaftConfig(
                    new RaftConfig()
                        .electionTick(10)
                        .heartbeatTick(1)
                        .readOnlyOption(ReadOnlyOption.READ_ONLY_SAFE)
                )
                .setRandom(rnd),
            netMock);
    }
}
