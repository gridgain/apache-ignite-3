package org.apache.ignite.service;

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
import org.apache.ignite.internal.replication.raft.server.RaftServer;
import org.apache.ignite.internal.replication.raft.server.Server;
import org.apache.ignite.internal.replication.raft.server.tmp.mock.NaiveNetworkMock;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.statemachine.CounterStateMachine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CounterServiceTest {
    @Test
    public void testPartitionedClient() throws Exception {
        RoutingTable routingTbl = new RoutingTable(Map.of(
            new RaftGroupId(RaftGroupId.GroupType.COUNTER, 0), preparePartitionedGroup()
        ));

        Thread.sleep(2000);

        CounterService cntrSvc = new CounterService(routingTbl);

        Assertions.assertEquals(10L, cntrSvc.incrementAndGet(10).get());

        Assertions.assertEquals(10L, cntrSvc.get().get());
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

    private static Server preparePartitionedServer(UUID nodeId, NaiveNetworkMock netMock, UUID... peerIds) {
        Random rnd = new Random(1L);

        MemoryStorage raftStorage = new MemoryStorage(
            nodeId,
            new HardState(1, null, 0),
            ConfigState.bootstrap(
                Arrays.asList(peerIds),
                Collections.emptyList()),
            Collections.emptyList(),
            null);

        return new RaftServer(
            raftStorage,
            new RawNodeBuilder()
//                .setMessageFactory(new TestMessageFactory())
//                .setEntryFactory(new TestEntryFactory())
                .setStorage(raftStorage)
                .setRaftConfig(
                    new RaftConfig()
                        .electionTick(10)
                        .heartbeatTick(1)
                        .readOnlyOption(ReadOnlyOption.READ_ONLY_SAFE)
                )
                .setRandom(rnd),
            new CounterStateMachine(),
            netMock);
    }


}
