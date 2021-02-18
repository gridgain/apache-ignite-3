package org.apache.ignite.internal.replication.raft.server.tmp.mock;

import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.internal.replication.raft.message.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

// TODO sanpwc: Non thread safe! Better implementation required.
public class NaiveNetworkMock {

    private Map<UUID, RawNode> nodes = new HashMap<>();

    public NaiveNetworkMock() {
    }

    public void processMessage(Message msg) {
        nodes.get(msg.to()).step(msg);
    }

    public void addNodes(List<RawNode> nodes) {
        this.nodes = nodes.stream().collect(Collectors.toMap(e -> e.basicStatus().id(), e -> e));
    }
}