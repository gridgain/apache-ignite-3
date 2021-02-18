package org.apache.ignite.service;

import org.apache.ignite.internal.replication.raft.server.Server;

import java.util.List;
import java.util.Map;

public class RoutingTable {
    private final Map<RaftGroupId, List<Server>> srvGroups;

    public RoutingTable(
        Map<RaftGroupId, List<Server>> srvGroups) {

        // TODO sanpwc: Should be immutable.
        this.srvGroups = srvGroups;
    }

    public Server server(RaftGroupId srvId) {
        return srvGroups.get(srvId).stream().filter(Server::isLeader).findFirst().orElseThrow();
    }
}
