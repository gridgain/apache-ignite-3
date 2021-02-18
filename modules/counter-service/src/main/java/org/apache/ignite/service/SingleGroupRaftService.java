package org.apache.ignite.service;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.command.CustomCommand;
import org.apache.ignite.raft.service.GroupChangeResult;
import org.apache.ignite.raft.service.RaftService;
import org.apache.ignite.raft.service.Result;

import java.util.List;
import java.util.concurrent.Future;

public class SingleGroupRaftService implements RaftService {
    private final RoutingTable routingTbl;

    private final RaftGroupId raftGrpId;

    public SingleGroupRaftService(RoutingTable routingTbl, RaftGroupId raftGrpId) {
        this.routingTbl = routingTbl;
        this.raftGrpId = raftGrpId;
    }

    @Override public State state(boolean refresh) {
        return null;
    }

    @Override public Future<GroupChangeResult> addPeer(PeerId id) {
        return null;
    }

    @Override public Future<GroupChangeResult> removePeer(PeerId id) {
        return null;
    }

    @Override public Future<GroupChangeResult> resetPeers(PeerId peerId, List<PeerId> peers) {
        return null;
    }

    @Override public Future<Result> snapshot(PeerId peerId) {
        return null;
    }

    @Override public Future<GroupChangeResult> changePeers(List<PeerId> peers) {
        return null;
    }

    @Override public Future<GroupChangeResult> addLearners(List<PeerId> learners) {
        return null;
    }

    @Override public Future<GroupChangeResult> removeLearners(List<PeerId> learners) {
        return null;
    }

    @Override public Future<GroupChangeResult> resetLearners(List<PeerId> learners) {
        return null;
    }

    @Override public Future<Result> transferLeader(PeerId newLeader) {
        return null;
    }

    @Override public <T extends CustomCommand, R> Future<R> command(T command) {
        return routingTbl.server(raftGrpId).propose(command);
    }
}
