package org.apache.ignite.raft.client.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.RaftClientCommonMessages;
import org.apache.ignite.raft.client.RaftGroupRpcClient;
import org.apache.ignite.raft.client.message.RaftClientCommonMessageBuilderFactory;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.Node;
import org.apache.ignite.raft.rpc.RaftGroupMessage;
import org.apache.ignite.raft.rpc.RpcClient;

public class RaftGroupRpcClientImpl implements RaftGroupRpcClient {
    private final ExecutorService executor;
    private final int defaultTimeout;
    private final RpcClient rpcClient;

    /** Where to ask for initial configuration. */
    private final Set<Node> initialCfgNodes;

    private Map<String, Future<PeerId>> leaders = new ConcurrentHashMap<>();

    /**
     * Accepts dependencies in constructor.
     * @param rpcClient
     * @param defaultTimeout
     * @param initialCfgNode Initial configuration nodes.
     */
    public RaftGroupRpcClientImpl(RpcClient rpcClient, int defaultTimeout, Set<Node> initialCfgNodes) {
        this.defaultTimeout = defaultTimeout;
        this.rpcClient = rpcClient;
        this.initialCfgNodes = new HashSet<>(initialCfgNodes);
        executor = Executors.newWorkStealingPool();
    }

    @Override public State state(String groupId, boolean refresh) {
        try {
            return new StateImpl(leaders.get(groupId).get(), Collections.emptyList(), Collections.emptyList());
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override public Future<RaftClientCommonMessages.AddPeerResponse> addPeer(RaftClientCommonMessages.AddPeerRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.RemovePeerResponse> removePeer(RaftClientCommonMessages.RemovePeerRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.StatusResponse> resetPeers(PeerId peerId, RaftClientCommonMessages.ResetPeerRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.StatusResponse> snapshot(PeerId peerId, RaftClientCommonMessages.SnapshotRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.ChangePeersResponse> changePeers(RaftClientCommonMessages.ChangePeersRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.LearnersOpResponse> addLearners(RaftClientCommonMessages.AddLearnersRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.LearnersOpResponse> removeLearners(RaftClientCommonMessages.RemoveLearnersRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.LearnersOpResponse> resetLearners(RaftClientCommonMessages.ResetLearnersRequest request) {
        return null;
    }

    @Override public Future<RaftClientCommonMessages.StatusResponse> transferLeader(RaftClientCommonMessages.TransferLeaderRequest request) {
        return null;
    }

    private CompletableFuture<RaftClientCommonMessages.GetLeaderResponse> refreshLeader(Node node, String groupId) {
        RaftClientCommonMessages.GetLeaderRequest req = RaftClientCommonMessageBuilderFactory.DEFAULT.createGetLeaderRequest().setGroupId(groupId).build();

        CompletableFuture<RaftClientCommonMessages.GetLeaderResponse> fut = new CompletableFuture<>();

        rpcClient.invokeAsync(node, req, new InvokeCallback<RaftClientCommonMessages.GetLeaderResponse>() {
            @Override public void complete(RaftClientCommonMessages.GetLeaderResponse response, Throwable err) {
                if (err != null)
                    fut.completeExceptionally(err);
                else
                    fut.complete(response);
            }
        }, executor, defaultTimeout);

        return fut;
    }

    @Override public <R extends Message> Future<R> sendCustom(RaftGroupMessage request) {
        Future<PeerId> leaderFut = leaders.computeIfAbsent(request.getGroupId(), gId -> {
            return refreshLeader(initialCfgNodes.iterator().next(), gId).orTimeout(
                defaultTimeout,
                TimeUnit.MILLISECONDS
            ).thenApply(r -> r.getLeaderId()); // TODO asch search all nodes.
        });

        CompletableFuture<R> res = new CompletableFuture<>();
        res.orTimeout(defaultTimeout, TimeUnit.MILLISECONDS);

        try {
            PeerIdImpl peerId = (PeerIdImpl)leaderFut.get();

            rpcClient.invokeAsync(peerId.getNode(), request, new InvokeCallback<R>() {
                @Override public void complete(R response, Throwable err) {
                    if (err != null)
                        res.completeExceptionally(err);
                    else
                        res.complete(response);
                }
            }, executor, defaultTimeout);
        }
        catch (InterruptedException | ExecutionException e) {
            res.completeExceptionally(e);
        }

        return res;
    }
}
