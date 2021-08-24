package org.apache.ignite.internal.raft;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.rpc.impl.client.RaftErrorCode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.jraft.rpc.impl.client.RaftException;
import org.apache.ignite.raft.jraft.rpc.impl.client.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.client.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.impl.client.RaftErrorResponse;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.jetbrains.annotations.NotNull;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.raft.jraft.rpc.impl.client.RaftErrorCode.NO_LEADER;

public class NewRaftGroupServiceImpl implements RaftGroupService {
    private static final IgniteLogger LOG = IgniteLogger.forClass(NewRaftGroupServiceImpl.class);

    /** */
    private volatile long timeout;

    /** */
    private final String groupId;

    /** */
    private volatile Peer leader;

    /** */
    private volatile List<Peer> peers;

    private RaftMessagesFactory raftMessagesFactory;

    /** */
    private volatile List<Peer> learners;

    /** */
    private final ClusterService cluster;

    /** */
    private final long retryDelay;

    private final CliClientServiceImpl clientService;

    /** */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    /**
     * Constructor.
     *
     * @param groupId Group id.
     * @param cluster A cluster.
     * @param timeout Request timeout.
     * @param peers Initial group configuration.
     * @param leader Group leader.
     * @param retryDelay Retry delay.
     */
    private NewRaftGroupServiceImpl(
        String groupId,
        ClusterService cluster,
        RaftMessagesFactory raftMessagesFactory,
        int timeout,
        List<Peer> peers,
        Peer leader,
        long retryDelay
    ) {
        this.cluster = requireNonNull(cluster);
        this.raftMessagesFactory = requireNonNull(raftMessagesFactory);
        this.peers = requireNonNull(peers);
        this.groupId = groupId;
        this.retryDelay = retryDelay;
        this.leader = leader;

        var rpcOptions = new CliOptions();
        rpcOptions.setClientExecutor(executor);
        rpcOptions.setRpcClient(new IgniteRpcClient(cluster));
        rpcOptions.setRaftMessagesFactory(raftMessagesFactory);
        rpcOptions.setTimeoutMs(timeout);

        clientService = new CliClientServiceImpl();
        clientService.init(rpcOptions);
    }

    /**
     * Starts raft group service.
     *
     * @param groupId Raft group id.
     * @param cluster Cluster service.
     * @param factory Message factory.
     * @param timeout Timeout.
     * @param peers List of all peers.
     * @param getLeader {@code True} to get the group's leader upon service creation.
     * @param retryDelay Retry delay.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<RaftGroupService> start(
        String groupId,
        ClusterService cluster,
        RaftMessagesFactory raftMessagesFactory,
        int timeout,
        List<Peer> peers,
        boolean getLeader,
        long retryDelay
    ) {
        var service = new NewRaftGroupServiceImpl(groupId, cluster, raftMessagesFactory, timeout, peers, null, retryDelay);

        if (!getLeader) {
            return CompletableFuture.completedFuture(service);
        }

        return service.refreshLeader().handle((unused, throwable) -> {
            if (throwable != null)
                LOG.error("Failed to refresh a leader", throwable);

            return service;
        });
    }

    /** {@inheritDoc} */
    @Override public @NotNull String groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long newTimeout) {
        this.timeout = newTimeout;
    }

    /** {@inheritDoc} */
    @Override public Peer leader() {
        return leader;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> peers() {
        return peers;
    }

    /** {@inheritDoc} */
    @Override public List<Peer> learners() {
        return learners;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshLeader() {
        var fut = new CompletableFuture<CliRequests.GetLeaderResponse>();
        CliRequests.GetLeaderRequest req = raftMessagesFactory.getLeaderRequest().groupId(groupId).build();

        var peer = randomNode();
        clientService.getLeader(new Endpoint(peer.address().host(), peer.address().port()), req, new RpcResponseClosure<CliRequests.GetLeaderResponse>() {
            @Override public void setResponse(CliRequests.GetLeaderResponse resp) {
                fut.complete(resp);
            }

            @Override public void run(Status status) {
                if (!status.isOk())
                    fut.completeExceptionally(new IgniteInternalException(status.getCode() + " " + status.getErrorMsg()));
            }
        });

        return fut.thenApply(resp -> {
            PeerId p = new PeerId();
            p.parse(resp.leaderId());
            leader = new Peer(NetworkAddress.from(p.getEndpoint().toString()));
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> refreshMembers(boolean onlyAlive) {
        CliRequests.GetPeersRequest req = raftMessagesFactory.getPeersRequest().onlyAlive(onlyAlive).groupId(groupId).build();

        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> refreshMembers(onlyAlive));

        CompletableFuture<CliRequests.GetPeersResponse> fut = new CompletableFuture<>();

        clientService.getPeers(new Endpoint(leader.address().host(), leader.address().port()), req, new RpcResponseClosure<CliRequests.GetPeersResponse>() {
            @Override public void setResponse(CliRequests.GetPeersResponse resp) {
                fut.complete(resp);
            }

            @Override public void run(Status status) {
                if (!status.isOk())
                    fut.completeExceptionally(new RaftException(RaftErrorCode.from(status.getCode()), status.getErrorMsg()));
            }
        });

        return fut.thenApply(resp -> {
            peers = resp.peersList().stream().map(s -> new Peer(NetworkAddress.from(s))).collect(Collectors.toList());
            learners = resp.learnersList().stream().map(s -> new Peer(NetworkAddress.from(s))).collect(Collectors.toList());

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addPeers(List<Peer> peers) {
        throw new UnsupportedOperationException("addPeers is not implemented yet");
//        Peer leader = this.leader;
//
//        if (leader == null)
//            return refreshLeader().thenCompose(res -> addPeers(peers));
//
//        CliRequests.AddPeerRequest req = cliOptions.getRaftClientMessagesFactory().addPeersRequest().groupId(groupId).peers(peers).build();
//
//        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();
//
//        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);
//
//        return fut.thenApply(resp -> {
//            this.peers = resp.newPeers();
//
//            return null;
//        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removePeers(List<Peer> peers) {
        throw new UnsupportedOperationException("removePeers is not implemented yet");
//        Peer leader = this.leader;
//
//        if (leader == null)
//            return refreshLeader().thenCompose(res -> removePeers(peers));
//
//        RemovePeersRequest req = cliOptions.getRaftClientMessagesFactory().removePeersRequest().groupId(groupId).peers(peers).build();
//
//        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();
//
//        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);
//
//        return fut.thenApply(resp -> {
//            this.peers = resp.newPeers();
//
//            return null;
//        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> addLearners(List<Peer> learners) {
        throw new UnsupportedOperationException("addLearners is not implemented yet");
//        Peer leader = this.leader;
//
//        if (leader == null)
//            return refreshLeader().thenCompose(res -> addLearners(learners));
//
//        AddLearnersRequest req = cliOptions.getRaftClientMessagesFactory().addLearnersRequest().groupId(groupId).learners(learners).build();
//
//        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();
//
//        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);
//
//        return fut.thenApply(resp -> {
//            this.learners = resp.newPeers();
//
//            return null;
//        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> removeLearners(List<Peer> learners) {
        throw new UnsupportedOperationException("removeLearners is not implemented yet");
//        Peer leader = this.leader;
//
//        if (leader == null)
//            return refreshLeader().thenCompose(res -> removeLearners(learners));
//
//        RemoveLearnersRequest req = cliOptions.getRaftClientMessagesFactory().removeLearnersRequest().groupId(groupId).learners(learners).build();
//
//        CompletableFuture<ChangePeersResponse> fut = new CompletableFuture<>();
//
//        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);
//
//        return fut.thenApply(resp -> {
//            this.learners = resp.newPeers();
//
//            return null;
//        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Peer peer) {
        CliRequests.SnapshotRequest req = raftMessagesFactory.snapshotRequest().groupId(groupId).build();

        // Disable the timeout for a snapshot request.
        CompletableFuture<NetworkMessage> fut = cluster.messagingService().invoke(peer.address(), req, Integer.MAX_VALUE);

        return fut.thenApply(resp -> {
            if (resp != null) {
                RaftErrorResponse resp0 = (RaftErrorResponse) resp;

                if (resp0.errorCode() != null)
                    throw new RaftException(resp0.errorCode(), resp0.errorMessage());
            }

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> transferLeadership(Peer newLeader) {
        PeerId leader = PeerId.fromPeer(this.leader);

        if (leader == null)
            return refreshLeader().thenCompose(res -> transferLeadership(newLeader));

        CliRequests.TransferLeaderRequest req = raftMessagesFactory.transferLeaderRequest().groupId(groupId).leaderId(PeerId.fromPeer(newLeader).toString()).build();

        CompletableFuture<?> fut = cluster.messagingService().invoke(newLeader.address(), req, timeout);

        return fut.thenApply(resp -> null);
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> run(Command cmd) {
        Peer leader = this.leader;

        if (leader == null)
            return refreshLeader().thenCompose(res -> run(cmd));

        ActionRequest req = raftMessagesFactory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(true).build();

        CompletableFuture<ActionResponse> fut = new CompletableFuture<>();

//        clientService.getPeers(new Endpoint(leader.address().host(), leader.address().port()), req, new RpcResponseClosure<ActionResponse>() {
//            @Override public void setResponse(ActionResponse resp) {
//                fut.complete(resp);
//            }
//
//            @Override public void run(Status status) {
//                if (!status.isOk())
//                    fut.completeExceptionally(new RaftException(RaftErrorCode.from(status.getCode()), status.getErrorMsg()));
//            }
//        });

        sendWithRetry(leader, req, currentTimeMillis() + timeout, fut);

        return fut.thenApply(resp -> (R) resp.result());
    }

    /**
     * {@inheritDoc}
     */
    @Override public <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd) {
        ActionRequest req = raftMessagesFactory.actionRequest().command(cmd).groupId(groupId).readOnlySafe(false).build();

        CompletableFuture<ActionResponse> fut = new CompletableFuture<>();

//        clientService.invokeWithDone(new Endpoint(peer.address().host(), peer.address().port()), req, new RpcResponseClosure<ActionResponse>() {
//            @Override public void setResponse(ActionResponse resp) {
//                fut.complete(resp);
//            }
//
//            @Override public void run(Status status) {
//                if (!status.isOk())
//                    fut.completeExceptionally(new RaftException(RaftErrorCode.from(status.getCode()), status.getErrorMsg()));
//            }
//        });

        return fut.thenApply(resp -> (R) ((ActionResponse) resp).result());
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        // No-op.
    }


    /**
     * Retries a request until success or timeout.
     *
     * @param peer Target peer.
     * @param req The request.
     * @param stopTime Stop time.
     * @param fut The future.
     * @param <R> Response type.
     */
    private <R> void sendWithRetry(Peer peer, Object req, long stopTime, CompletableFuture<R> fut) {
        if (currentTimeMillis() >= stopTime) {
            fut.completeExceptionally(new TimeoutException());

            return;
        }

        CompletableFuture<?> fut0 = cluster.messagingService().invoke(peer.address(), (NetworkMessage) req, timeout);

        fut0.whenComplete(new BiConsumer<Object, Throwable>() {
            @Override public void accept(Object resp, Throwable err) {
                if (err != null) {
                    if (recoverable(err)) {
                        executor.schedule(() -> {
                            sendWithRetry(randomNode(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
                    else
                        fut.completeExceptionally(err);
                }
                else if (resp instanceof RpcRequests.ErrorResponse) {
                    RpcRequests.ErrorResponse resp0 = (RpcRequests.ErrorResponse) resp;

                    if (resp0.errorCode() == 0) { // Handle OK response.
                        leader = peer; // The OK response was received from a leader.

                        fut.complete(null); // Void response.
                    }
                    else if (resp0.errorCode() == NO_LEADER.code()) {
                        executor.schedule(() -> {
                            sendWithRetry(randomNode(), req, stopTime, fut);

                            return null;
                        }, retryDelay, TimeUnit.MILLISECONDS);
                    }
//                    else if (resp0.errorCode() == LEADER_CHANGED.code()) {
//                        leader = resp0.newLeader(); // Update a leader.
//
//                        executor.schedule(() -> {
//                            sendWithRetry(resp0.newLeader(), req, stopTime, fut);
//
//                            return null;
//                        }, retryDelay, TimeUnit.MILLISECONDS);
//                    }
                    else
                        fut.completeExceptionally(new IgniteInternalException(resp0.errorCode() + resp0.errorMsg()));
                }
                else {
                    leader = peer; // The OK response was received from a leader.

                    fut.complete((R) resp);
                }
            }
        });
    }

    /**
     * Checks if an error is recoverable, for example, {@link java.net.ConnectException}.
     * @param t The throwable.
     * @return {@code True} if this is a recoverable exception.
     */
    private boolean recoverable(Throwable t) {
        return t.getCause() instanceof IOException;
    }

    /**
     * @return Random node.
     */
    private Peer randomNode() {
        List<Peer> peers0 = peers;

        if (peers0 == null || peers0.isEmpty())
            return null;

        return peers0.get(current().nextInt(peers0.size()));
    }
}
