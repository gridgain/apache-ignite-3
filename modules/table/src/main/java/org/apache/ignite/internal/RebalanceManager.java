package org.apache.ignite.internal;

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.client.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.client.Conditions.exists;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPendingKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPlannedKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsStableKey;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.jetbrains.annotations.NotNull;

public class RebalanceManager {

    public String pendingPartitionKey(String tableName, Integer partition) {
        return tableName + "." + partition + ".assignments.pending";
    }

    public String plannedPartitionKey(String tableName, Integer partition) {
        return tableName + "." + partition + ".assignments.planned";
    }

    public static CompletableFuture<Long> registerListener(String tableName, int partNumber, ByteArray pendingAssignments, ExtendedTableConfiguration tablesCfg, String groupId, MetaStorageManager mgr, Loza loza, Supplier<RaftGroupListener> raftGrpLsnr) {
        System.out.println("LISTEN KEY " + pendingAssignments);
        return mgr.registerWatch(pendingAssignments, new WatchListener() {
            @Override
            public boolean onUpdate(@NotNull WatchEvent evt) {
                List<ClusterNode> newPeers = ((List<ClusterNode>) ByteUtils.fromBytes(evt.entryEvent().newEntry().value()));
                List<ClusterNode> currentPeers = ((List<List<ClusterNode>>) ByteUtils.fromBytes(tablesCfg.assignments().value())).get(partNumber);
                var raftGrpSvc = loza.startRaftGroupIfNeeded(groupId, newPeers, currentPeers, raftGrpLsnr).join();

                raftGrpSvc.refreshLeader().join();
                System.out.println("Current peer " +new Peer(raftGrpSvc.clusterService().topologyService().localMember().address()) + "leader is "  + raftGrpSvc.leader());
                if (new Peer(raftGrpSvc.clusterService().topologyService().localMember().address()).equals(raftGrpSvc.leader())) {
                    var result = raftGrpSvc.changePeersAsync(fromClusterNodes(newPeers)).join();
                    System.out.println("[R] result " + result);
                    switch (result) {
                        case BUSY:
                            throw new IllegalStateException("Can't reach this point");
                        case DONE:
                            // onNewPeersConfigurationApplied should be executed
                            break;
                        case FAILED:
                            return onUpdate(evt);
                        case RECEIVED:
                        case WRONG_TERM:
                            // it's ok, no actions needed
                            break;
                    }

                }
                raftGrpSvc.shutdown();
                return false;
            }

            @Override
            public void onError(@NotNull Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static List<ClusterNode> checkPendingRebalance(String tableName, int partitionId) {
        return null;
    }

    public static RaftGroupEventsListener raftGroupEventsListener(String tableName, int partitionId, TableConfiguration view,
            IgniteLogger logger, MetaStorageManager mgr, String partId, TopologyService topologyService) {
        return new RaftGroupEventsListener() {
            @Override
            public void onLeaderElected() {
//                List<ClusterNode> pendingNodes = checkPendingRebalance(tableName, partitionId);
//                RebalanceWorker.getInstance().putRebalanceRequest(pendingNodes);

            }

            @Override
            public void onNewPeersConfigurationApplied(List<PeerId> peers) {
                Entry entry = mgr.get(partAssignmentsPlannedKey(partId)).join();
                System.out.println("KEY " + partAssignmentsPendingKey(partId).toString());
                if (entry.value() != null) {
                    if (!mgr.invoke(If.iif(
                            revision(partAssignmentsPlannedKey(partId)).eq(entry.revision()),
                            ops(
                                    put(partAssignmentsStableKey(partId), ByteUtils.toBytes(peers)),
                                    put(partAssignmentsPendingKey(partId), entry.value()),
                                    remove(partAssignmentsPlannedKey(partId)))
                                    .yield(true),
                            ops().yield(false))).join().getAsBoolean()) {
                        onNewPeersConfigurationApplied(peers);
                        return;
                    }
                } else {
                    if (!mgr.invoke(If.iif(
                            revision(partAssignmentsPlannedKey(partId)).eq(entry.revision()),
                            ops(put(partAssignmentsStableKey(partId), ByteUtils.toBytes(peers)), remove(partAssignmentsPendingKey(partId))).yield(true),
                            ops().yield(false))).join().getAsBoolean()) {
                        onNewPeersConfigurationApplied(peers);
                        return;
                    }
                }

                view.change(ch -> {
                    List<List<ClusterNode>> assignments = (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
                    var clusterNodes = topologyService.allMembers().stream().filter(m -> peers.stream().anyMatch(p -> new NetworkAddress(p.getEndpoint().getIp(), p.getEndpoint().getPort()).equals(m.address()))).collect(
                            Collectors.toList());
                    assignments.set(partitionId, clusterNodes);
                    ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
                });

            }

            @Override
            public void onReconfigurationError(Status status) {
//                logger.error("Can't run reconfiguration " + status);
//                RebalanceWorker.getInstance().putRebalanceRequest(checkPendingRebalance(tableName, partitionId));
            }
        };
    }

    private static void registerListener(ExtendedTableConfiguration cfg) {
        cfg.assignments().listen(new ConfigurationListener<byte[]>() {
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<byte[]> ctx) {
                return null;
            }
        });

    }

    private static List<ClusterNode> fromByteArray(byte[] bytes) {
        return null;
    }

    private static List<Peer> fromClusterNodes(List<ClusterNode> nodes) {
        return nodes.stream().map(n -> new Peer(n.address())).collect(Collectors.toList());
    }


    public static class RebalanceWorker {

        private final RaftGroupService raftGroupService = null;

        private BlockingQueue<List<ClusterNode>> rebalanceTasks;

        public static RebalanceWorker getInstance() {
            return null;
        }

        public void putRebalanceRequest(List<ClusterNode> peers) {

            if (!peers.isEmpty()) {
                rebalanceTasks.add(peers);
            }

        }

        public void process() {
            while (true) {
                List<ClusterNode> task = rebalanceTasks.poll();


                var result = raftGroupService.changePeersAsync(fromClusterNodes(task)).join();

                switch (result) {
                    case BUSY:
                        throw new IllegalStateException("Can't reach this point");
                    case DONE:
                        // onNewPeersConfigurationApplied should be executed
                        break;
                    case FAILED:
                        throw new IllegalStateException("FAILED");
                    case RECEIVED:
                    case WRONG_TERM:
                        // it's ok, no actions needed
                        break;
                }
            }

        }
    }
}
