package org.apache.ignite.internal.replication;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;

public interface ReplicationLayerManager {

    void registerStateMachineExtension(int id, RaftGroupListener partitionListener);

    void unregisterStateMachineExtension(int id);

    // KKK should we implement the multi-threaded RaftGroupService, which can handle all needed operations for this node per group?
    CompletableFuture<RaftGroupService> replicationGroupClient(int id);
}
