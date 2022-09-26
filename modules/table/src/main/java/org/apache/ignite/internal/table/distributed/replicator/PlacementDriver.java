package org.apache.ignite.internal.table.distributed.replicator;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;

public class PlacementDriver {
    private Map<IgniteBiTuple<UUID, Integer>, IgniteBiTuple<String, Set<ClusterNode>>> assignments = new ConcurrentHashMap<>();

    public IgniteBiTuple<String, Set<ClusterNode>> getAssignments(IgniteBiTuple<UUID, Integer> commitPartId) {
        return assignments.get(commitPartId);
    }

    public void putAssignment(IgniteBiTuple<UUID, Integer> commitPartId, IgniteBiTuple<String, Set<ClusterNode>> assignment) {
        assignments.put(commitPartId, assignment);
    }
}
