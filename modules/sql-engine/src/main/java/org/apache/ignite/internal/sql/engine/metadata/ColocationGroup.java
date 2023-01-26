/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.metadata;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteIntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * ColocationGroup.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ColocationGroup implements Serializable {
    private final List<Long> sourceIds;

    private final List<String> nodeNames;

    private final List<List<NodeWithTerm>> assignments;

    /**
     * ForNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forNodes(List<String> nodeNames) {
        return new ColocationGroup(null, nodeNames, null);
    }

    /**
     * ForAssignments.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forAssignments(List<List<NodeWithTerm>> assignments) {
        return new ColocationGroup(null, null, assignments);
    }

    /**
     * ForSourceId.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forSourceId(long sourceId) {
        return new ColocationGroup(Collections.singletonList(sourceId), null, null);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private ColocationGroup(List<Long> sourceIds, List<String> nodeNames, List<List<NodeWithTerm>> assignments) {
        this.sourceIds = sourceIds;
        this.nodeNames = nodeNames;
        this.assignments = assignments;
    }

    /**
     * Get lists of colocation group sources.
     */
    public List<Long> sourceIds() {
        return sourceIds == null ? Collections.emptyList() : sourceIds;
    }

    /**
     * Get lists of nodes capable to execute a query fragment for what the mapping is calculated.
     */
    public List<String> nodeNames() {
        return nodeNames == null ? Collections.emptyList() : nodeNames;
    }

    /**
     * Get list of partitions (index) and nodes (items) having an appropriate partition in OWNING state, calculated for
     * distributed tables, involved in query execution.
     */
    public List<List<NodeWithTerm>> assignments() {
        return assignments == null ? Collections.emptyList() : assignments;
    }

    /**
     * Prunes involved partitions (hence nodes, involved in query execution) on the basis of filter, its distribution,
     * query parameters and original nodes mapping.
     *
     * @param rel Filter.
     * @return Resulting nodes mapping.
     */
    public ColocationGroup prune(IgniteRel rel) {
        return this; // TODO https://issues.apache.org/jira/browse/IGNITE-12455
    }

    /**
     * Belongs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public boolean belongs(long sourceId) {
        return sourceIds != null && sourceIds.contains(sourceId);
    }

    /**
     * Merges this mapping with given one.
     *
     * @param other Mapping to merge with.
     * @return Merged nodes mapping.
     * @throws ColocationMappingException If involved nodes intersection is empty, hence there is no nodes capable to
     *     execute being calculated fragment.
     */
    public ColocationGroup colocate(ColocationGroup other) throws ColocationMappingException {
        List<Long> sourceIds;
        if (this.sourceIds == null || other.sourceIds == null) {
            sourceIds = firstNotNull(this.sourceIds, other.sourceIds);
        } else {
            sourceIds = Commons.combine(this.sourceIds, other.sourceIds);
        }

        List<String> nodeNames;
        if (this.nodeNames == null || other.nodeNames == null) {
            nodeNames = firstNotNull(this.nodeNames, other.nodeNames);
        } else {
            nodeNames = Commons.intersect(other.nodeNames, this.nodeNames);
        }

        if (nodeNames != null && nodeNames.isEmpty()) {
            throw new ColocationMappingException("Failed to map fragment to location. "
                    + "Replicated query parts are not co-located on all nodes");
        }

        List<List<NodeWithTerm>> assignments;
        if (this.assignments == null || other.assignments == null) {
            assignments = firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeNames != null) {
                List<List<NodeWithTerm>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<NodeWithTerm> assignment = filterByNodeNames(assignments.get(i), new HashSet<>(nodeNames));

                    if (assignment.isEmpty()) { // TODO check with partition filters
                        throw new ColocationMappingException("Failed to map fragment to location. "
                                + "Partition mapping is empty [part=" + i + "]");
                    }

                    assignments0.add(assignment);
                }

                assignments = assignments0;
            }
        } else {
            assert this.assignments.size() == other.assignments.size();
            assignments = new ArrayList<>(this.assignments.size());
            Set<String> filter = nodeNames == null ? null : new HashSet<>(nodeNames);
            for (int p = 0; p < this.assignments.size(); p++) {
                List<NodeWithTerm> assignment = intersect(this.assignments.get(p), other.assignments.get(p), filter, p);

                if (assignment.isEmpty()) { // TODO check with partition filters
                    throw new ColocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + p + "]");
                }

                assignments.add(assignment);
            }
        }

        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    private List<NodeWithTerm> intersect(List<NodeWithTerm> assign0, List<NodeWithTerm> assign1, @Nullable Set<String> filter, int partId)
            throws ColocationMappingException {
        List<NodeWithTerm> intersection = new ArrayList<>();

        for (NodeWithTerm nodeWithTerm : assign0) {
            if (filter != null && !filter.contains(nodeWithTerm.name())) {
                continue;
            }

            for (NodeWithTerm otherNodeWithTerm : assign1) {
                if (!otherNodeWithTerm.name().equals(nodeWithTerm.name())) {
                    continue;
                }

                if (nodeWithTerm.term() != otherNodeWithTerm.term()) {
                    throw new ColocationMappingException("Raft group primary replica term has been changed during mapping ["
                            + "part=" + partId
                            + ", leader=" + nodeWithTerm.name()
                            + ", expectedTerm=" + nodeWithTerm.term()
                            + ", actualTerm=" + otherNodeWithTerm.term() + ']');
                }

                intersection.add(otherNodeWithTerm);
            }
        }

        return intersection;
    }

    private List<NodeWithTerm> filterByNodeNames(List<NodeWithTerm> assignment, Set<String> filter) {
        if (nullOrEmpty(assignment) || nullOrEmpty(filter)) {
            return Collections.emptyList();
        }

        List<NodeWithTerm> res = new ArrayList<>();

        for (NodeWithTerm nodeWithTerm : assignment) {
            if (!filter.contains(nodeWithTerm.name())) {
                continue;
            }

            res.add(nodeWithTerm);
        }

        return res;
    }

    /**
     * Creates a new colocation group using only primary assignments.
     *
     * @return Colocation group with primary assignments.
     */
    public ColocationGroup finalaze() {
        if (assignments != null) {
            List<List<NodeWithTerm>> assignments = new ArrayList<>(this.assignments.size());
            Set<String> nodes = new HashSet<>();
            for (List<NodeWithTerm> assignment : this.assignments) {
                NodeWithTerm first = first(assignment);
                if (first != null) {
                    nodes.add(first.name());
                }
                assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return mapToNodes(nodeNames);
    }

    /**
     * MapToNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ColocationGroup mapToNodes(List<String> nodeNames) {
        return !nullOrEmpty(this.nodeNames) ? this : forNodes0(nodeNames);
    }

    @NotNull
    private ColocationGroup forNodes0(List<String> nodeNames) {
        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param nodeName Cluster node consistent ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(String nodeName) {
        IgniteIntList parts = new IgniteIntList(assignments.size());

        for (int p = 0; p < assignments.size(); p++) {
            List<NodeWithTerm> assignment = assignments.get(p);

            NodeWithTerm nodeWithTerm = first(assignment);

            assert nodeWithTerm != null : "part=" + p;

            if (Objects.equals(nodeName, nodeWithTerm.name())) {
                parts.add(p);
            }
        }

        return parts.array();
    }

    /**
     * Returns a raft group leader term.
     *
     * @param partId Partition ID.
     * @return Raft group leader term.
     */
    public long partitionLeaderTerm(int partId) {
        List<NodeWithTerm> assignment = assignments.get(partId);

        return assignment.get(0).term();
    }
}
