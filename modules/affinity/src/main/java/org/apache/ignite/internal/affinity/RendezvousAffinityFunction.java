/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.affinity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.NetworkMember;

/**
 * Affinity function for partitioned table based on Highest Random Weight algorithm. This function supports the
 * following configuration:
 * <ul>
 * <li>
 *      {@code partitions} - Number of partitions to spread across nodes.
 * </li>
 * <li>
 *      {@code excludeNeighbors} - If set to {@code true}, will exclude same-host-neighbors
 *      from being replicas of each other. This flag can be ignored in cases when topology has no enough nodes
 *      for assign replicas.
 *      Note that {@code memberFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
 * </li>
 * <li>
 *      {@code memberFilter} - Optional filter for replica nodes. If provided, then only
 *      nodes that pass this filter will be selected as replica nodes. If not provided, then
 *      replicas nodes will be selected out of all nodes available for this table.
 * </li>
 * </ul>
 */
public class RendezvousAffinityFunction {

    /** Comparator. */
    private static final Comparator<IgniteBiTuple<Long, NetworkMember>> COMPARATOR = new HashComparator();

    /** Maximum number of partitions. */
    public static final int MAX_PARTITIONS_COUNT = 65000;

    /** Exclude neighbors warning. */
    private static boolean exclNeighborsWarn;

    /** Logger instance. */
    private static LogWrapper log = new LogWrapper(RendezvousAffinityFunction.class);

    /**
     * Resolves node hash.
     *
     * @param node Cluster node;
     * @return Node hash.
     */
    public static Object resolveNodeHash(NetworkMember node) {
        return node.name();
    }

    /**
     * Returns collection of nodes for specified partition.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param replicas Number partition replicas.
     * @param neighborhoodCache Neighborhood.
     * @param exclNeighbors If true neighbors are excluded, false otherwise.
     * @param memberFilter Filter for members.
     * @return Assignment.
     */
    public static List<NetworkMember> assignPartition(
        int part,
        List<NetworkMember> nodes,
        int replicas,
        Map<UUID, Collection<NetworkMember>> neighborhoodCache,
        boolean exclNeighbors,
        IgniteBiPredicate<NetworkMember, List<NetworkMember>> memberFilter
    ) {
        if (nodes.size() <= 1)
            return nodes;

        IgniteBiTuple<Long, NetworkMember>[] hashArr =
            (IgniteBiTuple<Long, NetworkMember>[])new IgniteBiTuple[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NetworkMember node = nodes.get(i);

            Object nodeHash = resolveNodeHash(node);

            long hash = hash(nodeHash.hashCode(), part);

            hashArr[i] = new IgniteBiTuple<>(hash, node);
        }

        final int effectiveReplicas = replicas == Integer.MAX_VALUE ? nodes.size() : Math.min(replicas, nodes.size());

        Iterable<NetworkMember> sortedNodes = new LazyLinearSortedContainer(hashArr, effectiveReplicas);

        // REPLICATED cache case
        if (replicas == Integer.MAX_VALUE)
            return replicatedAssign(nodes, sortedNodes);

        Iterator<NetworkMember> it = sortedNodes.iterator();

        List<NetworkMember> res = new ArrayList<>(effectiveReplicas);

        Collection<NetworkMember> allNeighbors = new HashSet<>();

        NetworkMember first = it.next();

        res.add(first);

        if (exclNeighbors)
            allNeighbors.addAll(neighborhoodCache.get(first.id()));

        // Select another replicas.
        if (replicas > 1) {
            while (it.hasNext() && res.size() < effectiveReplicas) {
                NetworkMember node = it.next();

                if (exclNeighbors) {
                    if (!allNeighbors.contains(node)) {
                        res.add(node);

                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                    }
                }
                else if (memberFilter == null || memberFilter.apply(node, res)) {
                    res.add(node);

                    if (exclNeighbors)
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                }
            }
        }

        if (res.size() < effectiveReplicas && nodes.size() >= effectiveReplicas && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors replicas criteria.
            it = sortedNodes.iterator();

            it.next();

            while (it.hasNext() && res.size() < effectiveReplicas) {
                NetworkMember node = it.next();

                if (!res.contains(node))
                    res.add(node);
            }

            if (!exclNeighborsWarn) {
                log.warn("Affinity function excludeNeighbors property is ignored " +
                    "because topology has no enough nodes to assign all replicas.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= effectiveReplicas;

        return res;
    }

    /**
     * Creates assignment for REPLICATED table
     *
     * @param nodes Topology.
     * @param sortedNodes Sorted for specified partitions nodes.
     * @return Assignment.
     */
    private static List<NetworkMember> replicatedAssign(List<NetworkMember> nodes,
        Iterable<NetworkMember> sortedNodes) {
        NetworkMember first = sortedNodes.iterator().next();

        List<NetworkMember> res = new ArrayList<>(nodes.size());

        res.add(first);

        for (NetworkMember n : nodes)
            if (!n.equals(first))
                res.add(n);

        assert res.size() == nodes.size() : "Not enough replicas: " + res.size();

        return res;
    }

    /**
     * The pack partition number and nodeHash.hashCode to long and mix it by hash function based on the Wang/Jenkins
     * hash.
     *
     * @param key0 Hash key.
     * @param key1 Hash key.
     * @return Long hash key.
     * @see <a href="https://gist.github.com/badboy/6267743#64-bit-mix-functions">64 bit mix functions</a>
     */
    private static long hash(int key0, int key1) {
        long key = (key0 & 0xFFFFFFFFL)
            | ((key1 & 0xFFFFFFFFL) << 32);

        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >>> 24);
        key += (key << 3) + (key << 8); // key * 265
        key ^= (key >>> 14);
        key += (key << 2) + (key << 4); // key * 21
        key ^= (key >>> 28);
        key += (key << 31);

        return key;
    }

    /**
     * Generates an assignment by the given parameters.
     *
     * @param currentTopologySnapshot List of topology members.
     * @param partitions Number of table partitions.
     * @param replicas Number partition replicas.
     * @param exclNeighbors If true neighbors are excluded fro the one partition assignment, false otherwise.
     * @param memberFilter Filter for members.
     * @return List nodes by partition.
     */
    public static List<List<NetworkMember>> assignPartitions(
        Collection<NetworkMember> currentTopologySnapshot,
        int partitions,
        int replicas,
        boolean exclNeighbors,
        IgniteBiPredicate<NetworkMember, List<NetworkMember>> memberFilter
    ) {
        assert partitions <= MAX_PARTITIONS_COUNT : "partitions <= " + MAX_PARTITIONS_COUNT;
        assert partitions > 0 : "parts > 0";
        assert replicas > 0 : "replicas > 0";

        List<List<NetworkMember>> assignments = new ArrayList<>(partitions);

        Map<UUID, Collection<NetworkMember>> neighborhoodCache = exclNeighbors ?
            neighbors(currentTopologySnapshot) : null;

        List<NetworkMember> nodes = new ArrayList<>(currentTopologySnapshot);

        for (int i = 0; i < partitions; i++) {
            List<NetworkMember> partAssignment = assignPartition(i, nodes, replicas, neighborhoodCache, exclNeighbors, memberFilter);

            assignments.add(partAssignment);
        }

        return assignments;
    }

    /**
     * Builds neighborhood map for all nodes in snapshot.
     *
     * @param topSnapshot Topology snapshot.
     * @return Neighbors map.
     */
    public static Map<UUID, Collection<NetworkMember>> neighbors(Collection<NetworkMember> topSnapshot) {
        Map<String, Collection<NetworkMember>> macMap = new HashMap<>(topSnapshot.size(), 1.0f);

        // Group by mac addresses.
        for (NetworkMember node : topSnapshot) {
            String macs = String.valueOf(node.hashCode());
            //node.attribute(IgniteNodeAttributes.ATTR_MACS);

            Collection<NetworkMember> nodes = macMap.get(macs);

            if (nodes == null)
                macMap.put(macs, nodes = new HashSet<>());

            nodes.add(node);
        }

        Map<UUID, Collection<NetworkMember>> neighbors = new HashMap<>(topSnapshot.size(), 1.0f);

        for (Collection<NetworkMember> group : macMap.values())
            for (NetworkMember node : group)
                neighbors.put(node.id(), group);

        return neighbors;
    }

    /**
     *
     */
    private static class HashComparator implements Comparator<IgniteBiTuple<Long, NetworkMember>>, Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(IgniteBiTuple<Long, NetworkMember> o1, IgniteBiTuple<Long, NetworkMember> o2) {
            return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                o1.get2().id().compareTo(o2.get2().id());
        }
    }

    /**
     * Sorts the initial array with linear sort algorithm array
     */
    private static class LazyLinearSortedContainer implements Iterable<NetworkMember> {
        /** Initial node-hash array. */
        private final IgniteBiTuple<Long, NetworkMember>[] arr;

        /** Count of the sorted elements */
        private int sorted;

        /**
         * @param arr Node / partition hash list.
         * @param needFirstSortedCnt Estimate count of elements to return by iterator.
         */
        LazyLinearSortedContainer(IgniteBiTuple<Long, NetworkMember>[] arr, int needFirstSortedCnt) {
            this.arr = arr;

            if (needFirstSortedCnt > (int)Math.log(arr.length)) {
                Arrays.sort(arr, COMPARATOR);

                sorted = arr.length;
            }
        }

        /** {@inheritDoc} */
        @Override public Iterator<NetworkMember> iterator() {
            return new SortIterator();
        }

        /**
         *
         */
        private class SortIterator implements Iterator<NetworkMember> {
            /** Index of the first unsorted element. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < arr.length;
            }

            /** {@inheritDoc} */
            @Override public NetworkMember next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (cur < sorted)
                    return arr[cur++].get2();

                IgniteBiTuple<Long, NetworkMember> min = arr[cur];

                int minIdx = cur;

                for (int i = cur + 1; i < arr.length; i++) {
                    if (COMPARATOR.compare(arr[i], min) < 0) {
                        minIdx = i;

                        min = arr[i];
                    }
                }

                if (minIdx != cur) {
                    arr[minIdx] = arr[cur];

                    arr[cur] = min;
                }

                sorted = cur++;

                return min.get2();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException("Remove doesn't supported");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "U.toString(RendezvousAffinityFunction.class, this)";
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }
}
