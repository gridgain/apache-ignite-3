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

package org.apache.ignite.internal.replication.raft;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 *
 */
public class ConfigState {
    private final Set<UUID> voters;
    private final Set<UUID> outgoing;
    private final Set<UUID> learners;
    private final Set<UUID> learnersNext;
    private final boolean autoLeave;

    /**
     * Utility method to create a initial config state when starting a new Raft cluster.
     *
     * @param peers Contains the IDs of all nodes (including self) in the Raft group.
     * @param learners contains the IDs of all learner nodes (including self if the local node
     *      is a learner) in the Raft group. Learners only receive entries from the leader node.
     *      It does not vote or promote itself.
     * @return Instance of bootstrap config state.
     */
    public static ConfigState bootstrap(List<UUID> peers, List<UUID> learners) {
        Objects.requireNonNull(peers, "peers must be non-null");
        Objects.requireNonNull(learners, "learners must be non-null");

        return new ConfigState(
            Collections.unmodifiableSet(new TreeSet<>(peers)),
            Collections.emptySet(),
            Collections.unmodifiableSet(new TreeSet<>(learners)),
            Collections.emptySet(),
            false);
    }

    public ConfigState(
        Set<UUID> voters,
        Set<UUID> outgoing,
        Set<UUID> learners,
        Set<UUID> learnersNext,
        boolean autoLeave
    ) {
        Objects.requireNonNull(voters, "voters must be non-null");
        Objects.requireNonNull(outgoing, "outgoing must be non-null");
        Objects.requireNonNull(learners, "learners must be non-null");
        Objects.requireNonNull(learnersNext, "learnersNext must be non-null");

        this.voters = voters;
        this.outgoing = outgoing;
        this.learners = learners;
        this.learnersNext = learnersNext;
        this.autoLeave = autoLeave;
    }

    /**
     * @return
     */
    public Set<UUID> voters() {
        return voters;
    }

    /**
     * @return
     */
    public Set<UUID> outgoing() {
        return outgoing;
    }

    /**
     * @return
     */
    public Set<UUID> learners() {
        return learners;
    }

    /**
     * @return
     */
    public Set<UUID> learnersNext() {
        return learnersNext;
    }

    /**
     * @return
     */
    public boolean autoLeave() {
        return autoLeave;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConfigState that = (ConfigState)o;

        return autoLeave == that.autoLeave &&
            voters.equals(that.voters) &&
            outgoing.equals(that.outgoing) &&
            learners.equals(that.learners) &&
            learnersNext.equals(that.learnersNext);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = voters.hashCode();

        result = 31 * result + outgoing.hashCode();
        result = 31 * result + learners.hashCode();
        result = 31 * result + learnersNext.hashCode();
        result = 31 * result + (autoLeave ? 1 : 0);

        return result;
    }
}
