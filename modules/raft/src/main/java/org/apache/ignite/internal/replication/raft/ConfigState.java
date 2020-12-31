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
        return new ConfigState(
            Collections.unmodifiableSet(new TreeSet<>(peers)),
            null,
            Collections.unmodifiableSet(new TreeSet<>(learners)),
            null,
            false);
    }

    public ConfigState(
        Set<UUID> voters,
        Set<UUID> outgoing,
        Set<UUID> learners,
        Set<UUID> learnersNext,
        boolean autoLeave
    ) {
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
}
