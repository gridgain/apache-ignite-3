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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.quorum.JointConfig;

/**
 * TrackerConfig reflects the configuration tracked in a ProgressTracker.
 */
public class TrackerConfig {
    private final JointConfig voters;

    // autoLeave is true if the configuration is joint and a transition to the
    // incoming configuration should be carried out automatically by Raft when
    // this is possible. If false, the configuration will be joint until the
    // application initiates the transition manually.
    private final boolean autoLeave;

    // learners is a set of IDs corresponding to the learners active in the
    // current configuration.
    //
    // Invariant: learners and voters does not intersect, i.e. if a peer is in
    // either half of the joint config, it can't be a learner; if it is a
    // learner it can't be in either half of the joint config. This invariant
    // simplifies the implementation since it allows peers to have clarity about
    // its current role without taking into account joint consensus.
    private final Set<UUID> learners;

    // When we turn a voter into a learner during a joint consensus transition,
    // we cannot add the learner directly when entering the joint state. This is
    // because this would violate the invariant that the intersection of
    // voters and learners is empty. For example, assume a voter is removed and
    // immediately re-added as a learner (or in other words, it is demoted):
    //
    // Initially, the configuration will be
    //
    //   voters:   {1 2 3}
    //   learners: {}
    //
    // and we want to demote 3. Entering the joint configuration, we naively get
    //
    //   voters:   {1 2 3} & {1 2}
    //   learners: {3}
    //
    // but this violates the invariant (3 is both voter and learner). Instead,
    // we get
    //
    //   voters:   {1 2 3} & {1 2}
    //   learners: {}
    //   next_learners: {3}
    //
    // Where 3 is now still purely a voter, but we are remembering the intention
    // to make it a learner upon transitioning into the final configuration:
    //
    //   voters:   {1 2}
    //   learners: {3}
    //   next_learners: {}
    //
    // Note that next_learners is not used while adding a learner that is not
    // also a voter in the joint config. In this case, the learner is added
    // right away when entering the joint configuration, so that it is caught up
    // as soon as possible.
    private final Set<UUID> learnersNext;

    public TrackerConfig(JointConfig voters, Set<UUID> learners, Set<UUID> learnersNext, boolean autoLeave) {
        this.voters = voters;
        this.learners = learners == null ? null : Collections.unmodifiableSet(learners);
        this.learnersNext = learnersNext == null ? learnersNext : Collections.unmodifiableSet(learnersNext);
        this.autoLeave = autoLeave;
    }

    public JointConfig voters() {
        return voters;
    }

    public Set<UUID> learners() {
        return learners;
    }

    public Set<UUID> learnersNext() {
        return learnersNext;
    }

    /**
     */
    public boolean autoLeave() {
        return autoLeave;
    }
}
