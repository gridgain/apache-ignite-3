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

package org.apache.ignite.internal.replication.raft.storage;

import java.util.List;

/**
 *
 */
public class ConfChangeJoint implements ConfChange {
    /** */
    private final ConfChangeTransition transition;

    /** */
    private final List<ConfChangeSingle> changes;

    public ConfChangeJoint(List<ConfChangeSingle> changes) {
        this(ConfChangeTransition.ConfChangeTransitionAuto, changes);
    }

    public ConfChangeJoint(ConfChangeTransition transition, List<ConfChangeSingle> changes) {
        this.transition = transition;
        this.changes = changes;
    }

    /** {@inheritDoc} */
    @Override public ConfChangeTransition transition() {
        return transition;
    }

    /** {@inheritDoc} */
    @Override public List<ConfChangeSingle> changes() {
        return changes;
    }
}
