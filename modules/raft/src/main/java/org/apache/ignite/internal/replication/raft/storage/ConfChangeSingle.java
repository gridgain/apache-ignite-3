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

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * ConfChangeSingle is an individual configuration change operation. Multiple
 * such operations can be carried out atomically via a ConfChange
 */
public class ConfChangeSingle implements ConfChange {
    public enum ConfChangeType {
        ConfChangeAddNode,
        ConfChangeRemoveNode,
        ConfChangeUpdateNode,
        ConfChangeAddLearnerNode
    }

    private final UUID nodeId;
    private final ConfChangeType type;

    public ConfChangeSingle(UUID nodeId, ConfChangeType type) {
        this.nodeId = nodeId;
        this.type = type;
    }

    public UUID nodeId() {
        return nodeId;
    }

    public ConfChangeType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public ConfChangeTransition transition() {
        return ConfChangeTransition.ConfChangeTransitionAuto;
    }

    /** {@inheritDoc} */
    @Override public List<ConfChangeSingle> changes() {
        return Collections.singletonList(this);
    }
}
