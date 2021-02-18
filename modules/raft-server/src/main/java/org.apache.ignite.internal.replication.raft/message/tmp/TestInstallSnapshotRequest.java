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

package org.apache.ignite.internal.replication.raft.message.tmp;

import org.apache.ignite.internal.replication.raft.Snapshot;
import org.apache.ignite.internal.replication.raft.message.InstallSnapshotRequest;
import org.apache.ignite.internal.replication.raft.message.MessageType;

import java.util.UUID;

/**
 *
 */
public class TestInstallSnapshotRequest extends TestBaseMessage implements InstallSnapshotRequest {
    /** */
    private final Snapshot snap;

    public TestInstallSnapshotRequest(UUID from, UUID to, long term, Snapshot snap) {
        super(MessageType.MsgSnap, from, to, term);

        this.snap = snap;
    }

    @Override public Snapshot snapshot() {
        return snap;
    }
}
