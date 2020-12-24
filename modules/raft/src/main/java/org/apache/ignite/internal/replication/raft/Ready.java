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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.storage.Entry;

/**
 * Ready encapsulates the entries and messages that are ready to read,
 * be saved to stable storage, committed or sent to other peers.
 * All fields in Ready are read-only.
 */
public class Ready {
    // The current volatile state of a Node.
    // SoftState will be nil if there is no update.
    // It is not required to consume or store SoftState.
	private SoftState softState;

    // The current state of a Node to be saved to stable storage BEFORE
    // Messages are sent.
    // HardState will be equal to empty state if there is no update.
    private HardState hardState;

    // ReadStates can be used for node to serve linearizable read requests locally
    // when its applied index is greater than the index in ReadState.
    // Note that the readState will be returned when raft receives msgReadIndex.
    // The returned is only valid for the request that requested to read.
    private ReadState[] readStates;

    // Entries specifies entries to be saved to stable storage BEFORE
    // Messages are sent.
    private Entry[] entries;

    // Snapshot specifies the snapshot to be saved to stable storage.
    private Snapshot snapshot;

    // CommittedEntries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    private Entry[] committedEntries;

    // Messages specifies outbound messages to be sent AFTER Entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    private Message[] messages;

    // MustSync indicates whether the HardState and Entries must be synchronously
    // written to disk or if an asynchronous write is permissible.
    private boolean mustSync;

    // appliedCursor extracts from the Ready the highest index the client has
    // applied (once the Ready is confirmed via Advance). If no information is
    // contained in the Ready, returns zero.
    public long appliedCursor() {
        int n = committedEntries.length;

        if (n > 0)
            return committedEntries[n - 1].index();

        int idx = snapshot.metadata().index();

        return Math.max(idx, 0);
    }

    public Entry[] committedEntries() {
        return committedEntries;
    }

    public List<Entry> entries() {
        return Arrays.asList(entries);
    }

    public Snapshot snapshot() {
        return snapshot;
    }
}
