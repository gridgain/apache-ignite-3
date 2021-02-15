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

package org.apache.ignite.internal.replication.raft.server.partitioned;

import java.util.concurrent.Future;
import org.apache.ignite.internal.replication.raft.RawNodeBuilder;
import org.apache.ignite.internal.replication.raft.common.NaiveNetworkMock;
import org.apache.ignite.internal.replication.raft.common.paritioned.GetPartitionedOperation;
import org.apache.ignite.internal.replication.raft.common.paritioned.PartitionedOperation;
import org.apache.ignite.internal.replication.raft.common.paritioned.PutPartitionedOperation;
import org.apache.ignite.internal.replication.raft.server.AbstractServer;
import org.apache.ignite.internal.replication.raft.common.ExceptionableFutureTask;
import org.apache.ignite.internal.replication.raft.server.Task;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;

public class PartitionedServer extends AbstractServer<PartitionedOperation> {

    public PartitionedServer(MemoryStorage raftStorage, RawNodeBuilder rawNodeBuilder, NaiveNetworkMock netMock) {
        super(raftStorage, rawNodeBuilder, new PartitionedStateMachine(), netMock);
    }

    /** {@inheritDoc} */
    @Override public Future process(PartitionedOperation operation) {
        switch (operation.type()) {
            case PUT:
                return put((PutPartitionedOperation)operation);

            case GET:
                return get((GetPartitionedOperation)operation);

            default:
                throw new UnsupportedOperationException("Operation=[" + operation + "] is not supported.");
        }
    }

    /** {@inheritDoc} */
    @Override public Future process(byte[] rawPartitionedOperation) {
        // TODO sanpwc: Implement process() for serialized(remote) calls.
        return null;
    }

    public Future put(PutPartitionedOperation putOperation) {
        ExceptionableFutureTask res = new ExceptionableFutureTask(() -> null);

        propose(new Task(putOperation.id(), putOperation, res, false));

        return res;
    }

    public Future get (GetPartitionedOperation getOperation) {
        ExceptionableFutureTask res =
            new ExceptionableFutureTask(() -> stateMachine().getValue(getOperation.key()));

        propose(new Task(getOperation.id(), null, res, true));

        return res;
    }

    /** {@inheritDoc} */
    @Override protected PartitionedStateMachine stateMachine() {
        return (PartitionedStateMachine) super.stateMachine();
    }
}
