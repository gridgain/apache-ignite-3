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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.replication.raft.common.paritioned.PartitionedOperation;
import org.apache.ignite.internal.replication.raft.common.paritioned.PutPartitionedOperation;
import org.apache.ignite.internal.replication.raft.server.StateMachine;

public class PartitionedStateMachine implements StateMachine<PartitionedOperation> {

    private final Map<Object, Object> innerSMStorageMock = new ConcurrentHashMap<>();

    @Override public void apply(PartitionedOperation data) {
        switch (data.type()) {
            case PUT: {
                PutPartitionedOperation putOperation = (PutPartitionedOperation) data;

                innerSMStorageMock.put(putOperation.key(), putOperation.value());

                break;
            }

            case GET: {
                // TODO sanpwc: According to current logic we should call get explicitly.
                throw new UnsupportedOperationException("Operation=[" + data + "] is not supported.");
            }

            default:
                throw new UnsupportedOperationException("Operation=[" + data + "] is not supported.");
        }
    }

    Object getValue(Object key) {
        return innerSMStorageMock.get(key);
    }
}
