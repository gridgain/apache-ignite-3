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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;

/**
 *
 */
public class RawNodeStepper<T> implements Stepper {
    /** */
    private RawNode<T> rawNode;

    /** */
    private MemoryStorage memStorage;

    public RawNodeStepper(RawNode<T> rawNode, MemoryStorage memStorage) {
        this.rawNode = rawNode;
        this.memStorage = memStorage;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return rawNode.basicStatus().id();
    }

    /** {@inheritDoc} */
    @Override public void step(Message m) {
        rawNode.step(m);
    }

    /** {@inheritDoc} */
    @Override public List<Message> readMessages() {
        return rawNode.readMessages();
    }

    public RawNode<T> node() {
        return rawNode;
    }

    public MemoryStorage storage() {
        return memStorage;
    }
}
