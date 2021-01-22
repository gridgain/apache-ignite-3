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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;

/**
 *
 */
public class Network {
    private final UUID[] ids;
    private final Map<UUID, Stepper> peers;

    public UUID[] ids() {
        return ids;
    }

    public Network(Stepper... steppers) {
        peers = new HashMap<>(steppers.length, 1.0f);

        ids = new UUID[steppers.length];

        for (int i = 0; i < steppers.length; i++)
            ids[i] = steppers[i].id();

        for (Stepper node : steppers)
            peers.put(node.id(), node);
    }

    public <T extends Stepper> T peer(UUID id) {
        return (T)peers.get(id);
    }

    public <T extends Stepper> void action(UUID id, Consumer<T> action) {
        Stepper stepper = peers.get(id);

        action.accept((T)stepper);

        drainQueue(stepper.readMessages());
    }

    public MemoryStorage storage(UUID peerId) {
        return this.<RawNodeStepper<String>>peer(peerId).storage();
    }

    public void send(Message msg) {
        drainQueue(Collections.singletonList(msg));
    }

    private void drainQueue(List<Message> init) {
        Queue<Message> msgs = new ArrayDeque<>();

        msgs.addAll(init);

        while (!msgs.isEmpty()) {
            Message polled = msgs.poll();

            Stepper peer = peers.get(polled.to());

            if (peer != null) {
                peer.step(polled);

                msgs.addAll(peer.readMessages());
            }
        }
    }
}
