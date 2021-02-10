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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;

/**
 *
 */
public class Network {
    private Random rnd;
    private final UUID[] ids;
    private final Map<UUID, Stepper> peers;

    private Map<Edge, Double> drop = new HashMap<>();
    private Set<MessageType> ignore = new HashSet<>();

    public UUID[] ids() {
        return ids;
    }

    public Network(Random rnd, Stepper... steppers) {
        this.rnd = rnd;

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

    public <T> void action(UUID id, Consumer<RawNodeStepper<T>> action) {
        Stepper stepper = peers.get(id);

        action.accept((RawNodeStepper<T>)stepper);

        drainQueue(stepper.readMessages());
    }

    public void multiAction(Runnable r) {
        r.run();

        List<Message> msgs = new ArrayList<>();

        for (Stepper s : peers.values())
            msgs.addAll(s.readMessages());

        drainQueue(msgs);
    }

    public MemoryStorage storage(UUID peerId) {
        return this.<RawNodeStepper<String>>peer(peerId).storage();
    }

    public void send(Message msg) {
        drainQueue(Collections.singletonList(msg));
    }

    public void cut(UUID peer1, UUID peer2) {
        drop(peer1, peer2, 2.d);
    }

    public void isolate(UUID id) {
        for (UUID nId : ids) {
            if (!id.equals(nId)) {
                drop(id, nId, 1.d);
                drop(nId, id, 1.d);
            }
        }
    }

    public void ignore(MessageType msgType) {
        ignore.add(msgType);
    }

    public void drop(UUID peer1, UUID peer2, double percentage) {
        drop.put(new Edge(peer1, peer2), percentage);
    }

    public void recover() {
        drop.clear();
        ignore.clear();
    }

    private void drainQueue(List<Message> init) {
        Queue<Message> msgs = new ArrayDeque<>();

        msgs.addAll(init);

        while (!msgs.isEmpty()) {
            Message polled = msgs.poll();

            if (!filtered(polled)) {
                Stepper peer = peers.get(polled.to());

                if (peer != null) {
                    peer.step(polled);

                    msgs.addAll(peer.readMessages());
                }
            }
        }
    }

    private boolean filtered(Message msg) {
        if (ignore.contains(msg.type()))
            return true;

        Edge e = new Edge(msg.from(), msg.to());

        if (drop.containsKey(e)) {
            double percentage = drop.get(e);

            if (rnd.nextDouble() < percentage) {
                return true;
            }
        }

        return false;
    }

    private static class Edge {
        private final UUID peer1;
        private final UUID peer2;

        private Edge(UUID peer1, UUID peer2) {
            this.peer1 = peer1;
            this.peer2 = peer2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Edge edge = (Edge)o;

            return peer1.equals(edge.peer1) && peer2.equals(edge.peer2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = peer1.hashCode();

            result = 31 * result + peer2.hashCode();

            return result;
        }
    }
}
