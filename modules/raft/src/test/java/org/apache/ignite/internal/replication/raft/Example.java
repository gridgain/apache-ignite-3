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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.storage.Entry;

/**
 * 1. Model interaction bw RawNode & metastorage/partition
 * 2. Merge RawNode & Raft
 * 3. MessageFactory and messages interfaces
 * 4. RaftLog - impl
 * 5. ReadIndex - understand and optimize
 *
 */
public class Example {
    public Object get(Object key) {
        long linearizedReadIndex = partition.getReadIndex().get();

        return stateMachine.waitAppliedTo(linearizedReadIndex).get(key);
    }

    public void usage() throws InterruptedException {
        RawNode node = new RawNode();
        final long tickTimeout = 100;

        BlockingQueue<Message> incoming = new LinkedBlockingQueue<>();
        long lastTick = System.currentTimeMillis();

        while (true) {
            Message polled = incoming.poll(tickTimeout, TimeUnit.MILLISECONDS);

            if (polled != null)
                node.step(polled);

            long now = System.currentTimeMillis();

            if (now - lastTick > tickTimeout) {
                node.tick();
                lastTick = now;
            }

            if (node.hasReady()) {
                Ready rd = node.ready();

                persistLogAndSendMessages(rd);

                node.advance(rd);

                applyCommitted(rd);
            }
        }
    }

    private void persistLogAndSendMessages(Ready rd) {

    }

    private void applyCommitted(Ready rd) {
        for (Entry entry : rd.committedEntries()) {
            // Apply to state machine.
        }
    }
}
