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

package org.apache.ignite.internal.replication.raft.server;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.internal.replication.raft.RawNodeBuilder;
import org.apache.ignite.internal.replication.raft.ReadState;
import org.apache.ignite.internal.replication.raft.Ready;
import org.apache.ignite.internal.replication.raft.common.CustomOperation;
import org.apache.ignite.internal.replication.raft.common.NaiveNetworkMock;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.common.Operation;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.apache.ignite.lang.IgniteUuid;

// TODO sanpwc: Implement timeout logic.
public abstract class AbstractServer<T extends CustomOperation> implements Server<T> {
    private static final long TICK_INTERVAL = 100;

    private final RawNode<Operation> rawNode;

    private final StateMachine<Operation> stateMachine;

    private final MemoryStorage raftStorage;

    private final BlockingQueue<Task<T>> proposalQueue = new LinkedBlockingQueue<>();

    private final Map<IgniteUuid, ExceptionableFutureTask> commonRequestStorage = new ConcurrentHashMap<>();

    private final Map<IgniteUuid, FastTrackRequest> fastTrackRequestStorage = new ConcurrentHashMap<>();

    private final ExecutorService fastTrackRequestsExecutor = Executors.newSingleThreadExecutor();

    private final ExecutorService commonRequestsExecutor = Executors.newSingleThreadExecutor();

    private final NaiveNetworkMock networkMock;

    protected AbstractServer(
        MemoryStorage raftStorage,
        RawNodeBuilder rawNodeBuilder,
        StateMachine stateMachine,
        NaiveNetworkMock netMock
    ) {
        this.raftStorage = raftStorage;
        this.rawNode = rawNodeBuilder.build();
        this.stateMachine = stateMachine;
        this.networkMock = netMock;

        new Thread(this::eventLoop).start();
    }

    private void eventLoop() {
        long lastTick = System.currentTimeMillis();

        while (true) {
            Task<T> userProposal = null;

            try {
                userProposal = proposalQueue.poll(TICK_INTERVAL, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                // No-op;
            }

            if (userProposal != null) {
                if (userProposal.fastTrack()) {
                    try {
                        // TODO sanpwc: In some cases, e.g. single node cluster or READ_ONLY_LEASE_BASED we will get
                        // TODO corresponding read state right after calling requestReadIndex().
                        // TODO Not sure whether we should provide slightly optimized solution with completing Future
                        // TODO right here, or handling such situation within common readState processing is also fine.
                        rawNode.requestReadIndex(userProposal.id());
                    }
                    catch (Exception e) {
                        userProposal.result().setException(e);
                    }

                    fastTrackRequestStorage.put(userProposal.id(), new FastTrackRequest(userProposal.result()));
                }
                else {
                    rawNode.propose(userProposal.data());

                    commonRequestStorage.put(userProposal.id(), userProposal.result());
                }
            }

            long now = System.currentTimeMillis();

            if (now - lastTick > TICK_INTERVAL) {
                rawNode.tick();
                lastTick = now;
            }

            if (rawNode.hasReady()) {
                Ready ready = rawNode.ready();

                // Persist hard state and entries.
                if (ready.hardState() != null)
                    raftStorage.saveHardState(ready.hardState());

                if (!ready.entries().isEmpty())
                    raftStorage.append(ready.entries());

                if (!ready.readStates().isEmpty()) {
                    fastTrackRequestsExecutor.execute(new FastTrackReadyProposalsProcessor(
                        ready.readStates(),
                        rawNode.raftLog().applied()));
                }

                // TODO sanpwc: In go impl there's both rc.wal.Save(rd.HardState, rd.Entries) and
                // TODO rc.raftStorage.Append(rd.Entries), see func (rc *raftNode) serveChannels()

                // TODO sanpwc: tmp.
                for (Message msg : ready.messages())
                    networkMock.processMessage(msg);

                // Apply committed entries.
                for (Entry entry : ready.committedEntries()) {
                    if (entry.data() instanceof UserData)
                        commonRequestsExecutor.execute(new CommonRequestProcessor(((UserData<Operation>) entry.data()).data()));
                    else {
                        // TODO sanpwc: Implement configuration processing logic.
                    }
                }

                long oldApplied = rawNode.raftLog().applied();

                rawNode.advance(ready);

                // TODO sanpwc: consider using smarter logic here, for example, some sort of notification based logic.
                if (rawNode.raftLog().applied() > oldApplied)
                    fastTrackRequestsExecutor.execute(new FastTrackPromouterProcessor(rawNode.raftLog().applied()));
            }
        }
    }

    @Override public boolean isLeader() {
        return rawNode.isLeader();
    }

    protected void propose(Task task) {
        proposalQueue.add(task);
    }

    /**
     * @return State machine.
     */
    protected StateMachine stateMachine() {
        return stateMachine;
    }


    /** {@inheritDoc} */
    @Override public RawNode<Operation> rawNode() {
        return rawNode;
    }

    class FastTrackReadyProposalsProcessor implements Runnable {

        private final List<ReadState> readStates;

        private final long applied;

        FastTrackReadyProposalsProcessor(List<ReadState> readStates, long applied) {
            this.readStates = readStates;
            this.applied = applied;
        }

        @Override public void run() {
            for (ReadState state : readStates) {
                FastTrackRequest fastTrackRequest = fastTrackRequestStorage.get(state.context());

                fastTrackRequest.readIndex(state.index());

                if (applied >= state.index()) {
                    fastTrackRequest.result().run();

                    fastTrackRequestStorage.remove(state.context());
                }
                else
                    fastTrackRequest.readIndex(state.index());
            }
        }
    }

    class CommonRequestProcessor implements Runnable {
        private final Operation operation;

        CommonRequestProcessor(Operation operation) {
            this.operation = operation;
        }

        @Override public void run() {
            if (operation instanceof CustomOperation) {
                stateMachine.apply(operation);

                ExceptionableFutureTask res = commonRequestStorage.remove(((CustomOperation)operation).id());

                if (res != null)
                    res.run();
            }
        }
    }

    class FastTrackPromouterProcessor implements Runnable {
        private final long applied;

        FastTrackPromouterProcessor(long applied) {
            this.applied = applied;
        }

        @Override public void run() {
            Iterator<FastTrackRequest> fastTrackRequestIterator = fastTrackRequestStorage.values().iterator();

            while (fastTrackRequestIterator.hasNext()) {
                FastTrackRequest fastTrackRequest = fastTrackRequestIterator.next();

                if (applied >= fastTrackRequest.readIndex()) {
                    fastTrackRequest.result().run();

                    fastTrackRequestIterator.remove();
                }
            }
        }
    }
}
