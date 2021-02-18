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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.internal.replication.raft.RawNodeBuilder;
import org.apache.ignite.internal.replication.raft.ReadState;
import org.apache.ignite.internal.replication.raft.Ready;
import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.server.tmp.mock.NaiveNetworkMock;
import org.apache.ignite.internal.replication.raft.storage.Entry;
import org.apache.ignite.internal.replication.raft.storage.MemoryStorage;
import org.apache.ignite.internal.replication.raft.storage.UserData;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.StateMachine;
import org.apache.ignite.raft.client.command.Command;
import org.apache.ignite.raft.client.command.CustomCommand;
import org.apache.ignite.raft.client.command.ReadCommand;
import org.apache.ignite.raft.client.command.WriteCommand;
import org.apache.ignite.raft.service.CommandResult;

// TODO sanpwc: Implement timeout logic.
public class RaftServer implements Server {
    private static final long TICK_INTERVAL = 100;

    private final RawNode<Command> rawNode;

    private final StateMachine stateMachine;

    private final MemoryStorage raftStorage;

    private final BlockingQueue<Task> proposalQueue = new LinkedBlockingQueue<>();

    private final Map<IgniteUuid, CompletableFuture> writeCommandProposalStorage = new ConcurrentHashMap<>();

    private final Map<IgniteUuid, ReadCommandProposal> readCommandProposalStorage = new ConcurrentHashMap<>();

    private final ExecutorService fastTrackRequestsExecutor = Executors.newSingleThreadExecutor();

    private final ExecutorService commonRequestsExecutor = Executors.newSingleThreadExecutor();

    private final NaiveNetworkMock networkMock;

    public RaftServer(
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
            Task userProposal = null;

            try {
                userProposal = proposalQueue.poll(TICK_INTERVAL, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                // No-op;
            }

            if (userProposal != null) {
                if (userProposal.command() instanceof ReadCommand) {
                    try {
                        // TODO sanpwc: In some cases, e.g. single node cluster or READ_ONLY_LEASE_BASED we will get
                        // TODO corresponding read state right after calling requestReadIndex().
                        // TODO Not sure whether we should provide slightly optimized solution with completing Future
                        // TODO right here, or handling such situation within common readState processing is also fine.
                        rawNode.requestReadIndex(userProposal.id());
                    }
                    catch (Exception e) {
                        userProposal.result().completeExceptionally(e);
                    }

                    readCommandProposalStorage.put(
                        userProposal.id(),
                        new ReadCommandProposal((ReadCommand)userProposal.command(), userProposal.result()));
                }
                else {
                    rawNode.propose(userProposal.command());

                    writeCommandProposalStorage.put(userProposal.id(), userProposal.result());
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
                    fastTrackRequestsExecutor.execute(new ReadCommandReadyProposalsProcessor(
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
                    if (entry.data() instanceof UserData && ((UserData)entry.data()).data() instanceof WriteCommand)
                        commonRequestsExecutor.execute(new WriteCommandProcessor((WriteCommand)((UserData)entry.data()).data()));
                    else {
                        // TODO sanpwc: Implement configuration processing logic.
                    }
                }

                long oldApplied = rawNode.raftLog().applied();

                rawNode.advance(ready);

                // TODO sanpwc: consider using smarter logic here, for example, some sort of notification based logic.
                if (rawNode.raftLog().applied() > oldApplied)
                    fastTrackRequestsExecutor.execute(new ReadCommandPromouterProcessor(rawNode.raftLog().applied()));
            }
        }
    }

    @Override public boolean isLeader() {
        return rawNode.isLeader();
    }

    @Override public <T extends CustomCommand, R> Future<R> propose(T customCmd) {
        CompletableFuture res = new CompletableFuture();

        proposalQueue.add(new Task(customCmd.id(), customCmd, res));

        return res;
    }

    /** {@inheritDoc} */
    @Override public RawNode<Command> rawNode() {
        return rawNode;
    }

    class ReadCommandReadyProposalsProcessor implements Runnable {

        private final List<ReadState> readStates;

        private final long applied;

        ReadCommandReadyProposalsProcessor(List<ReadState> readStates, long applied) {
            this.readStates = readStates;
            this.applied = applied;
        }

        @Override public void run() {
            for (ReadState state : readStates) {
                ReadCommandProposal readCmdProposal = readCommandProposalStorage.get(state.context());

                readCmdProposal.readIndex(state.index());

                if (applied >= state.index()) {
                    readCommandProposalStorage.remove(state.context());

                    readCmdProposal.result().complete(stateMachine.applyRead(readCmdProposal.command()));
                }
                else
                    readCmdProposal.readIndex(state.index());
            }
        }
    }

    class WriteCommandProcessor implements Runnable {
        private final WriteCommand customCmd;

        WriteCommandProcessor(WriteCommand customCmd) {
            this.customCmd = customCmd;
        }

        @Override public void run() {
                Object stateMachineRes = stateMachine.applyWrite(customCmd);

                CompletableFuture res = writeCommandProposalStorage.remove(customCmd.id());

                if (res != null)
                    res.complete(stateMachineRes);
        }
    }

    class ReadCommandPromouterProcessor implements Runnable {
        private final long applied;

        ReadCommandPromouterProcessor(long applied) {
            this.applied = applied;
        }

        @Override public void run() {
            Iterator<ReadCommandProposal> fastTrackRequestIterator = readCommandProposalStorage.values().iterator();

            while (fastTrackRequestIterator.hasNext()) {
                ReadCommandProposal readCmdProposal = fastTrackRequestIterator.next();

                if (applied >= readCmdProposal.readIndex()) {
                    fastTrackRequestIterator.remove();

                    readCmdProposal.result().complete(stateMachine.applyRead(readCmdProposal.command()));
                }
            }
        }
    }
}
