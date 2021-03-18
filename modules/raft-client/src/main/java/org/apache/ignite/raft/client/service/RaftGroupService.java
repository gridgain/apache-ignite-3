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

package org.apache.ignite.raft.client.service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.PeerId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A service providing operations on a replication group.
 * <p>
 * Most of operations require a known group leader. The group leader can be refreshed at any time by calling
 * {@link #refreshLeader()} method, otherwise it will happen automatically on a first call.
 * <p>
 * If a leader has been changed while the operation in progress, it will be transparently retried until timeout is
 * reached. The current leader will be refreshed automatically (maybe several times) in the process.
 * <p>
 * Each async method (returning a future) uses a default timeout to finish. If a result is not ready within the timeout,
 * the future will be completed with a {@link TimeoutException}
 * <p>
 * If an error is occured during operation execution, the future will be completed with the corresponding
 * IgniteException having an error code and a related message.
 * <p>
 * Async operations provided by the service are not cancellable.
 */
public interface RaftGroupService {
    /**
     * @return Group id.
     */
    @NotNull String groupId();

    /**
     * @return Default timeout for the operations in milliseconds.
     */
    long timeout();

    /**
     * Changes default timeout value for all subsequent operations.
     *
     * @param newTimeout New timeout value.
     */
    void timeout(long newTimeout);

    /**
     * @return Current leader id or {@code null} if it has not been yet initialized.
     */
    @Nullable PeerId leader();

    /**
     * @return List of voting peers or {@code null} if it has not been yet initialized.
     */
    @Nullable List<PeerId> peers();

    /**
     * @return List of leaners or {@code null} if it has not been yet initialized.
     */
    @Nullable List<PeerId> learners();

    /**
     * Refreshes a replication group leader.
     *
     * @return A future to wait for completion.
     */
    CompletableFuture<Void> refreshLeader();

    /**
     * Refreshes replication group members.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @return A future to wait for completion.
     */
    CompletableFuture<Void> refreshMembers();

    /**
     * Adds a voting peer to the raft group.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<Void> addPeers(Collection<PeerId> peerIds);

    /**
     * Removes a peer from the raft group.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<Void> removePeers(Collection<PeerId> peerIds);

    /**
     * Adds learners (non-voting members).
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param learners List of learners.
     * @return A future with the result.
     */
    CompletableFuture<Void> addLearners(List<PeerId> learners);

    /**
     * Removes learners.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param learners List of learners.
     * @return A future with the result.
     */
    CompletableFuture<Void> removeLearners(List<PeerId> learners);

    /**
     * Takes a state machine snapshot on a given group peer.
     *
     * @param peerId Peer id.
     * @return A future with the result.
     */
    CompletableFuture<Void> snapshot(PeerId peerId);

    /**
     * Transfer leadership to other peer.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param newLeader New leader.
     * @return A future with the result.
     */
    CompletableFuture<Void> transferLeadership(PeerId newLeader);

    /**
     * Locally resets raft group peers. Intended for recovering from a group unavailability at the price of consistency.
     *
     * @param peerId Peer id.
     * @param peers List of peers.
     * @return A future with the result.
     */
    CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers);

    /**
     * Submits a user command to a replication group leader.
     *
     * @param cmd The command.
     * @param <R> Response type.
     * @return A future with the result.
     */
    <R> CompletableFuture<R> submit(Command cmd);
}
