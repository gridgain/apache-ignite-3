/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.service;

import java.util.List;
import java.util.concurrent.Future;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.command.CustomCommand;

/**
 * Raft group client service.
 */
public interface RaftService {
    /**
     * @param refresh Refresh state.
     * @return Current group state.
     */
    State state(boolean refresh);

    /**
     * Adds a voring peer to the raft group.
     *
     * @param request   request data
     * @return A future with the result
     */
    Future<GroupChangeResult> addPeer(PeerId id);

    /**
     * Removes a peer from the raft group.
     *
     * @param endpoint  server address
     * @param request   request data
     * @return a future with result
     */
    Future<GroupChangeResult> removePeer(PeerId id);

    /**
     * Locally resets raft group peers. Intended for recovering from a group unavailability at the price of consistency.
     *
     * @param peerId Node to execute the configuration reset.
     * @param request   request data
     * @return A future with result.
     */
    Future<GroupChangeResult> resetPeers(PeerId peerId, List<PeerId> peers);

    /**
     * Takes a local snapshot.
     *
     * @param peerId  Peer id.
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Result> snapshot(PeerId peerId);

    /**
     * Change peers.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<GroupChangeResult> changePeers(List<PeerId> peers);

    /**
     * Adds learners.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<GroupChangeResult> addLearners(List<PeerId> learners);

    /**
     * Removes learners.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<GroupChangeResult> removeLearners(List<PeerId> learners);

    /**
     * Resets learners to new set.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<GroupChangeResult> resetLearners(List<PeerId> learners);

    /**
     * Transfer leadership to other peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    Future<Result> transferLeader(PeerId newLeader);

    /**
     * Performs a custom action defined by specific request on the raft group leader.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    <T extends CustomCommand, R> Future<R> command(T cmd);
}
