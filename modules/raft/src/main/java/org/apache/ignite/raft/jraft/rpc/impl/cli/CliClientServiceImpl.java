/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.CliClientService;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.rpc.impl.AbstractClientService;

/**
 *
 */
public class CliClientServiceImpl extends AbstractClientService implements CliClientService {
    /** Options. */
    private CliOptions cliOptions;

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        boolean ret = super.init(rpcOptions);
        if (ret) {
            this.cliOptions = (CliOptions) this.rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> addPeer(final PeerId peerId, final AddPeerRequest request,
        final RpcResponseClosure<AddPeerResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> removePeer(final PeerId peerId, final RemovePeerRequest request,
        final RpcResponseClosure<RemovePeerResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> resetPeer(final PeerId peerId, final ResetPeerRequest request,
        final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> snapshot(final PeerId peerId, final SnapshotRequest request,
        final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> changePeersAndLearners(final PeerId peerId, final ChangePeersAndLearnersRequest request,
        final RpcResponseClosure<ChangePeersAndLearnersResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> addLearners(final PeerId peerId, final AddLearnersRequest request,
        final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> removeLearners(final PeerId peerId, final RemoveLearnersRequest request,
        final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> resetLearners(final PeerId peerId, final ResetLearnersRequest request,
        final RpcResponseClosure<LearnersOpResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> getLeader(final PeerId peerId, final GetLeaderRequest request,
        final RpcResponseClosure<GetLeaderResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> transferLeader(final PeerId peerId, final TransferLeaderRequest request,
        final RpcResponseClosure<ErrorResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> getPeers(final PeerId peerId, final CliRequests.GetPeersRequest request,
        final RpcResponseClosure<CliRequests.GetPeersResponse> done) {
        return invokeWithDone(peerId, request, done, this.cliOptions.getTimeoutMs());
    }
}
