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

/**
 * The config object contains parameters required to start a Raft group.
 */
public class RaftConfig {
    /**
     * {@code electionTick} is the number of {@link RawNode#tick()} invocations that must pass between
     * elections. That is, if a follower does not receive any message from the
     * leader of current term before {@code electionTick} has elapsed, it will become
     * candidate and start an election. {@code electionTick} must be greater than
     * {@code heartbeatTick}. We suggest {@code electionTick = 10 * heartbeatTick} to avoid
     * unnecessary leader switching.
     */
    private int electionTick;

    /**
     * {@code heartbeatTick} is the number of {@link Node#tick()} invocations that must pass between
     * heartbeats. That is, a leader sends heartbeat messages to maintain its
     * leadership every {@code heartbeatTick} ticks.
     */
    private int heartbeatTick;

    /**
     * {@code maxSizePerMsg} limits the max byte size of each append message. Smaller
     * value lowers the raft recovery cost (initial probing and message lost
     * during normal operation). On the other side, it might affect the
     * throughput during normal replication.
     * <p>
     * Integer.MAX_VALUE for unlimited, 0 for at most one entry per message.
     */
    private int maxSizePerMsg;

    /**
     * {@code maxCommittedSizePerReady} limits the size of the committed entries
     * returned by a single {@link Node#ready()} call.
     */
    private int maxCommittedSizePerReady;

    /**
     * {@code maxUncommittedEntriesSize} limits the aggregate byte size of the
     * uncommitted entries that may be appended to a leader's log. Once this
     * limit is exceeded, proposals will begin to fail with {@link ProposalDroppedException}.
     * <p>
     * 0 for no limit.
     */
    private long maxUncommittedEntriesSize;

    /**
     * {@code maxInflightMsgs} limits the max number of in-flight append messages during
     * optimistic replication phase. The application transport layer usually
     * has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
     * overflowing that sending buffer.
     */
    private int maxInflightMsgs = Integer.MAX_VALUE;

    /**
     * {@code checkQuorum} specifies if the leader should check quorum activity. Leader
     * steps down when quorum is not active for election timeout.
     */
    private boolean checkQuorum;

    /**
     * PreVote enables the Pre-Vote algorithm described in raft thesis section
     * 9.6. This prevents disruption when a node that has been partitioned away
     * rejoins the cluster.
     * TODO agoncharuk: I see no reason to keep this option, preVote should be always true?
     */
    private boolean preVote;

    /**
     * {@code checkQuorum} MUST be enabled if {@link ReadOnlyOption#READ_ONLY_LEASE_BASED} is used.
     * TODO agoncharuk: this should be moved to the read-only request so we can change the read guarantees at runtime.
     */
    private ReadOnlyOption readOnlyOption = ReadOnlyOption.READ_ONLY_SAFE;

    /**
     * @return
     */
    public int electionTick() {
        return electionTick;
    }

    /**
     * @param electionTick
     */
    public RaftConfig electionTick(int electionTick) {
        this.electionTick = electionTick;

        return this;
    }

    /**
     * @return
     */
    public int heartbeatTick() {
        return heartbeatTick;
    }

    /**
     * @param heartbeatTick
     */
    public RaftConfig heartbeatTick(int heartbeatTick) {
        this.heartbeatTick = heartbeatTick;

        return this;
    }

    /**
     * @return
     */
    public int maxSizePerMsg() {
        return maxSizePerMsg;
    }

    /**
     * @param maxSizePerMsg
     */
    public RaftConfig maxSizePerMsg(int maxSizePerMsg) {
        this.maxSizePerMsg = maxSizePerMsg;

        return this;
    }

    /**
     * @return
     */
    public int maxCommittedSizePerReady() {
        return maxCommittedSizePerReady;
    }

    /**
     * @param maxCommittedSizePerReady
     */
    public RaftConfig maxCommittedSizePerReady(int maxCommittedSizePerReady) {
        this.maxCommittedSizePerReady = maxCommittedSizePerReady;

        return this;
    }

    /**
     * @return
     */
    public long maxUncommittedEntriesSize() {
        return maxUncommittedEntriesSize;
    }

    /**
     * @param maxUncommittedEntriesSize
     */
    public RaftConfig maxUncommittedEntriesSize(long maxUncommittedEntriesSize) {
        this.maxUncommittedEntriesSize = maxUncommittedEntriesSize;

        return this;
    }

    /**
     * @return
     */
    public int maxInflightMsgs() {
        return maxInflightMsgs;
    }

    /**
     * @param maxInflightMsgs
     */
    public RaftConfig maxInflightMsgs(int maxInflightMsgs) {
        this.maxInflightMsgs = maxInflightMsgs;

        return this;
    }

    /**
     * @return
     */
    public boolean checkQuorum() {
        return checkQuorum;
    }

    /**
     * @param checkQuorum
     */
    public RaftConfig checkQuorum(boolean checkQuorum) {
        this.checkQuorum = checkQuorum;

        return this;
    }

    /**
     * @return
     */
    public boolean preVote() {
        return preVote;
    }

    /**
     * @param preVote
     */
    public RaftConfig preVote(boolean preVote) {
        this.preVote = preVote;

        return this;
    }

    /**
     * @return
     */
    public ReadOnlyOption readOnlyOption() {
        return readOnlyOption;
    }

    /**
     * @param readOnlyOption
     */
    public RaftConfig readOnlyOption(ReadOnlyOption readOnlyOption) {
        this.readOnlyOption = readOnlyOption;

        return this;
    }
}
