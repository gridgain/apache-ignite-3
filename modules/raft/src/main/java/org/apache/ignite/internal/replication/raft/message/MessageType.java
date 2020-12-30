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

package org.apache.ignite.internal.replication.raft.message;

/**
 *
 */
public enum MessageType {
    MsgApp(false),
    MsgAppResp(true),
    MsgVote(false),
    MsgVoteResp(true),
    MsgSnap(false),
    MsgHeartbeat(false),
    MsgHeartbeatResp(true),
    MsgTimeoutNow(false),
    // TODO agoncharuk pre-vote is already a flag, need to remove the separate message type.
    MsgPreVote(false),
    MsgPreVoteResp(true);

    private boolean resp;

    private MessageType(boolean resp) {
        this.resp = resp;
    }

    public boolean isResponse() {
        return resp;
    }

    public static MessageType voteResponseType(MessageType type) {
        switch (type) {
            case MsgVote:
                return MsgVoteResp;

            case MsgPreVote:
                return MsgPreVoteResp;

            default:
                throw new IllegalArgumentException("Not a vote request: " + type);
        }
    }
}
