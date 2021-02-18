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

import java.util.concurrent.Future;
import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.raft.client.command.Command;
import org.apache.ignite.raft.client.command.CustomCommand;

public interface Server {
    // TODO sanpwc: In addition to isLeader, or maybe even instead of isLeader it's possible to implement sort of
    // TODO: getLeader() method.
    boolean isLeader();

    <T extends CustomCommand, R> Future<R> propose(T customCmd);

    // TODO sanpwc: tmp, should be removed after implementing proper communication.
    RawNode<Command> rawNode();
}