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

package org.apache.ignite.internal.replication.raft.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.replication.raft.RawNode;
import org.apache.ignite.internal.replication.raft.message.Message;

// TODO sanpwc: Non thread safe! Better implementation required.
public class NaiveNetworkMock {

    private Map<UUID, RawNode> nodes = new HashMap<>();

    public NaiveNetworkMock() {
    }

    public void processMessage(Message msg) {
        nodes.get(msg.to()).step(msg);
    }

    public void addNodes(List<RawNode> nodes) {
        this.nodes = nodes.stream().collect(Collectors.toMap(e -> e.basicStatus().id(), e -> e));
    }
}
