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

package org.apache.ignite.raft.client.message;

import java.io.Serializable;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;

/**
 * Submit an action to a replication group.
 */
public interface ActionRequest<T> extends NetworkMessage, Serializable {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return Action's command.
     */
    Command command();

    /** */
    public interface Builder {
        /**
         * @param cmd Action's command.
         * @return The builder.
         */
        Builder command(Command cmd);

        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        ActionRequest build();
    }
}
