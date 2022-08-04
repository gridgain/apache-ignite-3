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

package org.apache.ignite.internal.replicator.message;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the replication process.
 */
@MessageGroup(groupType = 8, groupName = "ReplicaMessages")
public class ReplicaMessageGroup {
    /**
     * Message type for {@link WaiteOperationsResultRequest}.
     */
    public static final short COMPLETE_OP_REQUEST = 0;

    /** Message type for {@link CleanupRequest}. */
    public static final short CLEANUP_REQUEST = 1;

    /** Message type for {@link ErrorReplicaResponse}. */
    public static final short ERROR_REPLICA_RESPONSE = 2;

    /** Message type for {@link InstantResponse}. */
    public static final short INSTANT_RESPONSE = 3;

    /** Message type for {@link FutureResponse}. */
    public static final short FUTURE_RESPONSE = 4;

    /** Message type for {@link CompoundResponse}. */
    public static final short COMPOUND_RESPONSE = 5;
}
