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

package org.apache.ignite.internal.placementdriver;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/** Replica lease meta. */
public interface ReplicaMeta {
    /** Gets a leaseholder node consistent ID (assigned to a node once), {@code null} if nothing holds the lease. */
    @Nullable String getLeaseholder();

    /** Gets a leaseholder node ID (changes on every node startup), {@code null} if nothing holds the lease. */
    @Nullable UUID getLeaseholderId();

    /** Gets a lease start timestamp. */
    HybridTimestamp getStartTime();

    /** Gets a lease expiration timestamp. */
    HybridTimestamp getExpirationTime();
}
