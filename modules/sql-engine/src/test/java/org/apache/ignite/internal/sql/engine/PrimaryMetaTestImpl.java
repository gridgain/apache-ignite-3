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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;

/** Test implementation of the {@link ReplicaMeta} */
public class PrimaryMetaTestImpl implements ReplicaMeta {
    private static final long serialVersionUID = -382174507405586033L;

    /** A node that holds a lease. */
    private final String leaseholder;

    /** Lease start timestamp. The timestamp is assigned when the lease created and is not changed when the lease is prolonged. */
    private final HybridTimestamp startTime;

    /** Timestamp to expiration the lease. */
    private final HybridTimestamp expirationTime;

    /**
     * Creates a new primary meta.
     *
     * @param leaseholder Lease holder.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     */
    public PrimaryMetaTestImpl(
            String leaseholder,
            HybridTimestamp startTime,
            HybridTimestamp leaseExpirationTime
    ) {
        this.leaseholder = leaseholder;
        this.startTime = startTime;
        this.expirationTime = leaseExpirationTime;
    }

    @Override
    public String getLeaseholder() {
        return leaseholder;
    }

    @Override
    public HybridTimestamp getStartTime() {
        return startTime;
    }

    @Override
    public HybridTimestamp getExpirationTime() {
        return expirationTime;
    }
}
