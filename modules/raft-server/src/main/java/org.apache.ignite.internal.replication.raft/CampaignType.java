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
 *
 */
public enum CampaignType {
    /**
     * Represents the first phase of a normal election with pre-vote.
     */
    CAMPAIGN_PRE_ELECTION,

    /**
     * Represents a normal (time-based) election (the second phase of the election with pre-vote).
     */
    CAMPAIGN_ELECTION,

    /**
     * Represents the type of leader transfer.
     */
    CAMPAIGN_TRANSFER
}
