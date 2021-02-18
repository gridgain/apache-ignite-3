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

import java.util.UUID;

/**
 *
 */
public class HardState {
    private final long term;
    private final UUID vote;
    private final long committed;

    public HardState(long term, UUID vote, long committed) {
        this.term = term;
        this.vote = vote;
        this.committed = committed;
    }

    /**
     */
    public long term() {
        return term;
    }

    /**
     */
    public UUID vote() {
        return vote;
    }

    /**
     */
    public long committed() {
        return committed;
    }

    public boolean isEmpty() {
        return term == 0 && vote == null && committed == 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "HardState[" +
            "term=" + term +
            ", vote=" + vote +
            ", committed=" + committed +
            ']';
    }
}
