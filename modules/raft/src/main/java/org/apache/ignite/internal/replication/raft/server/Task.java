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

import org.apache.ignite.internal.replication.raft.common.ExceptionableFutureTask;
import org.apache.ignite.lang.IgniteUuid;

public class Task<T> {
    private final IgniteUuid id;

    private final T data;

    private final ExceptionableFutureTask res;

    private final boolean fastTrack;

    public Task(IgniteUuid id, T data, ExceptionableFutureTask res, boolean fastTrack) {
        this.id = id;
        this.data = data;
        this.res = res;
        this.fastTrack = fastTrack;
    }

    /**
     * @return User data.
     */
    public T data() {
        return data;
    }

    /**
     * @return Result future.
     */
    public ExceptionableFutureTask result() {
        return res;
    }

    /**
     * @return Id.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Fast track.
     */
    public boolean fastTrack() {
        return fastTrack;
    }
}
