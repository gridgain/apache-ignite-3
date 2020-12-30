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
 * The exception is thrown when Raft encounters a violation of an internal invariant. When caught, the Raft
 * instance must be no longer used. A new instance of Raft must be restored from the most recent hard state
 * stored on disk.
 */
public class UnrecoverableException extends RuntimeException {
    public UnrecoverableException(String message) {
        super(message);
    }

    public UnrecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableException(Throwable cause) {
        super(cause);
    }
}
