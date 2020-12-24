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

package org.apache.ignite.internal.replication.raft.storage;

import java.util.List;

/**
 * ConfChangeTransition specifies the behavior of a configuration change with
 * respect to joint consensus.
 */
public interface ConfChange extends LogData {
    public enum ConfChangeTransition {
        // Automatically use the simple protocol if possible, otherwise fall back
        // to ConfChangeJointImplicit. Most applications will want to use this.
        ConfChangeTransitionAuto,

        // Use joint consensus unconditionally, and transition out of them
        // automatically (by proposing a zero configuration change).
        //
        // This option is suitable for applications that want to minimize the time
        // spent in the joint configuration and do not store the joint configuration
        // in the state machine (outside of InitialState).
        ConfChangeTransitionJointImplicit,

        // Use joint consensus and remain in the joint configuration until the
        // application proposes a no-op configuration change. This is suitable for
        // applications that want to explicitly control the transitions, for example
        // to use a custom payload (via the Context field).
        ConfChangeTransitionJointExplicit
    }

    public ConfChangeTransition transition();

    public List<ConfChangeSingle> changes();
}
