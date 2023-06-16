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

package org.apache.ignite.internal.deployunit.metastore;

import java.util.List;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;

/**
 * Listener of deployment unit node status changes.
 */
public interface NodeEventCallback {
    /**
     * Change event.
     *
     * @param status Deployment unit status.
     * @param holders Nodes consistent id.
     */
    default void onUpdate(UnitNodeStatus status, List<String> holders) {
        switch (status.status()) {
            case UPLOADING:
                onUploading(status, holders);
                break;
            case DEPLOYED:
                onDeploy(status, holders);
                break;
            case REMOVING:
                onRemoving(status, holders);
                break;
            case OBSOLETE:
                onObsolete(status, holders);
                break;
            default:
                break;
        }
    }

    default void onUploading(UnitNodeStatus status, List<String> holders) {

    }

    default void onDeploy(UnitNodeStatus status, List<String> holders) {

    }

    default void onObsolete(UnitNodeStatus status, List<String> holders) {

    }

    default void onRemoving(UnitNodeStatus status, List<String> holders) {

    }
}
