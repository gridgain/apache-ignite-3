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

package org.apache.ignite.internal.vault;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;

public class VaultManager {
    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @param appliedRevision Revision for entries
     * @return Completed future.
     */
    @NotNull
    CompletableFuture<Void> putAll(@NotNull Map<String, byte[]> vals, long appliedRevision) {
        return CompletableFuture.allOf();
    }

    /**
     * @return Applied revision for {@link VaultManager#putAll} operation.
     */
    @NotNull
    CompletableFuture<Long> appliedRevision() {
        return CompletableFuture.completedFuture(0L);
    }

    public boolean bootstrapped() {
        return false;
    }
}
