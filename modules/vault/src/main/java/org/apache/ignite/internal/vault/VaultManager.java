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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.vault.common.Value;
import org.apache.ignite.internal.vault.service.VaultService;
import org.jetbrains.annotations.NotNull;

public class VaultManager {
    private VaultService vaultService;

    public VaultManager(VaultService vaultService) {
        this.vaultService = vaultService;
    }

    public boolean bootstrapped() {
        return false;
    }

    public CompletableFuture<Value> get(String key) {
        return vaultService.get(key);
    }

    public CompletableFuture<Long> appliedRevision(String key) {
        return vaultService.appliedRevision(key);
    }

    public CompletableFuture<Void> put(String key, Value val) {
        return vaultService.put(key, val);
    }

    public CompletableFuture<Void> remove(String key) {
        return vaultService.remove(key);
    }

    public Iterator<Value> range(String fromKey, String toKey) {
        return vaultService.range(fromKey, toKey);
    }

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
}
