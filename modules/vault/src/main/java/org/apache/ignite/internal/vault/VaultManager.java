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
import org.apache.ignite.internal.vault.common.VaultEntry;
import org.apache.ignite.internal.vault.impl.VaultServiceImpl;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;

/**
 * Manager for operating with vault.
 */
public class VaultManager {
    private VaultService vaultService;

    public VaultManager() {
        this.vaultService = new VaultServiceImpl();
    }

    public boolean bootstrapped() {
        return false;
    }

    public CompletableFuture<VaultEntry> get(ByteArray key) {
        return vaultService.get(key);
    }

    public CompletableFuture<Void> put(ByteArray key, byte[] val) {
        return vaultService.put(key, val);
    }

    public CompletableFuture<Void> remove(ByteArray key) {
        return vaultService.remove(key);
    }

    public Iterator<VaultEntry> range(ByteArray fromKey, ByteArray toKey) {
        return vaultService.range(fromKey, toKey);
    }

    /**
     * See {@link VaultService#putAll}
     */
    @NotNull
    public CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals, long revision) {
        return vaultService.putAll(vals, revision);
    }

    /**
     * See {@link VaultService#appliedRevision()}
     */
    @NotNull
    public CompletableFuture<Long> appliedRevision() {
        return vaultService.appliedRevision();
    }
}
