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

package org.apache.ignite.internal.vault.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.common.*;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Simple in-memory representation of vault. Only for test purposes.
 */
public class VaultServiceImpl implements VaultService {
    /** Map to store values. */
    private TreeMap<ByteArray, byte[]> storage = new TreeMap<>();

    private static ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    private long appliedRevision = 0;

    private final Object mux = new Object();

    private final WatcherImpl watcher;

    public VaultServiceImpl() {
        this.watcher = new WatcherImpl();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<VaultEntry> get(ByteArray key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(new VaultEntry(key, storage.get(key)));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Long> appliedRevision() {
        synchronized (mux) {
            return CompletableFuture.completedFuture(IgniteUtils.bytesToLong(storage.get(APPLIED_REV), 0));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> put(ByteArray key, byte[] val) {
        synchronized (mux) {
            storage.put(key, val);

            watcher.notify(new VaultEntry(key, val));

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> remove(ByteArray key) {
        synchronized (mux) {
            storage.remove(key);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<VaultEntry> range(ByteArray fromKey, ByteArray toKey) {
        synchronized (mux) {
            return new ArrayList<>(storage.subMap(fromKey, toKey).entrySet())
                .stream()
                .map(e -> new VaultEntry(e.getKey(), e.getValue()))
                .iterator();
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Watch watch) {
        synchronized (mux) {
            return watcher.register(watch);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        synchronized (mux) {
            watcher.cancel(id);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals, long revision) {
        synchronized (mux) {
            if (revision < appliedRevision)
                return CompletableFuture.allOf();

            storage.putAll(vals);

            this.appliedRevision = revision;

            return CompletableFuture.allOf();
        }
    }
}
