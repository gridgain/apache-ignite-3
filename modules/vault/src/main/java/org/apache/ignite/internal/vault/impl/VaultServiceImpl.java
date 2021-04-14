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

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.vault.common.*;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Simple in-memory representation of vault. Only for test purposes.
 */
public class VaultServiceImpl implements VaultService {
    /** Map to store values. */
    private TreeMap<String, Value> storage = new TreeMap<>();

    private final Object mux = new Object();

    private final WatcherImpl watcher;

    public VaultServiceImpl() {
        this.watcher = new WatcherImpl();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Value> get(String key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(storage.get(key));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Long> appliedRevision(String key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(storage.get(key).getRevision());
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> put(String key, Value val) {
        synchronized (mux) {
            if (storage.containsKey(key) && storage.get(key).getRevision() != -1 && storage.get(key).getRevision() >= val.getRevision())
                return CompletableFuture.allOf();

            storage.put(key, val);

            watcher.notify(val);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> remove(String key) {
        synchronized (mux) {
            storage.remove(key);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Value> range(String fromKey, String toKey) {
        synchronized (mux) {
            return storage.subMap(fromKey, toKey).values().iterator();
        }
    }

    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Watch watch) {
        return watcher.register(watch);
    }

    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        watcher.cancel(id);

        return CompletableFuture.allOf();
    }
}
