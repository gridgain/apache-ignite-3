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

package org.apache.ignite.runner.internal.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.common.Value;
import org.apache.ignite.lang.util.SerializationUtils;

public class LocalConfigurationStorage implements ConfigurationStorage {
    // TODO Distributed prefix. Will be replaced when ENUM with configuration type will be presented.
    // https://issues.apache.org/jira/browse/IGNITE-14476
    private static String LOCAL_PREFIX = "local";

    private final VaultManager vaultMgr;

    public LocalConfigurationStorage(VaultManager vaultMgr) {
        this.vaultMgr = vaultMgr;
    }

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. */
    private AtomicLong version = new AtomicLong(0);

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        Iterator<Value> iter =
            vaultMgr.range(LOCAL_PREFIX + ".", LOCAL_PREFIX + (char)('.' + 1));

        HashMap<String, Serializable> data = new HashMap<>();

        while (iter.hasNext()) {
            Value val = iter.next();

            data.put(val.getKey().replaceFirst(LOCAL_PREFIX + ".", ""),
                (Serializable)SerializationUtils.fromBytes(val.value()));
        }

        // storage revision 0?
        return new Data(data, version.get(), 0);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        if (sentVersion != version.get())
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            String key = LOCAL_PREFIX + "." + entry.getKey();

            if (entry.getValue() != null)
                vaultMgr.put(key, new Value(key, SerializationUtils.toBytes(entry.getValue()), -1));
            else
                vaultMgr.remove(key);
        }

        version.incrementAndGet();

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version.get(), 0)));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
    }
}
