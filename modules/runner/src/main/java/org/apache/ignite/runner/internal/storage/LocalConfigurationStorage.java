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
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.common.VaultEntry;
import org.apache.ignite.lang.ByteArray;
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
    private Long version;

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        Iterator<VaultEntry> iter =
            vaultMgr.range(ByteArray.fromString(LOCAL_PREFIX + "."), ByteArray.fromString(LOCAL_PREFIX + (char)('.' + 1)));

        HashMap<String, Serializable> data = new HashMap<>();

        while (iter.hasNext()) {
            VaultEntry val = iter.next();

            data.put(val.key().toString().replaceFirst(LOCAL_PREFIX + ".", ""),
                (Serializable)SerializationUtils.fromBytes(val.value()));
        }

        return new Data(data, version);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        if (sentVersion != version)
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            ByteArray key = ByteArray.fromString(LOCAL_PREFIX + "." + entry.getKey());

            if (entry.getValue() != null)
                vaultMgr.put(key, SerializationUtils.toBytes(entry.getValue()));
            else
                vaultMgr.remove(key);
        }

        version++;

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version)));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
    }
}
