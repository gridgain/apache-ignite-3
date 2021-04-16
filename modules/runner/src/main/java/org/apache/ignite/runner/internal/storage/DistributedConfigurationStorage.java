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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.internal.MetaStorageManager;

public class DistributedConfigurationStorage implements ConfigurationStorage {
    // TODO Distributed prefix. Will be replaced when ENUM with configuration type will be presented.
    // https://issues.apache.org/jira/browse/IGNITE-14476
    private static String DISTRIBUTED_PREFIX = "distributed";

    private final MetaStorageManager metaStorageMgr;

    public DistributedConfigurationStorage(MetaStorageManager metaStorageMgr) {
        this.metaStorageMgr = metaStorageMgr;
    }

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. */
    private Long version;

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {

        Cursor<Entry> cur = metaStorageMgr.range(new Key(DISTRIBUTED_PREFIX + "."), new Key(DISTRIBUTED_PREFIX + (char)('.' + 1)));

        HashMap<String, Serializable> data = new HashMap<>();

        for (Entry entry : cur)
            data.put(entry.key().toString().replaceFirst(DISTRIBUTED_PREFIX + ".", ""), (Serializable)SerializationUtils.fromBytes(entry.value()));

        // storage revision 0?
        return new Data(data, version, 0);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        if (sentVersion != version)
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            Key key = new Key(DISTRIBUTED_PREFIX + "." + entry.getKey());

            if (entry.getValue() != null)
                metaStorageMgr.put(key, SerializationUtils.toBytes(entry.getValue()));
            else
                metaStorageMgr.remove(key);
        }

        version++;

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version, 0)));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
        metaStorageMgr.watchByP()
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
    }
}
