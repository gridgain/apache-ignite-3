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
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.jetbrains.annotations.NotNull;

public class DistributedConfigurationStorage implements ConfigurationStorage {
    // TODO Distributed prefix. Will be replaced when ENUM with configuration type will be presented.
    // https://issues.apache.org/jira/browse/IGNITE-14476
    private static String DISTRIBUTED_PREFIX = "distributed";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DistributedConfigurationStorage.class);

    private static Key masterKey = new Key(DISTRIBUTED_PREFIX);

    private boolean watchRegistred;

    private final MetaStorageManager metaStorageMgr;

    private long appliedRevision;

    private long watchId;

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

        long maxRevision = 0L;

        Entry entryForMasterKey = null;

        for (Entry entry : cur) {
            if (!entry.key().equals(masterKey)) {
                data.put(entry.key().toString().replaceFirst(DISTRIBUTED_PREFIX + ".", ""),
                    (Serializable)SerializationUtils.fromBytes(entry.value()));

                if (maxRevision < entry.revision())
                    maxRevision = entry.revision();
            } else
                entryForMasterKey = entry;
        }

        assert entryForMasterKey != null;

        assert maxRevision == entryForMasterKey.revision();

        return new Data(data, maxRevision);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        try {
            version = metaStorageMgr.get(masterKey).get().revision();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Metastorage issue", e);

            return CompletableFuture.completedFuture(false);
        }

        if (sentVersion != version)
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            Key key = new Key(DISTRIBUTED_PREFIX + "." + entry.getKey());

            if (entry.getValue() != null)
                metaStorageMgr.put(key, SerializationUtils.toBytes(entry.getValue()));
            else
                metaStorageMgr.remove(key);
        }

        metaStorageMgr.put(masterKey, new byte[1]);

        // Seems that all of updates above must be done as one to ensure that all updates will be done with general revision.
        // We can do that for put using putAll, but we can't do that when value == null and we need to remove entry.
        // Also get for masterKey should be done in this update to ensure that all revision for this update equals to masterKey revision.

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);

        if (!watchRegistred) {
            try {
                watchId = metaStorageMgr.registerWatchByPrefix(masterKey, new WatchListener() {
                    @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                        HashMap<String, Serializable> data = new HashMap<>();

                        long maxRevision = 0L;

                        Entry entryForMasterKey = null;

                        for (WatchEvent event : events) {
                            Entry e = event.newEntry();

                            if (!e.key().equals(masterKey)) {
                                data.put(e.key().toString().replaceFirst(DISTRIBUTED_PREFIX + ".", ""),
                                    (Serializable)SerializationUtils.fromBytes(e.value()));

                                if (maxRevision < e.revision())
                                    maxRevision = e.revision();
                            } else
                                entryForMasterKey = e;
                        }

                        // Contract of metastorage ensures that all updates of one revision will come in one batch.
                        // Also masterKey should be updated every time when we update cfg.
                        // That means that masterKey update must be included in the batch.
                        assert entryForMasterKey != null;

                        assert maxRevision == entryForMasterKey.revision();

                        long finalMaxRevision = maxRevision;

                        listeners.forEach(listener -> listener.onEntriesChanged(new Data(data, finalMaxRevision)));

                        // Seems that we need to wait here until notifyApplied will be triggered.
                        // Anyway, it's out of scope now because notifyApplied is needed when restart will be developed.

                        return true;
                    }

                    @Override public void onError(@NotNull Throwable e) {
                        LOG.error("Metastorage listener issue", e);
                    }
                }).get();
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Metastorage listener issue", e);

                return;
            }

            watchRegistred = true;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);

        if (listeners.isEmpty())
            metaStorageMgr.unregisterWatch(watchId);
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
        assert appliedRevision <= storageRevision;

        appliedRevision = storageRevision;

        // Also we should persist appliedRevision,
        // this should be done in different ticket, when restart will be introduced.
    }
}
