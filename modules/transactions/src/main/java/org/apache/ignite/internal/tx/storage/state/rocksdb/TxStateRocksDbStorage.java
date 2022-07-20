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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_CREATE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_DESTROY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

public class TxStateRocksDbStorage implements TxStateStorage, AutoCloseable {
    private final Path dbPath;

    private volatile TransactionDB db;

    /** RockDB options. */
    @Nullable
    private volatile Options options;

    private volatile TransactionDBOptions txDbOptions;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor;

    private volatile RocksSnapshotManager snapshotManager;

    private final Object snapshotRestoreLock = new Object();

    private boolean isStarted;

    public TxStateRocksDbStorage(Path dbPath, ExecutorService snapshotExecutor) {
        this.dbPath = dbPath;
        this.snapshotExecutor = snapshotExecutor;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            this.options = new Options().setCreateIfMissing(true);

            this.txDbOptions = new TransactionDBOptions();

            this.db = TransactionDB.open(options, txDbOptions, dbPath.toString());

            ColumnFamily defaultCf = ColumnFamily.wrap(db, db.getDefaultColumnFamily());

            snapshotManager = new RocksSnapshotManager(db, List.of(fullRange(defaultCf)), snapshotExecutor);

            isStarted = true;
        } catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_CREATE_ERR, "Failed to start the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isStarted() {
        return isStarted;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        IgniteUtils.closeAll(options, txDbOptions, db);

        db = null;
        options = null;
        txDbOptions = null;

        isStarted = false;
    }

    /** {@inheritDoc} */
    @Override public TxMeta get(UUID txId) {
        try {
            byte[] txMetaBytes = db.get(toBytes(txId));

            return txMetaBytes == null ? null : (TxMeta) fromBytes(txMetaBytes);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UUID txId, TxMeta txMeta) {
        try {
            db.put(toBytes(txId), toBytes(txMeta));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(UUID txId, @NotNull TxState txStateExpected, @NotNull TxMeta txMeta) {
        requireNonNull(txStateExpected);
        requireNonNull(txMeta);

        byte[] txIdBytes = toBytes(txId);

        try (Transaction rocksTx = db.beginTransaction(new WriteOptions())) {
            byte[] txMetaExistingBytes = rocksTx.get(new ReadOptions(), toBytes(txId));
            TxMeta txMetaExisting = (TxMeta) fromBytes(txMetaExistingBytes);

            if (txMetaExisting.txState() == txStateExpected) {
                rocksTx.put(txIdBytes, toBytes(txMeta));

                rocksTx.commit();

                return true;
            } else {
                rocksTx.rollback();

                return false;
            }
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(UUID txId) {
        try {
            db.delete(toBytes(txId));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        try (Options options = new Options()) {
            close();

            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (Exception e) {
            throw new IgniteInternalException(TX_STATE_STORAGE_DESTROY_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path snapshotPath) {
        synchronized (snapshotRestoreLock) {
            destroy();

            start();

            snapshotManager.restoreSnapshot(snapshotPath);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        stop();
    }
}
