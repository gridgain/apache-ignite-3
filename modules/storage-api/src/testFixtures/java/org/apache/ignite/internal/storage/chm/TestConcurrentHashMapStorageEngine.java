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

package org.apache.ignite.internal.storage.chm;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;

/**
 * Test implementation of the {@link StorageEngine} based on class {@link ConcurrentHashMap}.
 */
public class TestConcurrentHashMapStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "test_chm";

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public MvTableStorage createMvTable(TableConfiguration tableCfg) throws StorageException {
        assert tableCfg.dataStorage().name().value().equals(ENGINE_NAME) : tableCfg.dataStorage().name().value();

        return new TestConcurrentHashMapMvTableStorage(tableCfg);
    }

    /** {@inheritDoc} */
    @Override public TxStateTableStorage createTxnStateTableStorage(TableConfiguration tableCfg) {
        throw new UnsupportedOperationException("Transaction state storage can be created only using RocksDb storage engine.");
    }
}
