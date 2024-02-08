/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Utilities for working with the catalog in tests. */
public class CatalogTestUtils {
    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(nodeName));

        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public CompletableFuture<Void> start() {
                return allOf(metastore.start(), clockWaiter.start(), super.start()).thenCompose(unused -> metastore.deployWatches());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
                metastore.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
                metastore.stop();
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clockWaiter Clock waiter.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, ClockWaiter clockWaiter) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(nodeName));

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public CompletableFuture<Void> start() {
                return allOf(metastore.start(), super.start()).thenCompose(unused -> metastore.deployWatches());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                metastore.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                metastore.stop();
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock, MetaStorageManager metastore) {
        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter) {
            @Override
            public CompletableFuture<Void> start() {
                return allOf(clockWaiter.start(), super.start());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManagerWithInterceptor(
            String nodeName,
            HybridClock clock,
            MetaStorageManager metastore,
            UpdateHandlerInterceptor interceptor
    ) {
        var clockWaiter = new ClockWaiter(nodeName, clock);

        UpdateLogImpl updateLog = new UpdateLogImpl(metastore) {
            @Override
            public void registerUpdateHandler(OnUpdateHandler handler) {
                interceptor.registerUpdateHandler(handler);

                super.registerUpdateHandler(interceptor);
            }
        };


        return new CatalogManagerImpl(updateLog, clockWaiter) {
            @Override
            public CompletableFuture<Void> start() {
                return allOf(clockWaiter.start(), super.start());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
            }
        };
    }

    /**
     * Create the same {@link CatalogManager} as for normal operations, but with {@link UpdateLog} that
     * simply notifies the manager without storing any updates in metastore.
     *
     * <p>Particular configuration of manager pretty fast (in terms of awaiting of certain safe time) and lightweight.
     * It doesn't contain any mocks from {@link org.mockito.Mockito}.
     *
     * @param nodeName Name of the node that is meant to own this manager. Any thread spawned by returned instance
     *      will have it as thread's name prefix.
     * @param clock This clock is used to assign activation timestamp for incoming updates, thus make it possible
     *      to acquired schema that was valid at give time.
     * @return An instance of {@link CatalogManager catalog manager}.
     */
    public static CatalogManager createCatalogManagerWithTestUpdateLog(String nodeName, HybridClock clock) {
        var clockWaiter = new ClockWaiter(nodeName, clock);

        return new CatalogManagerImpl(new TestUpdateLog(clock), clockWaiter) {
            @Override
            public CompletableFuture<Void> start() {
                return allOf(clockWaiter.start(), super.start());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public void stop() throws Exception {
                super.stop();

                clockWaiter.stop();
            }
        };
    }

    /** Default nullable behavior. */
    public static final boolean DEFAULT_NULLABLE = false;

    /** Append precision\scale according to type requirement. */
    public static Builder initializeColumnWithDefaults(ColumnType type, Builder colBuilder) {
        if (type.precisionAllowed()) {
            colBuilder.precision(11);
        }

        if (type.scaleAllowed()) {
            colBuilder.scale(0);
        }

        if (type.lengthAllowed()) {
            colBuilder.length(1 << 5);
        }

        return colBuilder;
    }

    /** Append precision according to type requirement. */
    public static void applyNecessaryPrecision(ColumnType type, Builder colBuilder) {
        if (type.precisionAllowed()) {
            colBuilder.precision(11);
        }
    }

    /** Append length according to type requirement. */
    public static void applyNecessaryLength(ColumnType type, Builder colBuilder) {
        if (type.lengthAllowed()) {
            colBuilder.length(1 << 5);
        }
    }

    static ColumnParams columnParams(String name, ColumnType type) {
        return columnParams(name, type, DEFAULT_NULLABLE);
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable) {
        return columnParamsBuilder(name, type, nullable).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision) {
        return columnParamsBuilder(name, type, nullable, precision).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return columnParamsBuilder(name, type, nullable, precision, scale).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, int length, boolean nullable) {
        return columnParamsBuilder(name, type, length, nullable).build();
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type) {
        return columnParamsBuilder(name, type, DEFAULT_NULLABLE);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision).scale(scale);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, int length, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).length(length);
    }

    static CatalogCommand dropTableCommand(String tableName) {
        return DropTableCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).build();
    }

    static CatalogCommand dropColumnParams(String tableName, String... columns) {
        return AlterTableDropColumnCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).columns(Set.of(columns)).build();
    }

    static CatalogCommand addColumnParams(String tableName, ColumnParams... columns) {
        return AlterTableAddColumnCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).columns(List.of(columns)).build();
    }

    public static CreateZoneCommandBuilder createZoneBuilder(String zoneName) {
        return CreateZoneCommand.builder().zoneName(zoneName);
    }

    public static AlterZoneCommandBuilder alterZoneBuilder(String zoneName) {
        return AlterZoneCommand.builder().zoneName(zoneName);
    }

    /**
     * Starts catalog compaction and waits it finished locally.
     *
     * @param catalogManager Catalog manager.
     * @param timestamp Timestamp catalog should be compacted up to.
     * @return {@code True} if a new snapshot has been successfully written, {@code false} otherwise.
     */
    static boolean waitCatalogCompaction(CatalogManager catalogManager, long timestamp) {
        int version = catalogManager.activeCatalogVersion(timestamp);

        CompletableFuture<Boolean> operationFuture = ((CatalogManagerImpl) catalogManager).compactCatalog(timestamp);

        try {
            boolean result = operationFuture.get();

            if (result) {
                waitForCondition(() -> catalogManager.earliestCatalogVersion() == version, 3_000);
            }
        } catch (Exception e) {
            fail(e);
        }

        return operationFuture.join();
    }

    private static class TestUpdateLog implements UpdateLog {
        private final HybridClock clock;

        private long lastSeenVersion = 0;
        private long snapshotVersion = 0;

        private volatile OnUpdateHandler onUpdateHandler;

        private TestUpdateLog(HybridClock clock) {
            this.clock = clock;
        }

        @Override
        public synchronized CompletableFuture<Boolean> append(VersionedUpdate update) {
            if (update.version() - 1 != lastSeenVersion) {
                return falseCompletedFuture();
            }

            lastSeenVersion = update.version();

            return onUpdateHandler.handle(update, clock.now(), update.version()).thenApply(ignored -> true);
        }

        @Override
        public synchronized CompletableFuture<Boolean> saveSnapshot(SnapshotEntry snapshotEntry) {
            snapshotVersion = snapshotEntry.version();
            return trueCompletedFuture();
        }

        @Override
        public void registerUpdateHandler(OnUpdateHandler handler) {
            this.onUpdateHandler = handler;
        }

        @Override
        public CompletableFuture<Void> start() throws IgniteInternalException {
            if (onUpdateHandler == null) {
                throw new IgniteInternalException(
                        Common.INTERNAL_ERR,
                        "Handler must be registered prior to component start"
                );
            }

            lastSeenVersion = snapshotVersion;

            return nullCompletedFuture();
        }

        @Override
        public void stop() throws Exception {

        }
    }

    /**
     * Searches for a table by name in the requested version of the catalog.
     *
     * @param catalogService Catalog service.
     * @param catalogVersion Catalog version in which to find the table.
     * @param tableName Table name.
     */
    public static CatalogTableDescriptor table(CatalogService catalogService, int catalogVersion, String tableName) {
        CatalogTableDescriptor tableDescriptor = catalogService.tables(catalogVersion).stream()
                .filter(table -> tableName.equals(table.name()))
                .findFirst()
                .orElse(null);

        assertNotNull(tableDescriptor, "catalogVersion=" + catalogVersion + ", tableName=" + tableName);

        return tableDescriptor;
    }

    /**
     * Searches for an index by name in the requested version of the catalog. Throws if the index is not found.
     *
     * @param catalogService Catalog service.
     * @param catalogVersion Catalog version in which to find the index.
     * @param indexName Index name.
     * @return Index (cannot be null).
     */
    public static CatalogIndexDescriptor index(CatalogService catalogService, int catalogVersion, String indexName) {
        CatalogIndexDescriptor indexDescriptor = indexOrNull(catalogService,
                catalogVersion, indexName);

        assertNotNull(indexDescriptor, "catalogVersion=" + catalogVersion + ", indexName=" + indexName);

        return indexDescriptor;
    }

    /**
     * Searches for an index by name in the requested version of the catalog.
     *
     * @param catalogService Catalog service.
     * @param catalogVersion Catalog version in which to find the index.
     * @param indexName Index name.
     * @return Index or {@code null} if not found.
     */
    @Nullable
    public static CatalogIndexDescriptor indexOrNull(CatalogService catalogService, int catalogVersion, String indexName) {
        return catalogService.indexes(catalogVersion).stream()
                .filter(index -> indexName.equals(index.name()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Update handler interceptor for test pusposes.
     */
    public abstract static class UpdateHandlerInterceptor implements OnUpdateHandler {
        private OnUpdateHandler delegate;

        void registerUpdateHandler(OnUpdateHandler handler) {
            this.delegate = handler;
        }

        protected OnUpdateHandler delegate() {
            return delegate;
        }
    }
}
