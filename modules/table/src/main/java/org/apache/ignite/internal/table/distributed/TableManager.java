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

package org.apache.ignite.internal.table.distributed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent, TableEventParameters> implements IgniteTables, IgniteTablesInternal, IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManager.class);
    // TODO sanpwc: Consider using list.size + 1 instead.
    private final int INITIAL_SCHEMA_VERSION = 1;

    private static final IgniteUuidGenerator TABLE_ID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Node configuration manager. */
    private final ConfigurationManager nodeCfgMgr;

    /** Cluster configuration manager. */
    private final ConfigurationManager clusterCfgMgr;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Affinity manager. */
    private final AffinityManager affMgr;

    /** Partitions store directory. */
    private final Path partitionsStoreDir;

    // TODO sanpwc: Consider combinig tables and tablesById.
    /** Tables. */
    private final Map<String, TableImpl> tables = new ConcurrentHashMap<>();

    /** Tables. */
    private final Map<IgniteUuid, TableImpl> tablesById = new ConcurrentHashMap<>();

    /**
     * Creates a new table manager.
     *
     * @param nodeCfgMgr Node configuration manager.
     * @param clusterCfgMgr Cluster configuration manager.
     * @param schemaMgr Schema manager.
     * @param affMgr Affinity manager.
     * @param raftMgr Raft manager.
     * @param partitionsStoreDir Partitions store directory.
     */
    public TableManager(
        ConfigurationManager nodeCfgMgr,
        ConfigurationManager clusterCfgMgr,
        SchemaManager schemaMgr,
        AffinityManager affMgr,
        Loza raftMgr,
        Path partitionsStoreDir
    ) {
        this.nodeCfgMgr = nodeCfgMgr;
        this.clusterCfgMgr = clusterCfgMgr;
        this.affMgr = affMgr;
        this.raftMgr = raftMgr;
        this.schemaMgr = schemaMgr;
        this.partitionsStoreDir = partitionsStoreDir;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().listenElements(new ConfigurationNamedListListener<TableView>() {
            @Override
            public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<TableView> ctx) {
                // Empty assignments might be a valid case if tables are created from within cluster init HOCON
                // configuration, which is not supported now.
                assert ((ExtendedTableView)ctx.newValue()).assignments() != null :
                    "Table =[" + ctx.newValue().name() + "] has empty assignments.";

                // TODO: IGNITE-15409 Listener with any placeholder should be used instead.
                ((ExtendedTableConfiguration) clusterCfgMgr.configurationRegistry().
                    getConfiguration(TablesConfiguration.KEY).tables().get(ctx.newValue().name())).schemas().
                    listenElements(new ConfigurationNamedListListener<>() {
                        @Override public @NotNull CompletableFuture<?> onCreate(
                            @NotNull ConfigurationNotificationEvent<SchemaView> schemasCtx) {
                            // TODO sanpwc: Check whether it's safe to use name.
                            // TODO sanpwc: consider adding this method to schema registry api.
                            ((SchemaRegistryImpl)tables.get(ctx.newValue().name()).schemaRegistry()).
                                onSchemaRegistered((SchemaDescriptor) ByteUtils.fromBytes(schemasCtx.newValue().schema()));

                            // TODO sanpwc: Check whether tablesById is backed or we should update schema within it explictly.
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public @NotNull CompletableFuture<?> onRename(@NotNull String oldName, @NotNull String newName,
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override public @NotNull CompletableFuture<?> onDelete(
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override public @NotNull CompletableFuture<?> onUpdate(
                            @NotNull ConfigurationNotificationEvent<SchemaView> ctx) {
                            return CompletableFuture.completedFuture(null);
                        }
                    });

                    createTableLocally(
                    ctx.newValue().name(),
                    IgniteUuid.fromString (((ExtendedTableView)ctx.newValue()).id()),
                    (List<List<ClusterNode>>)ByteUtils.fromBytes(((ExtendedTableView)ctx.newValue()).assignments()),
                    (SchemaDescriptor) ByteUtils.fromBytes(((ExtendedTableView)ctx.newValue()).schemas().get( String.valueOf(INITIAL_SCHEMA_VERSION)).schema())
                );

                return CompletableFuture.completedFuture(null);
            }

            @Override public @NotNull CompletableFuture<?> onRename(@NotNull String oldName, @NotNull String newName,
                @NotNull ConfigurationNotificationEvent<TableView> ctx) {
                // No-op.

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<TableView> ctx) {
                dropTableLocally(
                    ctx.oldValue().name(),
                    IgniteUuid.fromString (((ExtendedTableView)ctx.oldValue()).id()),
                    (List<List<ClusterNode>>)ByteUtils.fromBytes(((ExtendedTableView)ctx.oldValue()).assignments())
                );

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<TableView> ctx) {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
    }

    /**
     * Creates local structures for a table.
     *
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void createTableLocally(
        String name,
        IgniteUuid tblId,
        List<List<ClusterNode>> assignment,
        SchemaDescriptor schemaDescriptor
    ) {
        int partitions = assignment.size();

        var partitionsGroupsFutures = new ArrayList<CompletableFuture<RaftGroupService>>();

        IntStream.range(0, partitions).forEach(p ->
            partitionsGroupsFutures.add(
                raftMgr.prepareRaftGroup(
                    raftGroupName(tblId, p),
                    assignment.get(p),
                    () -> {
                        // TODO sanpwc: Use table id instead.
                        Path storageDir = partitionsStoreDir.resolve(name);

                        try {
                            Files.createDirectories(storageDir);
                        }
                        catch (IOException e) {
                            throw new IgniteInternalException(
                                "Failed to create partitions store directory for " + name + ": " + e.getMessage(),
                                e
                            );
                        }

                        return new PartitionListener(
                            new RocksDbStorage(
                                storageDir.resolve(String.valueOf(p)),
                                ByteBuffer::compareTo
                            )
                        );
                    }
                )
            )
        );

        CompletableFuture.allOf(partitionsGroupsFutures.toArray(CompletableFuture[]::new)).thenRun(() -> {
            HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

            for (int p = 0; p < partitions; p++) {
                CompletableFuture<RaftGroupService> future = partitionsGroupsFutures.get(p);

                assert future.isDone();

                RaftGroupService service = future.join();

                partitionMap.put(p, service);
            }

            InternalTableImpl internalTable = new InternalTableImpl(name, tblId, partitionMap, partitions);

            var schemaRegistry = new SchemaRegistryImpl(v -> schemaDescriptor);

            // TODO sapwc: wrap with try catch that will triger listener with exectpion.
            schemaRegistry.onSchemaRegistered(schemaDescriptor);

            var table = new TableImpl(
                internalTable,
                schemaRegistry,
                TableManager.this,
                null
            );

            tables.put(name, table);
            tablesById.put(tblId, table);

            onEvent(TableEvent.CREATE, new TableEventParameters(table), null);
        });
    }

    /**
     * Drops local structures for a table.
     *
     * @param name Table name.
     * @param tblId Table id.
     * @param assignment Affinity assignment.
     */
    private void dropTableLocally(String name, IgniteUuid tblId, List<List<ClusterNode>> assignment) {
        int partitions = assignment.size();

        for (int p = 0; p < partitions; p++)
            raftMgr.stopRaftGroup(raftGroupName(tblId, p), assignment.get(p));

        TableImpl table = tables.get(name);

        assert table != null : "There is no table with the name specified [name=" + name + ']';

        onEvent(TableEvent.DROP, new TableEventParameters(table), null);
    }

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tblId Table identifier.
     * @param partition Number of table partitions.
     * @return A RAFT group name.
     */
    @NotNull private String raftGroupName(IgniteUuid tblId, int partition) {
        return tblId + "_part_" + partition;
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange, true);
    }

    /** {@inheritDoc} */
    @Override public Table getOrCreateTable(String name, Consumer<TableChange> tableInitChange) {
        return getOrCreateTableAsync(name, tableInitChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> getOrCreateTableAsync(String name, Consumer<TableChange> tableInitChange) {
        return createTableAsync(name, tableInitChange, false);
    }

    /**
     * Creates a new table with the specified name or returns an existing table with the same name.
     *
     * @param name Table name.
     * @param tableInitChange Table configuration.
     * @param exceptionWhenExist If the value is {@code true}, an exception will be thrown when the table already exists,
     * {@code false} means the existing table will be returned.
     * @return A table instance.
     */
    private CompletableFuture<Table> createTableAsync(
        String name, Consumer<TableChange> tableInitChange,
        boolean exceptionWhenExist
    ) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null)
                    tblFut.complete(parameters.table());
                else
                    tblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                tblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tableAsync(name, true).thenAccept(tbl -> {
            if (tbl != null) {
                if (exceptionWhenExist) {
                    removeListener(TableEvent.CREATE, clo, new IgniteInternalCheckedException(
                            LoggerMessageHelper.format("Table already exists [name={}]", name)));
                } else if (tblFut.complete(tbl))
                    removeListener(TableEvent.CREATE, clo);
            } else {
                try {
                    clusterCfgMgr
                        .configurationRegistry()
                        .getConfiguration(TablesConfiguration.KEY)
                        .tables()
                        .change(
                            change -> change.create(
                                name,
                                (ch) -> {
                                    tableInitChange.accept(ch);
                                    ((ExtendedTableChange)ch).
                                        changeId(TABLE_ID_GENERATOR.randomUuid().toString()).
                                        changeAssignments(
                                            ByteUtils.toBytes(
                                                affMgr.calculateAssignments(ch.partitions(), ch.replicas())
                                            )
                                        ).
                                        changeSchemas(
                                            schemasCh -> schemasCh.create(
                                                String.valueOf(INITIAL_SCHEMA_VERSION),
                                                schemaCh -> schemaCh.changeSchema(
                                                    ByteUtils.toBytes(
                                                        schemaMgr.prepareSchemaDescriptor(ch)
                                                    )
                                                )
                                            )
                                        );
                                }
                            )
                        )
                        .get();
                    // TODO sanpwc: Consider using exceptionally here instead of synchronous waite.
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Table wasn't created [name=" + name + ']', e);

                    removeListener(TableEvent.CREATE, clo, new IgniteInternalCheckedException(e));
                }
            }
        });

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override public void alterTable(String name, Consumer<TableChange> tableChange) {
        alterTableAsync(name, tableChange).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        CompletableFuture<Void> tblFut = new CompletableFuture<>();

        listen(TableEvent.ALTER, new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null)
                    tblFut.complete(null);
                else
                    tblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                tblFut.completeExceptionally(e);
            }
        });

        try {
            clusterCfgMgr.configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY).tables().change(ch -> {
                ch.createOrUpdate(name, tableChange);

                ch.createOrUpdate(name, tblCh -> {
                    ((ExtendedTableChange)tblCh).changeSchemas(
                        schemasCh -> {
                            schemasCh.createOrUpdate(
                                String.valueOf(schemasCh.size() + 1),
                                schemaCh -> schemaCh.changeSchema(
                                    ByteUtils.toBytes(
                                        schemaMgr.prepareSchemaDescriptor(tblCh)
                                    )
                                )
                            );
                        }
                    );
                });
            }).get();

        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Table wasn't created [name=" + name + ']', e);

            tblFut.completeExceptionally(e);
        }

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        dropTableAsync(name).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> dropTableAsync(String name) {
        // TODO sanpwc: review and refactor if necessary.
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null) {
                    TableImpl droppedTable = tables.remove(tableName);

                    assert droppedTable != null;

                    tablesById.remove(droppedTable.tableId());

                    dropTblFut.complete(null);
                }
                else
                    dropTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                dropTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.DROP, clo);

        if (!isTableConfigured(name)) {
            if (dropTblFut.complete(null))
                removeListener(TableEvent.DROP, clo, null);
        }
        else {
            try {
                clusterCfgMgr
                    .configurationRegistry()
                    .getConfiguration(TablesConfiguration.KEY)
                    .tables()
                    .change(change -> change.delete(name))
                    .get();
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Table wasn't dropped [name=" + name + ']', e);

                removeListener(TableEvent.DROP, clo, new IgniteInternalCheckedException(e));
            }
        }

        return dropTblFut;
    }

    /** {@inheritDoc} */
    @Override public List<Table> tables() {
        return tablesAsync().join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<List<Table>> tablesAsync() {
        var tableNames = tableNamesConfigured();
        var tableFuts = new CompletableFuture[tableNames.size()];
        var i = 0;

        for (String tblName : tableNames)
            tableFuts[i++] = tableAsync(tblName, false);

        return CompletableFuture.allOf(tableFuts).thenApply(unused -> {
            var tables = new ArrayList<Table>(tableNames.size());

            try {
                for (var fut : tableFuts) {
                    var table = fut.get();

                    if (table != null)
                        tables.add((Table) table);
                }
            } catch (Throwable t) {
                throw new CompletionException(t);
            }

            return tables;
        });
    }

    /**
     * Collects a set of table names from the distributed configuration storage.
     *
     * @return A set of table names.
     */
    private Set<String> tableNamesConfigured() {
        // TODO: 01.09.21 Uncomment, properly implement.
//        return new HashSet<>(clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().
//            distirbuteValue().namedListKeys());

        return new HashSet<>(clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().
            value().namedListKeys());
    }

    /** {@inheritDoc} */
    @Override public Table table(String name) {
        return tableAsync(name).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Table> tableAsync(String name) {
        return tableAsync(name, true);
    }

    /**
     * Gets a table if it exists or {@code null} if it was not created or was removed before.
     *
     * @param id Table ID.
     * false otherwise.
     * @return A table or {@code null} if table does not exist.
     */
    @Override public TableImpl table(IgniteUuid id) {
        var tbl = tablesById.get(id);

        if (tbl != null)
            return tbl;

        CompletableFuture<TableImpl> getTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                if (!id.equals(parameters.tableId()))
                    return false;

                if (e == null)
                    getTblFut.complete(parameters.table());
                else
                    getTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                getTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tbl = tablesById.get(id);

        if (tbl != null && getTblFut.complete(tbl) || getTblFut.complete(null))
            removeListener(TableEvent.CREATE, clo, null);

        return getTblFut.join();
    }

    /**
     * Gets a table if it exists or {@code null} if it was not created or was removed before.
     *
     * @param checkConfiguration True when the method checks a configuration before tries to get a table,
     * false otherwise.
     * @return A table or {@code null} if table does not exist.
     */
    private CompletableFuture<Table> tableAsync(String name, boolean checkConfiguration) {
        if (checkConfiguration && !isTableConfigured(name))
            return CompletableFuture.completedFuture(null);

        Table tbl = tables.get(name);

        if (tbl != null)
            return CompletableFuture.completedFuture(tbl);

        CompletableFuture<Table> getTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                String tableName = parameters.tableName();

                if (!name.equals(tableName))
                    return false;

                if (e == null)
                    getTblFut.complete(parameters.table());
                else
                    getTblFut.completeExceptionally(e);

                return true;
            }

            @Override public void remove(@NotNull Throwable e) {
                getTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tbl = tables.get(name);

        if (tbl != null && getTblFut.complete(tbl) ||
            !isTableConfigured(name) && getTblFut.complete(null))
            removeListener(TableEvent.CREATE, clo, null);

        return getTblFut;
    }

    /**
     * Checks that the table is configured.
     *
     * @param name Table name.
     * @return True if table configured, false otherwise.
     */
    private boolean isTableConfigured(String name) {
        return tableNamesConfigured().contains(name);
    }
}
