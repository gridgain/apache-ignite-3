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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Initial schema id. */
    private static final int INITIAL_SCHEMA_VERSION = 1;

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Table manager. */
    private final TableManager tableManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<UUID, SchemaRegistryImpl>> registriesVv;

    /** Constructor. */
    public SchemaManager(Consumer<Consumer<Long>> registry, TablesConfiguration tablesCfg, TableManager tableManager) {
        this.registriesVv = new VersionedValue<>(registry, HashMap::new);

        this.tablesCfg = tablesCfg;
        this.tableManager = tableManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        tableManager.listen(TableEvent.CREATE, new TableCreateListener());
        tableManager.listen(TableEvent.ALTER, new TableUpdateListener());
        tableManager.listen(TableEvent.DROP, new TableDropListener());
    }

    /**
     * Gets and deserializes a schema descriptor from the metadata storage.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getSchemaFromStorage(UUID tblId, @Nullable int schemaVer) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            TableImpl table = (TableImpl) tableManager.table(tblId);

            assert table != null : "Table is undefined [tblId=" + tblId + ']';

            ExtendedTableConfiguration tblCfg = ((ExtendedTableConfiguration) tablesCfg.tables().get(table.name()));

            SchemaRegistryImpl registry = registriesVv.latest().get(tblId);

            CompletableFuture<SchemaDescriptor> fut = new CompletableFuture<>();

            var clo = new EventListener<TableEventParameters>() {
                @Override
                public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
                    if (tblId.equals(parameters.tableId()) && schemaVer <= registry.lastSchemaVersion()) {
                        fut.complete(registry.schema(schemaVer));

                        return true;
                    }

                    return false;
                }

                @Override
                public void remove(@NotNull Throwable exception) {
                    fut.completeExceptionally(exception);
                }
            };

            tableManager.listen(TableEvent.ALTER, clo);

            if (schemaVer <= registry.lastSchemaVersion()) {
                SchemaDescriptor descriptor = SchemaSerializerImpl.INSTANCE.deserialize(tblCfg.schemas().get(String.valueOf(schemaVer))
                        .schema().value());

                fut.complete(descriptor);
            }

            if (!isSchemaExists(tblId, schemaVer) && fut.complete(null)) {
                tableManager.removeListener(TableEvent.ALTER, clo);
            }

            return fut.join();
        } catch (NodeStoppingException e) {
            throw new AssertionError("Table manager was stopped before Schema manager", e);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Checks that the schema is configured in the Metasorage consensus.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return True when the schema configured, false otherwise.
     */
    private boolean isSchemaExists(UUID tblId, int schemaVer) {
        return latestSchemaVersion(tblId) >= schemaVer;
    }

    /**
     * Gets the latest schema version of the table.
     *
     * @param tblId Table id.
     * @return Version.
     */
    private int latestSchemaVersion(UUID tblId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            NamedListView<SchemaView> tblSchemas = ((ExtendedTableConfiguration) getByInternalId(
                    tableManager.directProxy(tablesCfg.tables()), tblId)).schemas().value();

            int lastVer = INITIAL_SCHEMA_VERSION;

            for (String schemaVerAsStr : tblSchemas.namedListKeys()) {
                int ver = Integer.parseInt(schemaVerAsStr);

                if (ver > lastVer) {
                    lastVer = ver;
                }
            }

            return lastVer;
        } catch (NoSuchElementException e) {
            assert false : "Table must exist. [tableId=" + tblId + ']';

            return INITIAL_SCHEMA_VERSION;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Table create event listener.
     */
    private class TableCreateListener implements EventListener<TableEventParameters> {
        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            UUID tblId = parameters.table().tableId();

            SchemaDescriptor descriptor = SchemaSerializerImpl.INSTANCE.deserialize(parameters.schemaBytes());

            registriesVv.update(parameters.causalityToken(), previous -> {
                var val = new HashMap<>(previous);

                var register = new SchemaRegistryImpl(
                        v -> getSchemaFromStorage(tblId, v),
                        () -> latestSchemaVersion(tblId),
                        descriptor);

                parameters.table().schemaView(register);

                val.put(tblId, register);

                return val;
            }, th -> {
                throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create an initial schema for the table "
                        + "[tblName={}, tblId={}, ver={}]", parameters.tableName(), tblId, descriptor.version()), th);
            });

            return false;
        }
    }

    /**
     * Table schema update event listener.
     */
    private class TableUpdateListener implements EventListener<TableEventParameters> {
        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable th) {
            UUID tblId = parameters.table().tableId();

            SchemaDescriptor descriptor = SchemaSerializerImpl.INSTANCE.deserialize(parameters.schemaBytes());

            registriesVv.get(parameters.causalityToken()).whenComplete((registries, throwable) -> {
                if (th != null) {
                    throw new IgniteInternalException(IgniteStringFormatter.format("Cannot register an new schema for the "
                            + "table [tblName={}, tblId={}, ver={}]", parameters.tableName(), tblId, descriptor.version()), th);
                }

                registries.get(tblId).onSchemaRegistered(descriptor);
            }).join();

            return false;
        }
    }

    /**
     * Table drop event listener.
     */
    private class TableDropListener implements EventListener<TableEventParameters> {
        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            UUID tblId = parameters.table().tableId();

            registriesVv.update(parameters.causalityToken(), previous -> {
                var val = new HashMap<>(previous);

                val.remove(tblId);

                return val;
            }, th -> {
                throw new IgniteInternalException(IgniteStringFormatter.format("Cannot remove a schema descriptor for the"
                        + " table [tblName={}, tblId={}]", parameters.tableName(), tblId), th);
            });

            return false;
        }
    }
}
