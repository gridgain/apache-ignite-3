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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.SYSTEM_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.addColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryLength;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryPrecision;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropTableCommand;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.Constants.DUMMY_STORAGE_PROFILE;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.NULL;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DataStorageParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.ArgumentCaptor;

/**
 * Catalog manager self test.
 */
public class CatalogManagerSelfTest extends BaseCatalogManagerTest {
    private static final String SCHEMA_NAME = DEFAULT_SCHEMA_NAME;
    private static final String ZONE_NAME = DEFAULT_ZONE_NAME;
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";
    private static final String NEW_COLUMN_NAME_2 = "NEWCOL2";
    private static final int DFLT_TEST_PRECISION = 11;

    @Test
    public void testEmptyCatalog() {
        CatalogSchemaDescriptor defaultSchema = manager.schema(DEFAULT_SCHEMA_NAME, 0);

        assertNotNull(defaultSchema);
        assertSame(defaultSchema, manager.activeSchema(DEFAULT_SCHEMA_NAME, clock.nowLong()));
        assertSame(defaultSchema, manager.schema(0));
        assertSame(defaultSchema, manager.activeSchema(clock.nowLong()));

        assertNull(manager.schema(1));
        assertThrows(IllegalStateException.class, () -> manager.activeSchema(-1L));

        // Validate default schema.
        assertEquals(DEFAULT_SCHEMA_NAME, defaultSchema.name());
        assertEquals(0, defaultSchema.id());
        assertEquals(0, defaultSchema.tables().length);
        assertEquals(0, defaultSchema.indexes().length);

        // Default distribution zone must exists.
        CatalogZoneDescriptor zone = manager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertEquals(DEFAULT_ZONE_NAME, zone.name());
        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_STORAGE_ENGINE, zone.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, zone.dataStorage().dataRegion());

        // System schema should exist.

        CatalogSchemaDescriptor systemSchema = manager.schema(SYSTEM_SCHEMA_NAME, 0);
        assertNotNull(systemSchema, "system schema");
        assertSame(systemSchema, manager.activeSchema(SYSTEM_SCHEMA_NAME, clock.nowLong()));
        assertSame(systemSchema, manager.schema(SYSTEM_SCHEMA_NAME, 0));

        // Validate system schema.
        assertEquals(SYSTEM_SCHEMA_NAME, systemSchema.name());
        assertEquals(1, systemSchema.id());
        assertEquals(0, systemSchema.tables().length);
        assertEquals(0, systemSchema.indexes().length);

        assertThat(manager.latestCatalogVersion(), is(0));
    }

    @Test
    public void testCreateTable() {
        assertThat(
                manager.execute(createTableCommand(
                        TABLE_NAME,
                        List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                        List.of("key1", "key2"),
                        List.of("key2")
                )),
                willBe(nullValue())
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(0);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(0L));
        assertSame(schema, manager.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, 123L));
        assertNull(manager.index(pkIndexName(TABLE_NAME), 123L));

        // Validate actual catalog
        schema = manager.schema(SCHEMA_NAME, 1);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogHashIndexDescriptor pkIndex = (CatalogHashIndexDescriptor) schema.index(pkIndexName(TABLE_NAME));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.index(pkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        // Validate newly created table
        assertEquals(TABLE_NAME, table.name());
        assertEquals(manager.zone(ZONE_NAME, clock.nowLong()).id(), table.zoneId());

        // Validate newly created pk index
        assertEquals(pkIndexName(TABLE_NAME), pkIndex.name());
        assertEquals(table.id(), pkIndex.tableId());
        assertEquals(table.primaryKeyColumns(), pkIndex.columns());
        assertTrue(pkIndex.unique());
        assertTrue(pkIndex.available());

        CatalogTableColumnDescriptor desc = table.columnDescriptor("key1");
        assertNotNull(desc);
        // INT32 key
        assertThat(desc.precision(), is(DEFAULT_PRECISION));

        // Validate another table creation.
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        // Validate actual catalog has both tables.
        schema = manager.schema(2);
        table = schema.table(TABLE_NAME);
        pkIndex = (CatalogHashIndexDescriptor) schema.index(pkIndexName(TABLE_NAME));
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogHashIndexDescriptor pkIndex2 = (CatalogHashIndexDescriptor) schema.index(pkIndexName(TABLE_NAME_2));

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(pkIndex, manager.index(pkIndexName(TABLE_NAME), clock.nowLong()));
        assertSame(pkIndex, manager.index(pkIndex.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.index(pkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        assertNotSame(table, table2);
        assertNotSame(pkIndex, pkIndex2);

        // Try to create another table with same name.
        assertThat(
                manager.execute(simpleTable(TABLE_NAME_2)),
                willThrowFast(CatalogValidationException.class)
        );

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void testDropTable() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(2);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);
        CatalogIndexDescriptor pkIndex1 = schema.index(pkIndexName(TABLE_NAME));
        CatalogIndexDescriptor pkIndex2 = schema.index(pkIndexName(TABLE_NAME_2));

        assertNotEquals(table1.id(), table2.id());
        assertNotEquals(pkIndex1.id(), pkIndex2.id());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table1, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table1, manager.table(table1.id(), beforeDropTimestamp));

        assertSame(pkIndex1, manager.index(pkIndexName(TABLE_NAME), beforeDropTimestamp));
        assertSame(pkIndex1, manager.index(pkIndex1.id(), beforeDropTimestamp));

        assertSame(table2, manager.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(table2, manager.table(table2.id(), beforeDropTimestamp));

        assertSame(pkIndex2, manager.index(pkIndexName(TABLE_NAME_2), beforeDropTimestamp));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table1.id(), clock.nowLong()));

        assertNull(schema.index(pkIndexName(TABLE_NAME)));
        assertNull(manager.index(pkIndexName(TABLE_NAME), clock.nowLong()));
        assertNull(manager.index(pkIndex1.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertSame(pkIndex2, manager.index(pkIndexName(TABLE_NAME_2), clock.nowLong()));
        assertSame(pkIndex2, manager.index(pkIndex2.id(), clock.nowLong()));

        // Validate schema wasn't changed.
        assertSame(schema, manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParamsBuilder(NEW_COLUMN_NAME, STRING, 11, true).defaultValue(constant("Ignite!")).build()
                )),
                willBe(nullValue())
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);

        assertEquals(NEW_COLUMN_NAME, column.name());
        assertEquals(STRING, column.type());
        assertTrue(column.nullable());

        assertEquals(DefaultValue.Type.CONSTANT, column.defaultValue().type());
        assertEquals("Ignite!", ((DefaultValue.ConstantValue) column.defaultValue()).value());

        assertEquals(11, column.length());
        assertEquals(DEFAULT_PRECISION, column.precision());
        assertEquals(DEFAULT_SCALE, column.scale());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "VAL")), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        assertNotNull(schema.table(TABLE_NAME));

        assertNull(schema.table(TABLE_NAME).column("VAL"));
    }

    @Test
    public void testAddDropMultipleColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add duplicate column.
        assertThat(
                manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32, true), columnParams("VAL", INT32, true))),
                willThrow(CatalogValidationException.class)
        );

        // Validate no column added.
        CatalogSchemaDescriptor schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Add multiple columns.
        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParams(NEW_COLUMN_NAME, INT32, true), columnParams(NEW_COLUMN_NAME_2, INT32, true)
                )),
                willBe(nullValue())
        );

        // Validate both columns added.
        schema = manager.activeSchema(clock.nowLong());

        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNotNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));

        // Drop multiple columns.
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME, NEW_COLUMN_NAME_2)), willBe(nullValue()));

        // Validate both columns dropped.
        schema = manager.activeSchema(clock.nowLong());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));
        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME_2));
    }

    /**
     * Checks for possible changes to the default value of a column descriptor.
     *
     * <p>Set/drop default value allowed for any column.
     */
    @Test
    public void testAlterColumnDefault() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULL-> NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // NULL -> 1 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 1 -> 1 : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(1)),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 1 -> 2 : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(2)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // 2 -> NULL : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, null, () -> constant(null)),
                willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));
    }

    /**
     * Checks for possible changes of the nullable flag of a column descriptor.
     *
     * <ul>
     *  <li>{@code DROP NOT NULL} is allowed on any non-PK column.
     *  <li>{@code SET NOT NULL} is forbidden.
     * </ul>
     */
    @Test
    public void testAlterColumnNotNull() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // NULLABLE -> NULLABLE : No-op.
        // NOT NULL -> NOT NULL : No-op.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, false, null), willBe(nullValue()));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null), willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // NOT NULL -> NULlABLE : Ok.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, false, null), willBe(nullValue()));
        assertNotNull(manager.schema(++schemaVer));

        // DROP NOT NULL for PK : PK column can't be `null`.
        assertThat(changeColumn(TABLE_NAME, "ID", null, false, null),
                willThrowFast(CatalogValidationException.class, "Dropping NOT NULL constraint on key column is not allowed"));

        // NULlABLE -> NOT NULL : Forbidden because this change lead to incompatible schemas.
        assertThat(changeColumn(TABLE_NAME, "VAL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", null, true, null),
                willThrowFast(CatalogValidationException.class, "Adding NOT NULL constraint is not allowed"));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the precision of a column descriptor.
     *
     * <ul>
     *  <li>Increasing precision is allowed for non-PK {@link ColumnType#DECIMAL} column.</li>
     *  <li>Decreasing precision is forbidden.</li>
     * </ul>
     */
    @Test
    public void testAlterColumnTypePrecision() {
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col1 = columnParamsBuilder("COL_DECIMAL1", DECIMAL).precision(DFLT_TEST_PRECISION - 1).scale(1).build();
        ColumnParams col2 = columnParamsBuilder("COL_DECIMAL2", DECIMAL).precision(DFLT_TEST_PRECISION).scale(1).build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col1, col2))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // precision increment : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willBe(nullValue())
        );
        assertNotNull(manager.schema(++schemaVer));

        assertThat(
                changeColumn(TABLE_NAME, col2.name(), new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willBe(nullValue())
        );
        assertNull(manager.schema(schemaVer + 1));

        // No change.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(), new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION, null, null), null, null),
                willBe(nullValue())
        );
        assertNull(manager.schema(schemaVer + 1));

        // precision decrement : Forbidden because this change lead to incompatible schemas.
        assertThat(
                changeColumn(TABLE_NAME, col1.name(),
                        new TestColumnTypeParams(col1.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));

        assertThat(
                changeColumn(TABLE_NAME, col2.name(),
                        new TestColumnTypeParams(col2.type(), DFLT_TEST_PRECISION - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the precision is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing precision is not supported for all types other than DECIMAL.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "DECIMAL"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyPrecisionChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams colWithPrecision;
        Builder colWithPrecisionBuilder = columnParamsBuilder("COL_PRECISION", type).precision(3);
        applyNecessaryLength(type, colWithPrecisionBuilder);

        if (type.scaleAllowed()) {
            colWithPrecisionBuilder.scale(0);
        }

        if (!type.precisionAllowed() && !type.scaleAllowed()) {
            assertThrowsWithCause(colWithPrecisionBuilder::build, CatalogValidationException.class);
            return;
        }

        colWithPrecision = colWithPrecisionBuilder.build();

        assertThat(manager.execute(
                simpleTable(TABLE_NAME, List.of(pkCol, colWithPrecision))), willBe(nullValue())
        );

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        int origPrecision = colWithPrecision.precision() == null ? 11 : colWithPrecision.precision();

        // change precision different from default
        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(),
                        new TestColumnTypeParams(type, origPrecision - 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the precision for column of type '" + colWithPrecision.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithPrecision.name(),
                        new TestColumnTypeParams(type, origPrecision + 1, null, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the precision for column of type '" + colWithPrecision.type() + "' is not allowed"));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the length of a column descriptor.
     *
     * <ul>
     *  <li>Increasing length is allowed for non-PK {@link ColumnType#STRING} and {@link ColumnType#BYTE_ARRAY} column.</li>
     *  <li>Decreasing length is forbidden.</li>
     * </ul>
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"STRING", "BYTE_ARRAY"}, mode = Mode.INCLUDE)
    public void testAlterColumnTypeLength(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        ColumnParams col = columnParamsBuilder("COL_" + type, type).length(10).build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // 10 -> 11 : Ok.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willBe(nullValue())
        );

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        // 11 -> 10 : Error.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class, "Decreasing the length is not allowed")
        );
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 11 : No-op.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 11, null), null, null),
                willBe(nullValue())
        );
        assertNull(manager.schema(schemaVer + 1));

        // No change.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 11 -> 10 : failed.
        assertThat(
                changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, 10, null), null, null),
                willThrowFast(CatalogValidationException.class)
        );
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing length is forbidden for all types other than STRING and BYTE_ARRAY.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"STRING", "BYTE_ARRAY", "NULL"}, mode = Mode.EXCLUDE)
    public void testAlterColumnTypeAnyLengthChangeIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        Builder colBuilder = columnParamsBuilder("COL", type);
        Builder colWithLengthBuilder = columnParamsBuilder("COL_PRECISION", type).length(10);

        applyNecessaryLength(type, colWithLengthBuilder);

        initializeColumnWithDefaults(type, colBuilder);
        initializeColumnWithDefaults(type, colWithLengthBuilder);

        ColumnParams col = colBuilder.build();

        if (!type.lengthAllowed()) {
            assertThrowsWithCause(colWithLengthBuilder::build, CatalogValidationException.class);
            return;
        }

        ColumnParams colWithLength = colWithLengthBuilder.build();

        assertThat(
                manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col, colWithLength))),
                willBe(nullValue())
        );

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(type, null, 1 << 6, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + col.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 1 << 5, null), null, null),
                willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 9, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + colWithLength.type() + "' is not allowed"));

        assertThat(changeColumn(TABLE_NAME, colWithLength.name(), new TestColumnTypeParams(type, null, 11, null), null, null),
                willThrowFast(CatalogValidationException.class,
                        "Changing the length for column of type '" + colWithLength.type() + "' is not allowed"));

        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Changing scale is incompatible change, thus it's forbidden for all types.
     */
    @ParameterizedTest
    @EnumSource(ColumnType.class)
    public void testAlterColumnTypeScaleIsRejected(ColumnType type) {
        ColumnParams pkCol = columnParams("ID", INT32);
        Builder colWithPrecisionBuilder = columnParamsBuilder("COL_" + type, type).scale(3);
        ColumnParams col;

        applyNecessaryPrecision(type, colWithPrecisionBuilder);
        applyNecessaryLength(type, colWithPrecisionBuilder);

        if (!type.scaleAllowed()) {
            assertThrowsWithCause(colWithPrecisionBuilder::build, CatalogValidationException.class);
            return;
        }

        col = colWithPrecisionBuilder.build();

        assertThat(manager.execute(simpleTable(TABLE_NAME, List.of(pkCol, col))), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        // ANY-> UNDEFINED SCALE : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type()), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 3 : No-op.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 3), null, null),
                willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 4 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 4), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale is not allowed"));
        assertNull(manager.schema(schemaVer + 1));

        // 3 -> 2 : Error.
        assertThat(changeColumn(TABLE_NAME, col.name(), new TestColumnTypeParams(col.type(), null, null, 2), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the scale is not allowed"));
        assertNull(manager.schema(schemaVer + 1));
    }

    /**
     * Checks for possible changes of the type of a column descriptor.
     *
     * <p>The following transitions are allowed for non-PK columns:
     * <ul>
     *     <li>INT8 -> INT16 -> INT32 -> INT64</li>
     *     <li>FLOAT -> DOUBLE</li>
     * </ul>
     * All other transitions are forbidden because they lead to incompatible schemas.
     */
    @ParameterizedTest(name = "set data type {0}")
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testAlterColumnType(ColumnType target) {
        EnumSet<ColumnType> types = EnumSet.allOf(ColumnType.class);
        types.remove(NULL);

        List<ColumnParams> testColumns = types.stream()
                .map(t -> initializeColumnWithDefaults(t, columnParamsBuilder("COL_" + t, t)))
                .map(Builder::build)
                .collect(toList());

        List<ColumnParams> tableColumns = new ArrayList<>(List.of(columnParams("ID", INT32)));
        tableColumns.addAll(testColumns);

        assertThat(manager.execute(simpleTable(TABLE_NAME, tableColumns)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        for (ColumnParams col : testColumns) {
            TypeSafeMatcher<CompletableFuture<?>> matcher;
            boolean sameType = col.type() == target;

            if (sameType || CatalogUtils.isSupportedColumnTypeChange(col.type(), target)) {
                matcher = willBe(nullValue());
                schemaVer += sameType ? 0 : 1;
            } else {
                matcher = willThrowFast(CatalogValidationException.class,
                        format("Changing the type from {} to {} is not allowed", col.type(), target));
            }

            TestColumnTypeParams typeParams = new TestColumnTypeParams(target);

            assertThat(col.type() + " -> " + target, changeColumn(TABLE_NAME, col.name(), typeParams, null, null), matcher);
            assertNotNull(manager.schema(schemaVer));
            assertNull(manager.schema(schemaVer + 1));
        }
    }

    @Test
    public void testAlterColumnTypeRejectedForPrimaryKey() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(changeColumn(TABLE_NAME, "ID", new TestColumnTypeParams(INT64), null, null),
                willThrowFast(CatalogValidationException.class, "Changing the type of key column is not allowed"));
    }

    /**
     * Ensures that the compound change command {@code SET DATA TYPE BIGINT NULL DEFAULT NULL} will change the type, drop NOT NULL and the
     * default value at the same time.
     */
    @Test
    public void testAlterColumnMultipleChanges() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        int schemaVer = 1;
        assertNotNull(manager.schema(schemaVer));
        assertNull(manager.schema(schemaVer + 1));

        Supplier<DefaultValue> dflt = () -> constant(null);
        boolean notNull = false;
        TestColumnTypeParams typeParams = new TestColumnTypeParams(INT64);

        // Ensures that 3 different actions applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        CatalogSchemaDescriptor schema = manager.schema(++schemaVer);
        assertNotNull(schema);

        CatalogTableColumnDescriptor desc = schema.table(TABLE_NAME).column("VAL_NOT_NULL");
        assertEquals(constant(null), desc.defaultValue());
        assertTrue(desc.nullable());
        assertEquals(INT64, desc.type());

        // Ensures that only one of three actions applied.
        dflt = () -> constant(2);
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));

        schema = manager.schema(++schemaVer);
        assertNotNull(schema);
        assertEquals(constant(2), schema.table(TABLE_NAME).column("VAL_NOT_NULL").defaultValue());

        // Ensures that no action will be applied.
        assertThat(changeColumn(TABLE_NAME, "VAL_NOT_NULL", typeParams, notNull, dflt), willBe(nullValue()));
        assertNull(manager.schema(schemaVer + 1));
    }

    @Test
    public void testAlterColumnForNonExistingTableRejected() {
        assertNotNull(manager.schema(0));
        assertNull(manager.schema(1));

        assertThat(changeColumn(TABLE_NAME, "ID", null, null, null), willThrowFast(TableNotFoundValidationException.class));

        assertNotNull(manager.schema(0));
        assertNull(manager.schema(1));
    }

    @Test
    public void testDropTableWithIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.execute(simpleIndex()), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(2);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        CatalogIndexDescriptor index = schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        assertSame(table, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table, manager.table(table.id(), beforeDropTimestamp));

        assertSame(index, manager.index(INDEX_NAME, beforeDropTimestamp));
        assertSame(index, manager.index(index.id(), beforeDropTimestamp));

        // Validate actual catalog
        schema = manager.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table.id(), clock.nowLong()));

        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, clock.nowLong()));
        assertNull(manager.index(index.id(), clock.nowLong()));
    }

    @Test
    public void testCreateHashIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));

        // Validate actual catalog
        schema = manager.schema(2);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created hash index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(List.of("VAL", "ID"), index.columns());
        assertFalse(index.unique());
        assertFalse(index.available());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CatalogCommand command = createSortedIndexCommand(
                INDEX_NAME,
                true,
                List.of("VAL", "ID"),
                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        );

        assertThat(manager.execute(command), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));
        assertNull(manager.index(4, 123L));

        // Validate actual catalog
        schema = manager.schema(2);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));

        // Validate newly created sorted index
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals("VAL", index.columns().get(0).name());
        assertEquals("ID", index.columns().get(1).name());
        assertEquals(DESC_NULLS_FIRST, index.columns().get(0).collation());
        assertEquals(ASC_NULLS_LAST, index.columns().get(1).collation());
        assertTrue(index.unique());
        assertFalse(index.available());
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockWaiter);
        manager.start();

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock manager and allow to
            // make another attempt, we must notify manager with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    updateFromInvocation.delayDurationMs(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update, clock.now(), 0);

            return falseCompletedFuture();
        });

        CompletableFuture<Void> createTableFut = manager.execute(simpleTable("T"));

        assertThat(createTableFut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogActivationTime() throws Exception {
        long delayDuration = TimeUnit.DAYS.toMillis(365);

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, delayDuration, 0);

        manager.start();

        try {
            CompletableFuture<Void> createTableFuture = manager.execute(simpleTable(TABLE_NAME));

            assertFalse(createTableFuture.isDone());

            verify(updateLog).append(any());
            // TODO IGNITE-19400: recheck createTable future completion guarantees

            // This waits till the new Catalog version lands in the internal structures.
            verify(clockWaiter, timeout(10_000)).waitFor(any());

            assertSame(manager.schema(0), manager.activeSchema(clock.nowLong()));
            assertNull(manager.table(TABLE_NAME, clock.nowLong()));

            clock.update(clock.now().addPhysicalTime(delayDuration));

            assertSame(manager.schema(1), manager.activeSchema(clock.nowLong()));
            assertNotNull(manager.table(TABLE_NAME, clock.nowLong()));
        } finally {
            manager.stop();
        }
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() throws Exception {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockWaiter);

        manager.start();

        verify(updateLogMock).start();

        manager.stop();

        verify(updateLogMock).stop();
    }

    @Test
    public void testTableEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_CREATE, eventListener);
        manager.listen(CatalogEvent.TABLE_DROP, eventListener);

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(CreateTableEventParameters.class), isNull());

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(DropTableEventParameters.class), isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testCreateIndexEvents() {
        CatalogCommand createIndexCmd = createHashIndexCommand(INDEX_NAME, List.of("ID"));

        CatalogCommand dropIndexCmd = DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.INDEX_CREATE, eventListener);
        manager.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Try to create index without table.
        assertThat(manager.execute(createIndexCmd), willThrow(TableNotFoundValidationException.class));
        verifyNoInteractions(eventListener);

        // Create table with PK index.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Create index.
        assertThat(manager.execute(createIndexCmd), willCompleteSuccessfully());
        verify(eventListener).notify(any(CreateIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop index.
        assertThat(manager.execute(dropIndexCmd), willBe(nullValue()));
        verify(eventListener).notify(any(DropIndexEventParameters.class), isNull());

        clearInvocations(eventListener);

        // Drop table with pk index.
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));

        // Try drop index once again.
        assertThat(manager.execute(dropIndexCmd), willThrow(IndexNotFoundValidationException.class));

        verify(eventListener).notify(any(DropIndexEventParameters.class), isNull());
    }

    @Test
    public void testCreateZone() {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .dataStorageParams(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(manager.zone(zoneName, 0));
        assertNull(manager.zone(zoneName, 123L));

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        // Validate that catalog returns null for previous timestamps.
        assertNull(manager.zone(zone.id(), 0));
        assertNull(manager.zone(zone.id(), 123L));

        // Validate newly created zone
        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
        assertEquals(73, zone.dataNodesAutoAdjust());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("expression", zone.filter());
        assertEquals("test_engine", zone.dataStorage().engine());
        assertEquals("test_region", zone.dataStorage().dataRegion());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());
    }

    @Test
    public void testDropZone() {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        CompletableFuture<Void> fut = manager.execute(dropCommand);

        assertThat(fut, willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertNull(manager.zone(zone.id(), clock.nowLong()));

        // Try to drop non-existing zone.
        assertThat(manager.execute(dropCommand), willThrow(DistributionZoneNotFoundValidationException.class));
    }

    @Test
    public void testRenameZone() throws InterruptedException {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        Thread.sleep(5);

        String newZoneName = "RenamedZone";

        CatalogCommand renameZoneCmd = RenameZoneCommand.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(manager.execute(renameZoneCmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));

        // Validate actual catalog
        zone = manager.zone(newZoneName, clock.nowLong());

        assertNotNull(zone);
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));
    }

    @Test
    public void testDefaultZone() {
        CatalogZoneDescriptor defaultZone = manager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        // Try to create zone with default zone name.
        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(DEFAULT_ZONE_NAME)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();
        assertThat(manager.execute(cmd), willThrow(IgniteInternalException.class));

        // Validate default zone wasn't changed.
        assertSame(defaultZone, manager.zone(DEFAULT_ZONE_NAME, clock.nowLong()));
    }

    @Test
    public void testAlterZone() {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .dataNodesAutoAdjust(73)
                .filter("expression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        CatalogCommand alterCmd = AlterZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(10)
                .replicas(2)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .dataStorageParams(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile("test_profile").build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());
        assertThat(manager.execute(alterCmd), willCompleteSuccessfully());

        // Validate actual catalog
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(10, zone.partitions());
        assertEquals(2, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        assertEquals("test_engine", zone.dataStorage().engine());
        assertEquals("test_region", zone.dataStorage().dataRegion());
        assertEquals("test_profile", zone.storageProfiles().profiles().get(0).storageProfile());
    }

    @Test
    public void testCreateZoneWithSameName() {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(42)
                .replicas(15)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        // Try to create zone with same name.
        cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .partitions(8)
                .replicas(1)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willThrowFast(DistributionZoneExistsValidationException.class));

        // Validate zone was NOT changed
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zoneName, clock.nowLong()));
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(42, zone.partitions());
        assertEquals(15, zone.replicas());
    }

    @Test
    public void testCreateZoneEvents() {
        String zoneName = ZONE_NAME + 1;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build()))
                .build();

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.ZONE_CREATE, eventListener);
        manager.listen(CatalogEvent.ZONE_DROP, eventListener);

        CompletableFuture<Void> fut = manager.execute(cmd);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(CreateZoneEventParameters.class), isNull());

        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();

        fut = manager.execute(dropCommand);

        assertThat(fut, willCompleteSuccessfully());

        verify(eventListener).notify(any(DropZoneEventParameters.class), isNull());
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testColumnEvents() {
        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);
        when(eventListener.notify(any(), any())).thenReturn(falseCompletedFuture());

        manager.listen(CatalogEvent.TABLE_ALTER, eventListener);

        // Try to add column without table.
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32))),
                willThrow(TableNotFoundValidationException.class));
        verifyNoInteractions(eventListener);

        // Create table.
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        // Add column.
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams(NEW_COLUMN_NAME, INT32))), willBe(nullValue()));
        verify(eventListener).notify(any(AddColumnEventParameters.class), isNull());

        // Drop column.
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, NEW_COLUMN_NAME)), willBe(nullValue()));
        verify(eventListener).notify(any(DropColumnEventParameters.class), isNull());

        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void userFutureCompletesAfterClusterWideActivationHappens() throws Exception {
        long delayDuration = TimeUnit.DAYS.toMillis(365);

        HybridTimestamp startTs = clock.now();

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, delayDuration, 0);

        manager.start();

        try {
            CompletableFuture<Void> createTableFuture = manager.execute(simpleTable(TABLE_NAME));

            assertFalse(createTableFuture.isDone());

            ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

            verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
            HybridTimestamp userWaitTs = tsCaptor.getValue();
            assertThat(
                    userWaitTs.getPhysical() - startTs.getPhysical(),
                    greaterThanOrEqualTo(delayDuration + HybridTimestamp.maxClockSkew())
            );
        } finally {
            manager.stop();
        }
    }

    // TODO: remove after IGNITE-20378 is implemented.
    @Test
    public void userFutureCompletesAfterClusterWideActivationWithAdditionalIdleSafeTimePeriodHappens() throws Exception {
        long delayDuration = TimeUnit.DAYS.toMillis(365);
        long partitionIdleSafeTimePropagationPeriod = TimeUnit.DAYS.toDays(365);

        HybridTimestamp startTs = clock.now();

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLog, clockWaiter, delayDuration, partitionIdleSafeTimePropagationPeriod);

        manager.start();

        try {
            CompletableFuture<Void> createTableFuture = manager.execute(simpleTable(TABLE_NAME));

            assertFalse(createTableFuture.isDone());

            ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

            verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
            HybridTimestamp userWaitTs = tsCaptor.getValue();
            assertThat(
                    userWaitTs.getPhysical() - startTs.getPhysical(),
                    greaterThanOrEqualTo(
                            delayDuration + HybridTimestamp.maxClockSkew()
                                    + partitionIdleSafeTimePropagationPeriod + HybridTimestamp.maxClockSkew()
                    )
            );
        } finally {
            manager.stop();
        }
    }

    @Test
    void testGetCatalogEntityInCatalogEvent() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        manager.listen(CatalogEvent.TABLE_CREATE, (parameters, exception) -> {
            try {
                assertNotNull(manager.schema(parameters.catalogVersion()));

                result.complete(null);

                return trueCompletedFuture();
            } catch (Throwable t) {
                result.completeExceptionally(t);

                return failedFuture(t);
            }
        });

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(result, willCompleteSuccessfully());
    }

    @Test
    void testGetTableByIdAndCatalogVersion() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CatalogTableDescriptor table = manager.table(TABLE_NAME, clock.nowLong());

        assertNull(manager.table(table.id(), 0));
        assertSame(table, manager.table(table.id(), 1));
    }

    @Test
    void testGetTableIdOnDropIndexEvent() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL"))), willBe(nullValue()));

        int tableId = manager.table(TABLE_NAME, clock.nowLong()).id();
        int pkIndexId = manager.index(pkIndexName(TABLE_NAME), clock.nowLong()).id();
        int indexId = manager.index(INDEX_NAME, clock.nowLong()).id();

        assertNotEquals(tableId, indexId);

        EventListener<CatalogEventParameters> eventListener = mock(EventListener.class);

        ArgumentCaptor<DropIndexEventParameters> captor = ArgumentCaptor.forClass(DropIndexEventParameters.class);

        doReturn(falseCompletedFuture()).when(eventListener).notify(captor.capture(), any());

        manager.listen(CatalogEvent.INDEX_DROP, eventListener);

        // Let's remove the index.
        assertThat(
                manager.execute(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willBe(nullValue())
        );

        DropIndexEventParameters eventParameters = captor.getValue();

        assertEquals(indexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());

        // Let's delete the table.
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));

        // Let's make sure that the PK index has been deleted.
        eventParameters = captor.getValue();

        assertEquals(pkIndexId, eventParameters.indexId());
        assertEquals(tableId, eventParameters.tableId());
    }

    @Test
    void testLatestCatalogVersion() {
        assertEquals(0, manager.latestCatalogVersion());

        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertEquals(1, manager.latestCatalogVersion());

        assertThat(manager.execute(simpleIndex()), willBe(nullValue()));
        assertEquals(2, manager.latestCatalogVersion());
    }

    @Test
    void testTables() {
        assertThat(manager.execute(simpleTable(TABLE_NAME + 0)), willBe(nullValue()));
        assertThat(manager.execute(simpleTable(TABLE_NAME + 1)), willBe(nullValue()));

        assertThat(manager.tables(0), empty());
        assertThat(manager.tables(1), hasItems(table(1, TABLE_NAME + 0)));
        assertThat(manager.tables(2), hasItems(table(2, TABLE_NAME + 0), table(2, TABLE_NAME + 1)));
    }

    @Test
    void testIndexes() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.execute(simpleIndex()), willBe(nullValue()));

        assertThat(manager.indexes(0), empty());
        assertThat(manager.indexes(1), hasItems(index(1, pkIndexName(TABLE_NAME))));
        assertThat(manager.indexes(2), hasItems(index(2, pkIndexName(TABLE_NAME)), index(2, INDEX_NAME)));
    }

    @Test
    public void createTableProducesTableVersion1() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(1));
    }

    @Test
    public void addColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        addSomeColumn();

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    private void addSomeColumn() {
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("val2", INT32))), willCompleteSuccessfully());
    }

    @Test
    public void dropColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "val1")), willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void alterColumnIncrementsTableVersion() {
        createSomeTable(TABLE_NAME);

        CompletableFuture<Void> future = manager.execute(
                AlterTableAlterColumnCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columnName("val1")
                        .type(INT64)
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));
    }

    @Test
    public void testTableCreationToken() {
        createSomeTable(TABLE_NAME);

        CatalogTableDescriptor table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        long expectedCreationToken = table.updateToken();

        assertEquals(expectedCreationToken, table.creationToken());

        CompletableFuture<Void> future = manager.execute(
                AlterTableAlterColumnCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(TABLE_NAME)
                        .columnName("val1")
                        .type(INT64)
                        .build()
        );
        assertThat(future, willCompleteSuccessfully());

        table = manager.table(TABLE_NAME, Long.MAX_VALUE);

        assertThat(table.tableVersion(), is(2));

        assertEquals(expectedCreationToken, table.creationToken());

        table = manager.table(tableId(TABLE_NAME), 1);

        assertEquals(expectedCreationToken, table.creationToken());
    }

    @Test
    void testCreateZoneWithDefaults() {
        assertThat(
                manager.execute(
                        CreateZoneCommand.builder()
                                .zoneName(ZONE_NAME + 1)
                                .storageProfilesParams(
                                        List.of(StorageProfileParams.builder().storageProfile(DUMMY_STORAGE_PROFILE).build())
                                ).build()
                ),
                willBe(nullValue())
        );

        CatalogZoneDescriptor zone = manager.zone(ZONE_NAME, clock.nowLong());

        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(DEFAULT_STORAGE_ENGINE, zone.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, zone.dataStorage().dataRegion());
        assertEquals(DUMMY_STORAGE_PROFILE, zone.storageProfiles().defaultProfile().storageProfile());
    }

    @Test
    void testCreateIndexWithAlreadyExistingName() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex()), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL"))),
                willThrowFast(IndexExistsValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(IndexExistsValidationException.class)
        );
    }

    @Test
    void testCreateIndexWithSameNameAsExistingTable() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableExistsValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableExistsValidationException.class)
        );
    }

    @Test
    void testCreateIndexWithNotExistingTable() {
        assertThat(
                manager.execute(createHashIndexCommand(TABLE_NAME, List.of("VAL"))),
                willThrowFast(TableNotFoundValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(TABLE_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(TableNotFoundValidationException.class)
        );
    }

    @Test
    void testCreateIndexWithMissingTableColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("fake"))),
                willThrowFast(CatalogValidationException.class)
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("fake"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class)
        );
    }

    @Test
    void testCreateUniqIndexWithMissingTableColocationColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, true, List.of("VAL"))),
                willThrowFast(CatalogValidationException.class, "Unique index must include all colocation columns")
        );

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, true, List.of("VAL"), List.of(ASC_NULLS_LAST))),
                willThrowFast(CatalogValidationException.class, "Unique index must include all colocation columns")
        );
    }

    @Test
    void testDropNotExistingIndex() {
        assertThat(
                manager.execute(DropIndexCommand.builder().schemaName(SCHEMA_NAME).indexName(INDEX_NAME).build()),
                willThrowFast(IndexNotFoundValidationException.class)
        );
    }

    @Test
    void testDropNotExistingTable() {
        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willThrowFast(CatalogValidationException.class));
    }

    @Test
    void testDropColumnWithNotExistingTable() {
        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "key")), willThrowFast(TableNotFoundValidationException.class));
    }

    @Test
    void testDropColumnWithMissingTableColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "fake")), willThrowFast(CatalogValidationException.class));
    }

    @Test
    void testDropColumnWithPrimaryKeyColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(
                manager.execute(dropColumnParams(TABLE_NAME, "ID")),
                willThrowFast(CatalogValidationException.class, "Deleting column `ID` belonging to primary key is not allowed")
        );
    }

    @Test
    void testDropColumnWithIndexColumns() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());
        assertThat(manager.execute(simpleIndex()), willCompleteSuccessfully());

        assertThat(
                manager.execute(dropColumnParams(TABLE_NAME, "VAL")),
                willThrowFast(
                        CatalogValidationException.class,
                        "Deleting column 'VAL' used by index(es) [myIndex], it is not allowed"
                )
        );
    }

    @Test
    void testAddColumnWithNotExistingTable() {
        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("key", INT32))),
                willThrowFast(TableNotFoundValidationException.class));
    }

    @Test
    void testAddColumnWithExistingName() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        assertThat(manager.execute(addColumnParams(TABLE_NAME, columnParams("ID", INT32))),
                willThrowFast(CatalogValidationException.class));
    }

    @Test
    void bulkCommandEitherAppliedAtomicallyOrDoesntAppliedAtAll() {
        String tableName1 = "TEST1";
        String tableName2 = "TEST2";
        String tableName3 = "TEST1"; // intentional name conflict with table1

        List<CatalogCommand> bulkUpdate = List.of(
                simpleTable(tableName1),
                simpleTable(tableName2),
                simpleTable(tableName3)
        );

        assertThat(manager.table(tableName1, Long.MAX_VALUE), nullValue());
        assertThat(manager.table(tableName2, Long.MAX_VALUE), nullValue());
        assertThat(manager.table(tableName3, Long.MAX_VALUE), nullValue());

        assertThat(manager.execute(bulkUpdate), willThrowFast(TableExistsValidationException.class));

        // now let's truncate problematic table and retry
        assertThat(manager.execute(bulkUpdate.subList(0, bulkUpdate.size() - 1)), willCompleteSuccessfully());

        assertThat(manager.table(tableName1, Long.MAX_VALUE), notNullValue());
        assertThat(manager.table(tableName2, Long.MAX_VALUE), notNullValue());
    }

    @Test
    void bulkUpdateIncrementsVersionByOne() {
        String tableName1 = "T1";
        String tableName2 = "T2";
        String tableName3 = "T3";

        int versionBefore = manager.latestCatalogVersion();

        assertThat(manager.table(tableName1, Long.MAX_VALUE), nullValue());
        assertThat(manager.table(tableName2, Long.MAX_VALUE), nullValue());
        assertThat(manager.table(tableName3, Long.MAX_VALUE), nullValue());

        assertThat(
                manager.execute(List.of(simpleTable(tableName1), simpleTable(tableName2), simpleTable(tableName3))),
                willCompleteSuccessfully()
        );

        int versionAfter = manager.latestCatalogVersion();

        assertThat(manager.table(tableName1, Long.MAX_VALUE), notNullValue());
        assertThat(manager.table(tableName2, Long.MAX_VALUE), notNullValue());
        assertThat(manager.table(tableName3, Long.MAX_VALUE), notNullValue());

        assertThat(versionAfter - versionBefore, is(1));
    }

    @Test
    void bulkUpdateDoesntIncrementVersionInCaseOfError() {
        String tableName1 = "T1";

        int versionBefore = manager.latestCatalogVersion();

        assertThat(manager.table(tableName1, Long.MAX_VALUE), nullValue());

        assertThat(
                manager.execute(List.of(simpleTable(tableName1), simpleTable(tableName1))),
                willThrow(CatalogValidationException.class)
        );

        int versionAfter = manager.latestCatalogVersion();

        assertThat(manager.table(tableName1, Long.MAX_VALUE), nullValue());

        assertThat(versionAfter, is(versionBefore));
    }

    @Test
    void testMakeHashIndexAvailable() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(MakeIndexAvailableCommand.builder().indexId(indexId(INDEX_NAME)).build()),
                willBe(nullValue())
        );

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertTrue(index.available());
    }

    @Test
    void testMakeSortedIndexAvailable() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createSortedIndexCommand(INDEX_NAME, List.of("key1"), List.of(ASC_NULLS_LAST))),
                willBe(nullValue())
        );

        assertThat(
                manager.execute(MakeIndexAvailableCommand.builder().indexId(indexId(INDEX_NAME)).build()),
                willBe(nullValue())
        );

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) index(manager.latestCatalogVersion(), INDEX_NAME);

        assertTrue(index.available());
    }

    @Test
    void testAvailableIndexEvent() {
        createSomeTable(TABLE_NAME);

        assertThat(
                manager.execute(createHashIndexCommand(INDEX_NAME, List.of("key1"))),
                willBe(nullValue())
        );

        int indexId = index(manager.latestCatalogVersion(), INDEX_NAME).id();

        CompletableFuture<Void> fireEventFuture = new CompletableFuture<>();

        manager.listen(CatalogEvent.INDEX_AVAILABLE, (parameters, exception) -> {
            if (exception != null) {
                fireEventFuture.completeExceptionally(exception);
            } else {
                try {
                    assertEquals(indexId, ((MakeIndexAvailableEventParameters) parameters).indexId());

                    fireEventFuture.complete(null);
                } catch (Throwable t) {
                    fireEventFuture.completeExceptionally(t);
                }
            }

            return falseCompletedFuture();
        });

        assertThat(
                manager.execute(MakeIndexAvailableCommand.builder().indexId(indexId(INDEX_NAME)).build()),
                willBe(nullValue())
        );

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    void testPkAvailableIndexEvent() {
        CompletableFuture<Integer> fireEventFuture = new CompletableFuture<>();

        manager.listen(CatalogEvent.INDEX_AVAILABLE, (parameters, exception) -> {
            if (exception != null) {
                fireEventFuture.completeExceptionally(exception);
            } else {
                try {
                    fireEventFuture.complete(((MakeIndexAvailableEventParameters) parameters).indexId());
                } catch (Throwable t) {
                    fireEventFuture.completeExceptionally(t);
                }
            }

            return falseCompletedFuture();
        });

        String tableName = TABLE_NAME + "_new";

        createSomeTable(tableName);

        assertThat(fireEventFuture, willBe(notNullValue()));

        assertEquals(indexId(pkIndexName(tableName)), fireEventFuture.join());
    }

    @Test
    void testPkAvailableOnCreateIndexEvent() {
        CompletableFuture<Void> fireEventFuture = new CompletableFuture<>();

        manager.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                fireEventFuture.completeExceptionally(exception);
            } else {
                try {
                    CreateIndexEventParameters createIndexEventParameters = (CreateIndexEventParameters) parameters;

                    assertTrue(createIndexEventParameters.indexDescriptor().available());

                    fireEventFuture.complete(null);
                } catch (Throwable t) {
                    fireEventFuture.completeExceptionally(t);
                }
            }

            return falseCompletedFuture();
        });

        createSomeTable(TABLE_NAME);

        assertThat(fireEventFuture, willCompleteSuccessfully());
    }

    @Test
    void testGetIndexesForTables() {
        String tableName0 = TABLE_NAME + 0;
        String tableName1 = TABLE_NAME + 1;

        createSomeTable(tableName0);
        createSomeTable(tableName1);

        createSomeIndex(tableName1, INDEX_NAME);

        int catalogVersion = manager.latestCatalogVersion();

        // Let's check for a non-existent table.
        assertThat(tableIndexIds(catalogVersion, Integer.MAX_VALUE), empty());

        // Let's check for an existing tables.
        int tableId0 = tableId(tableName0);
        int tableId1 = tableId(tableName1);

        assertThat(tableIndexIds(catalogVersion, tableId0), hasItems(indexId(pkIndexName(tableName0))));
        assertThat(tableIndexIds(catalogVersion, tableId1), hasItems(indexId(pkIndexName(tableName1)), indexId(INDEX_NAME)));
    }

    @Test
    void testGetIndexesForTableInSortedOrderById() {
        createSomeTable(TABLE_NAME);

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;

        createSomeIndex(TABLE_NAME, indexName0);
        createSomeIndex(TABLE_NAME, indexName1);

        int indexId0 = indexId(pkIndexName(TABLE_NAME));
        int indexId1 = indexId(indexName0);
        int indexId2 = indexId(indexName1);

        int catalogVersion = manager.latestCatalogVersion();

        assertThat(tableIndexIds(catalogVersion, tableId(TABLE_NAME)), equalTo(List.of(indexId0, indexId1, indexId2)));
    }

    private CompletableFuture<Void> changeColumn(
            String tab,
            String col,
            @Nullable TestColumnTypeParams typeParams,
            @Nullable Boolean notNull,
            @Nullable Supplier<DefaultValue> dflt
    ) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder()
                .schemaName(SCHEMA_NAME)
                .tableName(tab)
                .columnName(col);

        if (notNull != null) {
            builder.nullable(!notNull);
        }

        if (dflt != null) {
            builder.deferredDefaultValue(ignore -> dflt.get());
        }

        if (typeParams != null) {
            builder.type(typeParams.type);

            if (typeParams.precision != null) {
                builder.precision(typeParams.precision);
            }

            if (typeParams.length != null) {
                builder.length(typeParams.length);
            }

            if (typeParams.scale != null) {
                builder.scale(typeParams.scale);
            }
        }

        return manager.execute(builder.build());
    }

    private static CatalogCommand simpleIndex() {
        return createSortedIndexCommand(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST));
    }

    private static class TestColumnTypeParams {
        private final ColumnType type;
        private final Integer precision;
        private final Integer length;
        private final Integer scale;

        private TestColumnTypeParams(ColumnType type) {
            this(type, null, null, null);
        }

        private TestColumnTypeParams(ColumnType type, @Nullable Integer precision, @Nullable Integer length, @Nullable Integer scale) {
            this.type = type;
            this.precision = precision;
            this.length = length;
            this.scale = scale;
        }
    }

    private @Nullable CatalogTableDescriptor table(int catalogVersion, String tableName) {
        return manager.schema(catalogVersion).table(tableName);
    }

    private @Nullable CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return manager.schema(catalogVersion).index(indexName);
    }

    private int tableId(String tableName) {
        CatalogTableDescriptor table = manager.table(tableName, clock.nowLong());

        assertNotNull(table, tableName);

        return table.id();
    }

    private int indexId(String indexName) {
        CatalogIndexDescriptor index = manager.index(indexName, clock.nowLong());

        assertNotNull(index, indexName);

        return index.id();
    }

    private void createSomeTable(String tableName) {
        assertThat(
                manager.execute(createTableCommand(
                        tableName,
                        List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                        List.of("key1"),
                        List.of("key1")
                )),
                willCompleteSuccessfully()
        );
    }

    private void createSomeIndex(String tableName, String indexName) {
        assertThat(
                manager.execute(createHashIndexCommand(tableName, indexName, false, List.of("key1"))),
                willCompleteSuccessfully()
        );
    }

    private List<Integer> tableIndexIds(int catalogVersion, int tableId) {
        return manager.indexes(catalogVersion, tableId).stream().map(CatalogObjectDescriptor::id).collect(toList());
    }
}
