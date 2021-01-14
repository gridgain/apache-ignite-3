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

package org.apache.ignite.schema;

import org.apache.ignite.internal.schema.builder.HashIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.PartialIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.SchemaTableBuilderImpl;
import org.apache.ignite.internal.schema.builder.SortedIndexBuilderImpl;
import org.apache.ignite.internal.schema.builder.TableColumnBuilderImpl;
import org.apache.ignite.schema.builder.HashIndexBuilder;
import org.apache.ignite.schema.builder.PartialIndexBuilder;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.apache.ignite.schema.builder.SortedIndexBuilder;
import org.apache.ignite.schema.builder.TableColumnBuilder;

/**
 * Schema builder helper.
 */
public final class SchemaBuilders {
    /** Default schema name. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /**
     * Create table descriptor from classes.
     *
     * @param schemaName Schema name.
     * @param keyClass Key class.
     * @param valueClass Value class.
     * @return Table descriptor for given key-value pair.
     */
    public static SchemaTable buildSchema(String schemaName, String tableName, Class<?> keyClass, Class<?> valueClass) {
        // TODO: implement schema generation from classes.

        return null;
    }

    /**
     * Creates table descriptor builder.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @return Table descriptor builder.
     */
    public static SchemaTableBuilder tableBuilder(String schemaName,
        String tableName) {
        return new SchemaTableBuilderImpl(schemaName, tableName);
    }

    /**
     * Creates table column buidler.
     *
     * @param name Column name.
     * @return Column builder.
     */
    public static TableColumnBuilder column(String name) {
        return new TableColumnBuilderImpl(name);
    }

    /**
     * @param name Index name.
     * @return Sorted index builder.
     */
    public static SortedIndexBuilder sortedIndex(String name) {
        return new SortedIndexBuilderImpl(name);
    }

    /**
     * @param name Index name.
     * @return Partial index builder.
     */
    public static PartialIndexBuilder partialIndex(String name) {
        return new PartialIndexBuilderImpl(name);
    }

    /**
     * @param name Index name.
     * @return Hash index builder.
     */
    public static HashIndexBuilder hashIndex(String name) {
        return new HashIndexBuilderImpl(name);
    }

    // Stub.
    private SchemaBuilders() {
    }
}
