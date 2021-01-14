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

package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;

/**
 * Table column builder.
 */
public interface TableColumnBuilder {
    /**
     * Sets column type.
     *
     * @param columnType Column type.
     * @return {@code this} for chaining.
     */
    TableColumnBuilder withType(ColumnType columnType);

    /**
     * Mark column as nullable.
     *
     * @return {@code this} for chaining.
     */
    TableColumnBuilder asNullable();

    /**
     * Mark column as non-nullable.
     *
     * @return {@code this} for chaining.
     */
    TableColumnBuilder asNonNull();

    /**
     * Sets column default value.
     *
     * @param defValue Default value.
     * @return {@code this} for chaining.
     */
    TableColumnBuilder withDefaultValue(Object defValue);

    /**
     * Builds column.
     *
     * @return Built column.
     */
    Column build();
}

