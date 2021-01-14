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

package org.apache.ignite.internal.schema.modification;

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

class AlterColumnBuilderImpl implements AlterColumnBuilder {

    private final TableModificationBuilderImpl tableBuilder;

    public AlterColumnBuilderImpl(TableModificationBuilderImpl tableBuilder) {
        this.tableBuilder = tableBuilder;
    }

    @Override public AlterColumnBuilder withNewName(String newName) {
        return this;
    }

    @Override public AlterColumnBuilder convertTo(ColumnType newType) {
        return this;
    }

    @Override public AlterColumnBuilder withNewDefault(Object defaultValue) {
        return this;
    }

    @Override public AlterColumnBuilder asNullable() {
        return this;
    }

    @Override public AlterColumnBuilder asNonNullable(Object replacement) {
        return this;
    }

    @Override public TableModificationBuilder done() {
        return tableBuilder;
    }
}
