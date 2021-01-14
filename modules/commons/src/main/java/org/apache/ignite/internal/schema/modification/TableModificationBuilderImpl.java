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

import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

public class TableModificationBuilderImpl implements TableModificationBuilder {

    private final SchemaTableImpl table;

    public TableModificationBuilderImpl(SchemaTableImpl table) {
        this.table = table;
    }

    @Override public TableModificationBuilder addColumn(Column column) {
        return this;
    }

    @Override public TableModificationBuilder addKeyColumn(Column column) {
        return this;
    }

    @Override public AlterColumnBuilder alterColumn(String columnName) {
        return new AlterColumnBuilderImpl(this);
    }

    @Override public TableModificationBuilder dropColumn(String columnName) {
        return this;
    }

    @Override public TableModificationBuilder addIndex(TableIndex index) {
        return this;
    }

    @Override public TableModificationBuilder dropIndex(String indexName) {
        return this;
    }

    @Override public void apply() {

    }
}
