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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.BitSet;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Converts rows to execution engine representation.
 */
public class TableRowConverterImpl implements TableRowConverter {

    private final SchemaRegistry schemaRegistry;

    private final SchemaDescriptor schemaDescriptor;

    private final TableDescriptor desc;

    /** Constructor. */
    public TableRowConverterImpl(SchemaRegistry schemaRegistry, SchemaDescriptor schemaDescriptor, TableDescriptor desc) {
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaDescriptor;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow binaryRow,
            RowHandler.RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns
    ) {
        RowHandler<RowT> handler = factory.handler();

        assert handler == ectx.rowHandler();

        Row row = schemaRegistry.resolve(binaryRow, schemaDescriptor);

        if (requiredColumns == null) {
            return factory.wrap(row);
        }

        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(requiredColumns.cardinality());

        BinaryTupleSchema binarySchema = row.binaryTupleSchema();

        for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
            int index = desc.columnDescriptor(j).physicalIndex();

            BinaryRowConverter.appendValue(
                    tupleBuilder,
                    binarySchema.element(index),
                    binarySchema.value(row, index)
            );
        }

        return factory.wrap(new BinaryTuple(requiredColumns.cardinality(), tupleBuilder.build()));
    }
}
