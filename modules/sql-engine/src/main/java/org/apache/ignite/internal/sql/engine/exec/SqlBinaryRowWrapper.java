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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Sql binary row wrapper.
 */
public class SqlBinaryRowWrapper implements SqlRowWrapper {
    private final InternalTuple row;

    private final RowSchema rowSchema;

    SqlBinaryRowWrapper(RowSchema rowSchema, ByteBuffer buf) {
        this.rowSchema = rowSchema;
        this.row = new BinaryTuple(rowSchema.fields().size(), buf);
    }

    SqlBinaryRowWrapper(RowSchema rowSchema, InternalTuple row) {
        this.row = row;
        this.rowSchema = rowSchema;
    }

    @Override
    public int columnsCount() {
        return rowSchema.fields().size();
    }

    @Override
    public @Nullable Object get(int i) {
        Object val = SqlRowSchemaConverterUtils.readRow(rowSchema, i, row, i);

        return TypeUtils.toInternal(val);
    }

    @Override
    public void set(int i, Object v) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return row.byteBuffer();
    }

    @Override
    public RowSchema rowSchema() {
        return rowSchema;
    }
}
