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

package org.apache.ignite.internal.client.sql;

import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;

/**
 * Column origin.
 */
public class ClientColumnOrigin implements ColumnMetadata.ColumnOrigin {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Column name. */
    private final String columnName;

    /**
     * Constructor.
     */
    public ClientColumnOrigin(
            ClientMessageUnpacker unpacker,
            String cursorColumnName,
            List<ColumnMetadata> prevColumns) {
        this.columnName = unpacker.tryUnpackNil() ? cursorColumnName : unpacker.unpackString();

        int schemaNameIdx = unpacker.tryUnpackInt(-1);

        //noinspection ConstantConditions
        this.schemaName = schemaNameIdx == -1
                ? unpacker.unpackString()
                : prevColumns.get(schemaNameIdx).origin().schemaName();

        int tableNameIdx = unpacker.tryUnpackInt(-1);

        //noinspection ConstantConditions
        this.tableName = tableNameIdx == -1
                ? unpacker.unpackString()
                : prevColumns.get(tableNameIdx).origin().tableName();
    }

    /** {@inheritDoc} */
    @Override
    public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override
    public String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override
    public String columnName() {
        return columnName;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ClientColumnOrigin.class,  this);
    }
}
