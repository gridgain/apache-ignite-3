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

package org.apache.ignite.internal.jdbc;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NULL;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcResultSetMetadata2 implements ResultSetMetaData {
    /** Column width. */
    private static final int COL_WIDTH = 30;

    /** Table names. */
    private final List<ColumnMetadata> meta;

    private final ResultSetMetadata metadata;

    /**
     * Constructor.
     *
     * @param meta Metadata.
     */
    JdbcResultSetMetadata2(ResultSetMetadata meta) {
        assert meta != null;

        this.metadata = meta;
        this.meta = meta.columns();
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnCount() throws SQLException {
        return meta.size();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAutoIncrement(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCaseSensitive(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSearchable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCurrency(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int isNullable(int col) throws SQLException {
        return meta.get(col - 1).nullable() ? columnNullable : columnNoNulls;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSigned(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnDisplaySize(int col) throws SQLException {
        return COL_WIDTH;
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnLabel(int col) throws SQLException {
        return meta.get(col - 1).name();
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnName(int col) throws SQLException {
        ColumnOrigin origin = meta.get(col - 1).origin();

        if (origin != null) {
            return origin.columnName();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String getSchemaName(int col) throws SQLException {
        ColumnOrigin origin = meta.get(col - 1).origin();

        if (origin != null) {
            return origin.schemaName();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int getPrecision(int col) throws SQLException {
        return meta.get(col - 1).precision();
    }

    /** {@inheritDoc} */
    @Override
    public int getScale(int col) throws SQLException {
        return meta.get(col - 1).scale();
    }

    /** {@inheritDoc} */
    @Override
    public String getTableName(int col) throws SQLException {
        ColumnOrigin origin = meta.get(col - 1).origin();

        if (origin != null) {
            return origin.tableName();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String getCatalogName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnType(int col) throws SQLException {
        return typeId(meta.get(col - 1).type());
    }

    private static int typeId(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN: return BOOLEAN;
            case INT8: return TINYINT;
            case INT16: return SMALLINT;
            case INT32: return INTEGER;
            case INT64: return BIGINT;
            case FLOAT: return REAL;
            case DOUBLE: return DOUBLE;
            case STRING: return VARCHAR;
            case BYTE_ARRAY: return VARBINARY;
            case TIME: return TIME;
            case DATETIME: return TIMESTAMP;
            case DATE: return DATE;
            case DECIMAL: return DECIMAL;
            case NULL: return NULL;
            case UUID:
            case PERIOD:
            case DURATION:
            case TIMESTAMP:
                // IgniteCustomType: JDBC spec allows database dependent type name. See DatabaseMetadata::getColumns (TYPE_NAME column);
                // So include JDBC TYPE_NAME of your type otherwise its type name is going to be OTHER.
                return OTHER;
            default:
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Unknown column type: " + columnType);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnTypeName(int col) throws SQLException {
        return typeName(meta.get(col - 1).type());
    }

    /**
     * Converts column type to SQL type name.
     *
     * @param columnType Column type.
     * @return SQL type name.
     */
    private static String typeName(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN: return "BOOLEAN";
            case INT8: return "TINYINT";
            case INT16: return "SMALLINT";
            case INT32: return "INTEGER";
            case INT64: return "BIGINT";
            case FLOAT: return "REAL";
            case DOUBLE: return "DOUBLE";
            case STRING: return "VARCHAR";
            case BYTE_ARRAY: return "VARBINARY";
            case TIME: return "TIME";
            case DATETIME: return "TIMESTAMP";
            case TIMESTAMP: return "TIMESTAMP WITH LOCAL TIME ZONE";
            case DATE: return "DATE";
            case DECIMAL: return "DECIMAL";
            case NULL: return "NULL";
            case UUID: return "UUID";
            case PERIOD:
            case DURATION:
                // IgniteCustomType: JDBC spec allows database dependent type name. See DatabaseMetadata::getColumns (TYPE_NAME column);
                // So include JDBC TYPE_NAME of your type otherwise its type name is going to be OTHER.
                return "OTHER";
            default:
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Unknown column type: " + columnType);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDefinitelyWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnClassName(int col) throws SQLException {
        return meta.get(col - 1).type().javaClass().getName();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new SQLException("Result set meta data is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcResultSetMetadata2.class);
    }
}
