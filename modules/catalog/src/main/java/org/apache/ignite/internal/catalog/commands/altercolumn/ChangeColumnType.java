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

package org.apache.ignite.internal.catalog.commands.altercolumn;

import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_DDL_OPERATION_ERR;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Changes {@code type} of the column descriptor according to the {@code ALTER COLUMN SET DATA TYPE} command.
 */
public class ChangeColumnType implements ColumnChangeAction {
    public static final int UNDEFINED_PRECISION = -1;
    public static final int UNDEFINED_SCALE = Integer.MIN_VALUE;
    private static final String UNSUPPORTED_TYPE = "Cannot change data type for column '{}' [from={}, to={}].";
    private static final String UNSUPPORTED_SCALE = "Cannot change scale for column '{}' [from={}, to={}].";
    private static final String UNSUPPORTED_LENGTH = "Cannot decrease length for column '{}' [from={}, to={}].";
    private static final String UNSUPPORTED_PRECISION = "Cannot {} precision for column '{}' [from={}, to={}].";

    private static final Map<ColumnType, Set<ColumnType>> supportedTransitions = new EnumMap<>(ColumnType.class);

    static {
        supportedTransitions.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        supportedTransitions.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        supportedTransitions.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        supportedTransitions.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));
    }

    private final ColumnType type;

    private final int precision;

    private final int scale;

    /** Constructor. */
    public ChangeColumnType(ColumnType type) {
        this(type, UNDEFINED_PRECISION, UNDEFINED_SCALE);
    }

    /** Constructor. */
    public ChangeColumnType(ColumnType type, int precision, int scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public Priority priority() {
        return Priority.DATA_TYPE;
    }

    @Override
    public @Nullable TableColumnDescriptor apply(TableColumnDescriptor source) {
        if (source.type() == type
                && (precision == UNDEFINED_PRECISION || source.precision() == precision)
                && (scale == UNDEFINED_SCALE || source.scale() == scale)) {
            // No-op.
            return null;
        }

        if (source.type() != type) {
            Set<ColumnType> supportedTypes = supportedTransitions.get(source.type());

            if (supportedTypes == null || !supportedTypes.contains(type)) {
                throwUnsupportedTypeChange(source);
            }
        }

        if (precision != UNDEFINED_PRECISION) {
            if (type == ColumnType.STRING) {
                if (precision < source.length()) {
                    throwException(UNSUPPORTED_LENGTH, source.name(), source.length(), precision);
                }
            } else if (type == ColumnType.DECIMAL) {
                if (precision < source.precision()) {
                    throwException(UNSUPPORTED_PRECISION, "decrease", source.name(), source.precision(), precision);
                }
            } else {
                throwException(UNSUPPORTED_PRECISION, "change", source.name(), source.precision(), precision);
            }
        }

        if (type == ColumnType.STRING) {
            if (precision != UNDEFINED_PRECISION && precision < source.length()) {
                throwException(UNSUPPORTED_LENGTH, source.name(), source.length(), precision);
            }
        } else if (precision != UNDEFINED_PRECISION && precision < source.precision()) {
            throwException(UNSUPPORTED_PRECISION, source.name(), source.precision(), precision);
        }

        if (scale != UNDEFINED_SCALE && source.scale() != scale) {
            throwException(UNSUPPORTED_SCALE, source.name(), source.scale(), scale);
        }

        return new TableColumnDescriptor(source.name(), type, source.nullable(), source.defaultValue(), precision, scale, precision);
    }

    private void throwUnsupportedTypeChange(TableColumnDescriptor source) {
        throwException(UNSUPPORTED_TYPE, source.name(), source.type(), type);
    }

    private static void throwException(String msg, Object... params) {
        throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR, IgniteStringFormatter.format(msg, params));
    }
}
