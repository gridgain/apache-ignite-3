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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.transform;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeCoercionRules;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NumberNativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * TypeUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TypeUtils {
    private static final Set<SqlTypeName> CONVERTABLE_TYPES = EnumSet.of(
            SqlTypeName.DATE,
            SqlTypeName.TIME,
            SqlTypeName.BINARY,
            SqlTypeName.VARBINARY,
            SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
            SqlTypeName.TIMESTAMP,
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            SqlTypeName.INTERVAL_SECOND,
            SqlTypeName.INTERVAL_MINUTE,
            SqlTypeName.INTERVAL_MINUTE_SECOND,
            SqlTypeName.INTERVAL_HOUR,
            SqlTypeName.INTERVAL_HOUR_MINUTE,
            SqlTypeName.INTERVAL_HOUR_SECOND,
            SqlTypeName.INTERVAL_DAY,
            SqlTypeName.INTERVAL_DAY_HOUR,
            SqlTypeName.INTERVAL_DAY_MINUTE,
            SqlTypeName.INTERVAL_DAY_SECOND,
            SqlTypeName.INTERVAL_MONTH,
            SqlTypeName.INTERVAL_YEAR,
            SqlTypeName.INTERVAL_YEAR_MONTH
    );

    private static class SupportedParamClassesHolder {
        static final Set<Class<?>> supportedParamClasses;

        static {
            supportedParamClasses = Arrays.stream(ColumnType.values()).map(ColumnType::javaClass).collect(Collectors.toSet());
            supportedParamClasses.add(boolean.class);
            supportedParamClasses.add(byte.class);
            supportedParamClasses.add(short.class);
            supportedParamClasses.add(int.class);
            supportedParamClasses.add(long.class);
            supportedParamClasses.add(float.class);
            supportedParamClasses.add(double.class);
        }
    }

    private static final Set<ColumnType> NUMERIC_COLUMN_TYPES = Set.of(
            ColumnType.INT8,
            ColumnType.INT16,
            ColumnType.INT32,
            ColumnType.INT64,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.DECIMAL
    );

    private static final Set<ColumnType> TIME_COLUMN_TYPES = Set.of(
            ColumnType.DATE,
            ColumnType.DATETIME,
            ColumnType.TIME,
            ColumnType.TIMESTAMP
    );

    private static final ZoneId UTC = ZoneId.of("UTC");

    private static Set<Class<?>> supportedParamClasses() {
        return SupportedParamClassesHolder.supportedParamClasses;
    }

    /** Return {@code true} if supplied object is suitable as dynamic parameter. */
    public static boolean supportParamInstance(@Nullable Object param) {
        return param == null || supportedParamClasses().contains(param.getClass());
    }

    /**
     * CombinedRowType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelDataType combinedRowType(IgniteTypeFactory typeFactory, RelDataType... types) {

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        Set<String> names = new HashSet<>();

        for (RelDataType type : types) {
            for (RelDataTypeField field : type.getFieldList()) {
                int idx = 0;
                String fieldName = field.getName();

                while (!names.add(fieldName)) {
                    fieldName = field.getName() + idx++;
                }

                builder.add(fieldName, field.getType());
            }
        }

        return builder.build();
    }

    /** Assembly output type from input types. */
    public static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields) {
        return createRowType(typeFactory, fields, "$F");
    }

    private static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields, String namePreffix) {
        List<String> names = IntStream.range(0, fields.size())
                .mapToObj(ord -> namePreffix + ord)
                .collect(Collectors.toList());

        return typeFactory.createStructType(fields, names);
    }

    /**
     * Function.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> Function<RowT, RowT> resultTypeConverter(ExecutionContext<RowT> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            RowHandler<RowT> handler = ectx.rowHandler();
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            RowSchema rowSchema = rowSchemaFromRelTypes(types);
            RowHandler.RowFactory<RowT> factory = handler.factory(rowSchema);
            List<Function<Object, Object>> converters = transform(types, t -> fieldConverter(ectx, t));
            return r -> {
                assert handler.columnCount(r) == converters.size();

                RowBuilder<RowT> rowBuilder = factory.rowBuilder();

                for (int i = 0; i < converters.size(); i++) {
                    Object converted = converters.get(i).apply(handler.get(i, r));
                    rowBuilder.addField(converted);
                }

                RowT newRow = rowBuilder.buildAndReset();

                assert handler.columnCount(newRow) == converters.size();

                return newRow;
            };
        }

        return Function.identity();
    }

    private static Function<Object, Object> fieldConverter(ExecutionContext<?> ectx, RelDataType fieldType) {
        Type storageType = ectx.getTypeFactory().getResultClass(fieldType);

        if (isConvertableType(fieldType)) {
            return v -> fromInternal(v, storageType);
        }

        return Function.identity();
    }

    /**
     * IsConvertableType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean isConvertableType(RelDataType type) {
        return CONVERTABLE_TYPES.contains(type.getSqlTypeName());
    }

    private static boolean hasConvertableFields(RelDataType resultType) {
        return RelOptUtil.getFieldTypeList(resultType).stream()
                .anyMatch(TypeUtils::isConvertableType);
    }

    /**
     * Converts the given value to its presentation used by the execution engine.
     */
    public static @Nullable Object toInternal(@Nullable Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class) {
            return (int) ((LocalDate) val).toEpochDay();
        } else if (storageType == LocalTime.class) {
            return (int) (TimeUnit.NANOSECONDS.toMillis(((LocalTime) val).toNanoOfDay()));
        } else if (storageType == LocalDateTime.class) {
            var dt = (LocalDateTime) val;

            return TimeUnit.SECONDS.toMillis(dt.toEpochSecond(ZoneOffset.UTC)) + TimeUnit.NANOSECONDS.toMillis(dt.getNano());
        } else if (storageType == Instant.class) {
            var timeStamp = (Instant) val;

            return timeStamp.toEpochMilli();
        } else if (storageType == Duration.class) {
            return TimeUnit.SECONDS.toMillis(((Duration) val).getSeconds())
                    + TimeUnit.NANOSECONDS.toMillis(((Duration) val).getNano());
        } else if (storageType == Period.class) {
            return (int) ((Period) val).toTotalMonths();
        } else if (storageType == byte[].class) {
            if (val instanceof String) {
                return new ByteString(((String) val).getBytes(StandardCharsets.UTF_8));
            } else if (val instanceof byte[]) {
                return new ByteString((byte[]) val);
            } else {
                assert val instanceof ByteString : "Expected ByteString but got " + val + ", type=" + val.getClass().getTypeName();
                return val;
            }
        } else if (val instanceof Number && storageType != val.getClass()) {
            // For dynamic parameters we don't know exact parameter type in compile time. To avoid casting errors in
            // runtime we should convert parameter value to expected type.
            Number num = (Number) val;

            return Byte.class.equals(storageType) || byte.class.equals(storageType) ? SqlFunctions.toByte(num) :
                    Short.class.equals(storageType) || short.class.equals(storageType) ? SqlFunctions.toShort(num) :
                            Integer.class.equals(storageType) || int.class.equals(storageType) ? SqlFunctions.toInt(num) :
                                    Long.class.equals(storageType) || long.class.equals(storageType) ? SqlFunctions.toLong(num) :
                                            Float.class.equals(storageType) || float.class.equals(storageType) ? SqlFunctions.toFloat(num) :
                                                    Double.class.equals(storageType) || double.class.equals(storageType)
                                                            ? SqlFunctions.toDouble(num) :
                                                            BigDecimal.class.equals(storageType) ? SqlFunctions.toBigDecimal(num) : num;
        } else {
            var nativeTypeSpec = NativeTypeSpec.fromClass((Class<?>) storageType);
            assert nativeTypeSpec != null : "No native type spec for type: " + storageType;

            var customType = SafeCustomTypeInternalConversion.INSTANCE.tryConvertToInternal(val, nativeTypeSpec);
            return customType != null ? customType : val;
        }
    }

    /**
     * FromInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static @Nullable Object fromInternal(@Nullable  Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class && val instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) val);
        } else if (storageType == LocalTime.class && val instanceof Integer) {
            return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(Long.valueOf((Integer) val)));
        } else if (storageType == LocalDateTime.class && (val instanceof Long)) {
            return LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds((Long) val),
                    (int) TimeUnit.MILLISECONDS.toNanos((Long) val % 1000), ZoneOffset.UTC);
        } else if (storageType == Instant.class && val instanceof Long) {
            return Instant.ofEpochMilli((long) val);
        } else if (storageType == Duration.class && val instanceof Long) {
            return Duration.ofMillis((Long) val);
        } else if (storageType == Period.class && val instanceof Integer) {
            return Period.of((Integer) val / 12, (Integer) val % 12, 0);
        } else if (storageType == byte[].class && val instanceof ByteString) {
            return ((ByteString) val).getBytes();
        } else {
            var nativeTypeSpec = NativeTypeSpec.fromClass((Class<?>) storageType);
            assert nativeTypeSpec != null : "No native type spec for type: " + storageType;

            var customType = SafeCustomTypeInternalConversion.INSTANCE.tryConvertFromInternal(val, nativeTypeSpec);
            return customType != null ? customType : val;
        }
    }

    /**
     * Convert calcite date type to Ignite native type.
     */
    public static ColumnType columnType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return ColumnType.STRING;
            case DATE:
                return ColumnType.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return ColumnType.TIME;
            case INTEGER:
                return ColumnType.INT32;
            case TIMESTAMP:
                return ColumnType.DATETIME;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ColumnType.TIMESTAMP;
            case BIGINT:
                return ColumnType.INT64;
            case SMALLINT:
                return ColumnType.INT16;
            case TINYINT:
                return ColumnType.INT8;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case DECIMAL:
                return ColumnType.DECIMAL;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case REAL:
            case FLOAT:
                return ColumnType.FLOAT;
            case BINARY:
            case VARBINARY:
            case ANY:
                if (type instanceof IgniteCustomType) {
                    IgniteCustomType customType = (IgniteCustomType) type;
                    return customType.spec().columnType();
                }
                // fallthrough
            case OTHER:
                return ColumnType.BYTE_ARRAY;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return ColumnType.PERIOD;
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
            case INTERVAL_DAY:
                return ColumnType.DURATION;
            case NULL:
                return ColumnType.NULL;
            default:
                assert false : "Unexpected type of result: " + type.getSqlTypeName();
                return null;
        }
    }

    /**
     * Converts a {@link NativeType native type} to {@link RelDataType relational type}.
     *
     * @param factory Type factory.
     * @param nativeType A native type to convert.
     * @return Relational type.
     */
    public static RelDataType native2relationalType(RelDataTypeFactory factory, NativeType nativeType) {
        switch (nativeType.spec()) {
            case BOOLEAN:
                return factory.createSqlType(SqlTypeName.BOOLEAN);
            case INT8:
                return factory.createSqlType(SqlTypeName.TINYINT);
            case INT16:
                return factory.createSqlType(SqlTypeName.SMALLINT);
            case INT32:
                return factory.createSqlType(SqlTypeName.INTEGER);
            case INT64:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return factory.createSqlType(SqlTypeName.REAL);
            case DOUBLE:
                return factory.createSqlType(SqlTypeName.DOUBLE);
            case DECIMAL:
                assert nativeType instanceof DecimalNativeType;

                var decimal = (DecimalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.DECIMAL, decimal.precision(), decimal.scale());
            case UUID:
                IgniteTypeFactory concreteTypeFactory = (IgniteTypeFactory) factory;
                return concreteTypeFactory.createCustomType(UuidType.NAME);
            case STRING: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARCHAR, varlen.length());
            }
            case BYTES: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARBINARY, varlen.length());
            }
            case BITMASK:
                // TODO IGNITE-18431.
                throw new AssertionError("BITMASK is not supported yet");
            case NUMBER:
                assert nativeType instanceof NumberNativeType;

                var number = (NumberNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.DECIMAL, number.precision(), 0);
            case DATE:
                return factory.createSqlType(SqlTypeName.DATE);
            case TIME:
                assert nativeType instanceof TemporalNativeType;

                var time = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIME, time.precision());
            case TIMESTAMP:
                assert nativeType instanceof TemporalNativeType;

                var ts = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, ts.precision());
            case DATETIME:
                assert nativeType instanceof TemporalNativeType;

                var dt = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIMESTAMP, dt.precision());
            default:
                throw new IllegalStateException("Unexpected native type " + nativeType);
        }
    }

    /**
     * Converts a {@link NativeType native type} to {@link RelDataType relational type} with respect to the nullability flag.
     *
     * @param factory Type factory.
     * @param nativeType A native type to convert.
     * @param nullable A flag that specify whether the resulting type should be nullable or not.
     * @return Relational type.
     */
    public static RelDataType native2relationalType(RelDataTypeFactory factory, NativeType nativeType, boolean nullable) {
        return factory.createTypeWithNullability(native2relationalType(factory, nativeType), nullable);
    }

    /**
     * Converts a {@link NativeType native types} to {@link RelDataType relational types}.
     *
     * @param factory Type factory.
     * @param nativeTypes A native types to convert.
     * @return Relational types.
     */
    public static List<RelDataType> native2relationalTypes(RelDataTypeFactory factory, NativeType... nativeTypes) {
        return Arrays.stream(nativeTypes).map(t -> native2relationalType(factory, t)).collect(Collectors.toList());
    }

    /** Converts {@link ColumnType} to corresponding {@link NativeType}. */
    public static NativeType columnType2NativeType(ColumnType columnType, int precision, int scale, int length) {
        switch (columnType) {
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(precision, scale);
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time(precision);
            case DATETIME:
                return NativeTypes.datetime(precision);
            case TIMESTAMP:
                return NativeTypes.timestamp(precision);
            case UUID:
                return NativeTypes.UUID;
            case BITMASK:
                return NativeTypes.bitmaskOf(length);
            case STRING:
                return NativeTypes.stringOf(length);
            case BYTE_ARRAY:
                return NativeTypes.blobOf(length);
            case NUMBER:
                return NativeTypes.numberOf(precision);
                // fallthrough
            case PERIOD:
            case DURATION:
            case NULL:
            default:
                throw new IllegalArgumentException("No NativeType for type: " + columnType);
        }
    }

    /** Checks whether cast operation is necessary in {@code SearchBound}. */
    public static boolean needCastInSearchBounds(IgniteTypeFactory typeFactory, RelDataType fromType, RelDataType toType) {
        // Checks for character and binary types should allow comparison
        // between types with precision, types w/o precision, and varying non-varying length variants.
        // Otherwise the optimizer wouldn't pick an index for conditions such as
        // col (VARCHAR(M)) = CAST(s AS VARCHAR(N) (M != N) , col (VARCHAR) = CAST(s AS VARCHAR(N))

        // No need to cast between char and varchar.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType)) {
            return false;
        }

        // No need to cast if the source type precedence list
        // contains target type. i.e. do not cast from
        // tinyint to int or int to bigint.
        if (fromType.getPrecedenceList().containsType(toType)
                && SqlTypeUtil.isIntType(fromType)
                && SqlTypeUtil.isIntType(toType)) {
            return false;
        }

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(typeFactory, fromType, toType)) {
            return false;
        }
        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);
        return true;
    }

    /**
     * Checks whether one type can be casted to another if one of type is a custom data type.
     *
     * <p>This method expects at least one of its arguments to be a custom data type.
     */
    public static boolean customDataTypeNeedCast(IgniteTypeFactory factory, RelDataType fromType, RelDataType toType) {
        IgniteCustomTypeCoercionRules typeCoercionRules = factory.getCustomTypeCoercionRules();
        if (toType instanceof IgniteCustomType) {
            IgniteCustomType to = (IgniteCustomType) toType;
            return typeCoercionRules.needToCast(fromType, to);
        } else if (fromType instanceof IgniteCustomType) {
            boolean sameType = SqlTypeUtil.equalSansNullability(fromType, toType);
            return !sameType;
        } else {
            String message = format("Invalid arguments. Expected at least one custom data type but got {} and {}", fromType, toType);
            throw new AssertionError(message);
        }
    }

    /**
     * Checks that {@code toType} and {@code fromType} have compatible type families taking into account custom data types.
     * Types {@code T1} and {@code T2} have compatible type families if {@code T1} can be assigned to {@code T2} and vice-versa.
     *
     * @see SqlTypeUtil#canAssignFrom(RelDataType, RelDataType)
     */
    public static boolean typeFamiliesAreCompatible(RelDataTypeFactory typeFactory, RelDataType toType, RelDataType fromType) {

        // Same types are always compatible.
        if (SqlTypeUtil.equalSansNullability(typeFactory, toType, fromType)) {
            return true;
        }

        // NULL is compatible with all types.
        if (fromType.getSqlTypeName() == SqlTypeName.NULL || toType.getSqlTypeName() == SqlTypeName.NULL) {
            return true;
        } else if (fromType instanceof IgniteCustomType && toType instanceof IgniteCustomType) {
            IgniteCustomType fromCustom = (IgniteCustomType) fromType;
            IgniteCustomType toCustom = (IgniteCustomType) toType;

            // IgniteCustomType: different custom data types are not compatible.
            return Objects.equals(fromCustom.getCustomTypeName(), toCustom.getCustomTypeName());
        } else if (fromType instanceof IgniteCustomType || toType instanceof IgniteCustomType) {
            // IgniteCustomType: custom data types are not compatible with other types.
            return false;
        } else if (SqlTypeUtil.canAssignFrom(toType, fromType)) {
            return SqlTypeUtil.canAssignFrom(fromType, toType);
        } else {
            return false;
        }
    }

    /** Creates an instance of {@link RowSchema} from a list of the given {@link RelDataType}s. */
    public static RowSchema rowSchemaFromRelTypes(List<RelDataType> types) {
        RowSchema.Builder fieldTypes = RowSchema.builder();

        for (RelDataType relType : types) {
            TypeSpec typeSpec = convertToTypeSpec(relType);
            fieldTypes.addField(typeSpec);
        }

        return fieldTypes.build();
    }

    private static TypeSpec convertToTypeSpec(RelDataType type) {
        boolean simpleType = type instanceof BasicSqlType;
        boolean nullable = type.isNullable();

        if (type instanceof IgniteCustomType) {
            NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(type);
            return RowSchemaTypes.nativeTypeWithNullability(nativeType, nullable);
        } else if (SqlTypeName.ANY == type.getSqlTypeName()) {
            // TODO Some JSON functions that return ANY as well : https://issues.apache.org/jira/browse/IGNITE-20163
            return new BaseTypeSpec(null, nullable);
        } else if (SqlTypeUtil.isNull(type)) {
            return RowSchemaTypes.NULL;
        } else if (simpleType) {
            NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(type);
            return RowSchemaTypes.nativeTypeWithNullability(nativeType, nullable);
        } else if (type instanceof IntervalSqlType) {
            IntervalSqlType intervalType = (IntervalSqlType) type;
            boolean yearMonth = intervalType.getIntervalQualifier().isYearMonth();

            if (yearMonth) {
                // YEAR MONTH interval is stored as number of days in ints.
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.INT32, nullable);
            } else {
                // DAY interval is stored as time as long.
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.INT64, nullable);
            }
        } else if (SqlTypeUtil.isRow(type)) {
            List<TypeSpec> fields = new ArrayList<>();

            for (RelDataTypeField field : type.getFieldList()) {
                TypeSpec fieldTypeSpec = convertToTypeSpec(field.getType());
                fields.add(fieldTypeSpec);
            }

            return new RowType(fields, type.isNullable());

        } else if (SqlTypeUtil.isMap(type) || SqlTypeUtil.isMultiset(type) || SqlTypeUtil.isArray(type)) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-20162
            //  Add collection types support
            throw new IllegalArgumentException("Collection types is not supported: " + type);
        } else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    /**
     * Convert {@link ColumnType} to string representation of SQL type.
     *
     * @param columnType Ignite type column.
     * @return String representation of SQL type.
     */
    public static String toSqlType(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN:
                return SqlTypeName.BOOLEAN.getName();
            case INT8:
                return SqlTypeName.TINYINT.getName();
            case INT16:
                return SqlTypeName.SMALLINT.getName();
            case INT32:
                return SqlTypeName.INTEGER.getName();
            case INT64:
                return SqlTypeName.BIGINT.getName();
            case FLOAT:
                return SqlTypeName.REAL.getName();
            case DOUBLE:
                return SqlTypeName.DOUBLE.getName();
            case DECIMAL:
                return SqlTypeName.DECIMAL.getName();
            case DATE:
                return SqlTypeName.DATE.getName();
            case TIME:
                return SqlTypeName.TIME.getName();
            case DATETIME:
                return SqlTypeName.TIMESTAMP.getName();
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName();
            case UUID:
                return UuidType.NAME;
            case STRING:
                return SqlTypeName.VARCHAR.getName();
            case BYTE_ARRAY:
                return SqlTypeName.VARBINARY.getName();
            case NUMBER:
                return SqlTypeName.INTEGER.getName();
            case NULL:
                return SqlTypeName.NULL.getName();
            default:
                throw new IllegalArgumentException("Unsupported type " + columnType);
        }
    }

    /**
     * Converts an SQL type name of a builtin type ({@link SqlTypeName#getName()}) or an SQL name of a custom data type to a ColumnType.
     *
     * @param name SQL type name.
     * @return Column type.
     */
    public static ColumnType toColumnTypeFromSqlName(String name) {
        if (SqlTypeName.ALL_TYPES.stream().anyMatch(n -> n.getName().equals(name))) {
            SqlTypeName sqlTypeName = SqlTypeName.valueOf(name);
            switch (sqlTypeName) {
                case BOOLEAN:
                    return ColumnType.BOOLEAN;
                case TINYINT:
                    return ColumnType.INT8;
                case SMALLINT:
                    return ColumnType.INT16;
                case INTEGER:
                    return ColumnType.INT32;
                case BIGINT:
                    return ColumnType.INT64;
                case DECIMAL:
                    return ColumnType.DECIMAL;
                case FLOAT:
                case REAL:
                    return ColumnType.FLOAT;
                case DOUBLE:
                    return ColumnType.DOUBLE;
                case DATE:
                    return ColumnType.DATE;
                case TIME:
                    return ColumnType.TIME;
                case TIMESTAMP:
                    return ColumnType.DATETIME;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return ColumnType.TIMESTAMP;
                case CHAR:
                case VARCHAR:
                    return ColumnType.STRING;
                case BINARY:
                case VARBINARY:
                    return ColumnType.BYTE_ARRAY;
                case NULL:
                    return ColumnType.NULL;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + sqlTypeName);
            }
        } else if (UuidType.NAME.equals(name)) {
            // IgniteCustomType: Add string name to ColumnType conversion.
            return ColumnType.UUID;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + name);
        }
    }

    /**
     *  Converts the given value to the specified target type.
     *
     *  <p>This function is performs conversion iif cast operation from SQL type of the given value  to
     *  SQL type of the target type is possible. Otherwise  {@link SqlException} with {@link Sql#RUNTIME_ERR RUNTIME_ERR} is thrown.
     */
    public static @Nullable Object convertValue(@Nullable Object value, ColumnType targetType, int precision, int scale) {
        return doConvert(value, targetType, precision, scale, Clock.systemDefaultZone());
    }

    @TestOnly
    public static @Nullable Object convertValue(@Nullable Object value, ColumnType targetType, int precision, int scale, Clock clock) {
        return doConvert(value, targetType, precision, scale, clock);
    }

    private static @Nullable Object doConvert(@Nullable Object value, ColumnType targetType, int precision, int scale, Clock clock) {
        // Return NULL if value is null.
        if (value == null) {
            return null;
        }

        // Convert to another type

        NativeTypeSpec spec = NativeTypeSpec.fromObject(value);
        assert spec != null : "No spec: " + value;

        ColumnType sourceType = spec.asColumnType();

        if (targetType == ColumnType.NULL) {
            throw canNotConvert(sourceType, ColumnType.NULL);
        }

        switch (targetType) {
            case BOOLEAN:
                return convertToBoolean(value, sourceType, targetType);
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                if (value instanceof String) {
                    return convertFromStringToNumeric((String) value, targetType, precision, scale);
                } else if (value instanceof Number) {
                    return convertFromNumericToNumeric((Number) value, sourceType, targetType, precision, scale);
                } else {
                    throw canNotConvert(sourceType, targetType);
                }
            case DATE:
                return convertToDate(value, targetType, sourceType);
            case TIME:
                return convertToTime(value, targetType, sourceType);
            case DATETIME:
                return convertToDatetime(value, targetType, clock, sourceType);
            case TIMESTAMP:
                return convertToTimestamp(value, targetType, sourceType);
            case UUID:
                if (sourceType == ColumnType.STRING) {
                    return UUID.fromString(value.toString());
                } else if (sourceType == ColumnType.UUID) {
                    return value;
                } else {
                    throw canNotConvert(sourceType, targetType);
                }
            case BITMASK:
                if (sourceType == ColumnType.BITMASK) {
                    return value;
                } else {
                    throw canNotConvert(sourceType, targetType);
                }
            case STRING:
                return convertToString(value, targetType, sourceType, precision);
            case BYTE_ARRAY:
                return convertToByteArray(value, targetType, sourceType, precision);
            case PERIOD:
                if (sourceType == ColumnType.PERIOD) {
                    return value;
                } else {
                    throw canNotConvert(sourceType, targetType);
                }
            case DURATION:
                if (sourceType == ColumnType.DURATION) {
                    return value;
                } else {
                    throw canNotConvert(sourceType, targetType);
                }
            case NULL:
                return value;
            case NUMBER:
            default:
                throw new IllegalArgumentException("Unexpected type: " + targetType);
        }
    }

    private static Object convertFromStringToNumeric(String value, ColumnType targetType, int precision, int scale) {
        switch (targetType) {
            case INT8:
                return Byte.valueOf(value);
            case INT16:
                return Short.valueOf(value);
            case INT32:
                return Integer.valueOf(value);
            case INT64:
                return Long.valueOf(value);
            case FLOAT:
                return Float.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case DECIMAL:
                return IgniteSqlFunctions.toBigDecimal(value, precision, scale);
            default:
                throw canNotConvert(ColumnType.STRING, targetType);
        }
    }

    private static String convertToString(Object value, ColumnType targetType, ColumnType sourceType, int precision) {
        if (sourceType == ColumnType.STRING
                || NUMERIC_COLUMN_TYPES.contains(sourceType)
                || TIME_COLUMN_TYPES.contains(sourceType)
                || sourceType == ColumnType.BOOLEAN
                || sourceType == ColumnType.UUID
        ) {
            String str = value.toString();
            if (precision < 0 || precision > str.length() || sourceType != ColumnType.STRING) {
                return str;
            } else {
                return str.substring(0, precision);
            }
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertToBoolean(Object value, ColumnType sourceType, ColumnType targetType) {
        if (value instanceof Boolean) {
            return value;
        } else if (value instanceof String) {
            String str = (String) value;
            if ("true".equalsIgnoreCase(str)) {
                return Boolean.TRUE;
            } else if ("false".equalsIgnoreCase(str)) {
                return Boolean.FALSE;
            } else {
                throw canNotConvert(sourceType, targetType);
            }
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertFromNumericToNumeric(Number value, ColumnType sourceType,
            ColumnType targetType, int precision, int scale) {

        switch (targetType) {
            case INT8:
                return IgniteMath.convertToByteExact(value.longValue());
            case INT16:
                return IgniteMath.convertToShortExact(value.longValue());
            case INT32:
                return IgniteMath.convertToIntExact(value.longValue());
            case INT64:
                return IgniteMath.convertToLongExact(value.toString());
            case FLOAT:
                return value.floatValue();
            case DOUBLE:
                return value.doubleValue();
            case DECIMAL:
                BigDecimal rs = IgniteSqlFunctions.toBigDecimal(value, precision, scale);
                assert rs != null : "Never returns null";
                return rs;
            default:
                throw new AssertionError("Unexpected type for numeric conversion: " + sourceType);
        }
    }

    private static Object convertToByteArray(Object value, ColumnType targetType, ColumnType sourceType, int precision) {
        if (sourceType == ColumnType.BYTE_ARRAY) {
            byte[] bytes = (byte[]) value;
            if (precision < 0 || precision > bytes.length) {
                return bytes;
            } else {
                return Arrays.copyOf(bytes, precision);
            }
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertToTime(Object value, ColumnType targetType, ColumnType sourceType) {
        if (sourceType == ColumnType.TIME) {
            return value;
        } else if (sourceType == ColumnType.TIMESTAMP) {
            Instant instant = (Instant) value;
            return LocalTime.ofInstant(instant, UTC);
        } else if (sourceType == ColumnType.STRING) {
            return LocalTime.parse((CharSequence) value);
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertToDate(Object value, ColumnType targetType, ColumnType sourceType) {
        if (sourceType == ColumnType.DATE) {
            return value;
        } else if (sourceType == ColumnType.TIMESTAMP) {
            Instant instant = (Instant) value;
            return LocalDate.ofInstant(instant, UTC);
        } else if (sourceType == ColumnType.STRING) {
            return LocalDate.parse((CharSequence) value);
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertToTimestamp(Object value, ColumnType targetType, ColumnType sourceType) {
        if (sourceType == ColumnType.TIMESTAMP) {
            return value;
        } else if (sourceType == ColumnType.TIME) {
            Instant instant = (Instant) value;
            return LocalTime.ofInstant(instant, UTC);
        } else if (sourceType == ColumnType.DATE) {
            Instant instant = (Instant) value;
            return LocalDate.ofInstant(instant, UTC);
        } else if (sourceType == ColumnType.STRING) {
            return Instant.parse((CharSequence) value);
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static Object convertToDatetime(Object value, ColumnType targetType, Clock clock, ColumnType sourceType) {
        if (sourceType == ColumnType.DATETIME) {
            return value;
        } else if (sourceType == ColumnType.STRING) {
            return Instant.parse(value.toString());
        } else if (sourceType == ColumnType.DATE) {
            LocalDate localDate = (LocalDate) value;
            long epochSecond = localDate.toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC);

            return Instant.ofEpochSecond(epochSecond);
        } else if (sourceType == ColumnType.TIME) {
            LocalTime localTime = (LocalTime) value;
            long epochSecond = localTime.toEpochSecond(LocalDate.now(clock), ZoneOffset.UTC);
            long nanoOfDay = localTime.toNanoOfDay();

            return Instant.ofEpochSecond(epochSecond, nanoOfDay);
        } else {
            throw canNotConvert(sourceType, targetType);
        }
    }

    private static SqlException canNotConvert(ColumnType sourceType, ColumnType targetType) {
        String fromTypeName = toSqlType(sourceType);
        String toTypeName = toSqlType(targetType);

        return new SqlException(Sql.RUNTIME_ERR, format("Cannot convert from {} to {}", fromTypeName, toTypeName));
    }
}
