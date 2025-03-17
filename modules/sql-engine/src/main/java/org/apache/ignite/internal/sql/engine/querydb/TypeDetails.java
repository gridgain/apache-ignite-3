package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.ParameterType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;

public final class TypeDetails {

    private final String name;

    private final int precision;

    private final int scale;

    private TypeDetails(String name, int precision, int scale) {
        this.name = name;
        this.precision = precision;
        this.scale = scale == RelDataType.SCALE_NOT_SPECIFIED ? -1 : scale;
    }

    public String name() {
        return name;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    static TypeDetails getParamType(ParameterType parameterType) {
        RelDataType relType;
        if (parameterType.columnType() != ColumnType.NULL) {
            NativeType nativeType = TypeUtils.columnType2NativeType(parameterType.columnType(),
                    parameterType.precision(),
                    parameterType.scale(),
                    parameterType.precision()
            );
            relType = TypeUtils.native2relationalType(Commons.typeFactory(), nativeType);
        } else {
            relType = Commons.typeFactory().createSqlType(SqlTypeName.NULL);
        }
        return getType(relType);
    }

    static TypeDetails getType(RelDataType dataType) {
        StringBuilder sb = new StringBuilder();

        if (dataType.isStruct()) {
            for (var field : dataType.getFieldList()) {
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                RelDataType type = field.getType();
                sb.append(getTypeName(type));
            }
            return new TypeDetails(sb.toString(), -1, -1);
        } else {
            return new TypeDetails(getTypeName(dataType), dataType.getPrecision(), dataType.getScale());
        }
    }

    private static String getTypeName(RelDataType type) {
        if (type instanceof IgniteCustomType) {
            IgniteCustomType customType = (IgniteCustomType) type;
            return customType.getCustomTypeName();
        } else {
            return type.getSqlTypeName().name();
        }
    }
}
