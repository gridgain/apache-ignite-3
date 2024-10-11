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

package org.apache.ignite.internal.sql.engine.type;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;

/** UUID SQL type. */
public final class NumericType extends IgniteCustomType {

    /** A string name of this type: {@code UUID}. **/
    public static final String NAME = "NUMERIC";
    
    /** Type spec of this type. **/
    public static final IgniteCustomTypeSpec SPEC = new IgniteCustomTypeSpec(NAME, NativeTypes.decimalOf(100, 50),
            ColumnType.NUMERIC, BigDecimal.class, IgniteCustomTypeSpec.getCastFunction(NumericType.class, "cast"));

    public static final NumericType INSTANCE = new NumericType(false);
    public static final NumericType NULLABLE_INSTANCE = new NumericType(true);

    /** Constructor. */
    private NumericType(boolean nullable) {
        super(SPEC, nullable, PRECISION_NOT_SPECIFIED);
    }

    /** {@inheritDoc} */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME);
    }

    /** {@inheritDoc} */
    @Override
    public NumericType createWithNullability(boolean nullable) {
        return nullable ? NULLABLE_INSTANCE : INSTANCE;
    }

    @Override public SqlTypeName getSqlTypeName() {
        return SqlTypeName.DECIMAL;
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return SqlTypeFamily.NUMERIC;
    }

    /**
     * Implementation of a cast function for {@code UUID} data type.
     */
    public static UUID cast(Object value) {
        // It would be better to generate Expression tree that is equivalent to the code below
        // from type checking rules for this type in order to avoid code duplication.
        if (value instanceof String) {
            return UUID.fromString((String) value);
        } else {
            return (UUID) value;
        }
    }
}
