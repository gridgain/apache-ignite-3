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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.BiScalar;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.exec.exp.SingleScalar;

/**
 * Contains methods used in metadata definitions.
 */
public enum IgniteMethod {
    /** See {@link RowHandler#set(int, Object, Object)}. */
    ROW_HANDLER_SET(RowHandler.class, "set", int.class, Object.class, Object.class),

    /** See {@link RowHandler#get(int, Object)}. */
    ROW_HANDLER_GET(RowHandler.class, "get", int.class, Object.class),

    /** See {@link Commons#getFieldFromBiRows(RowHandler, int, Object, Object)}. */
    ROW_HANDLER_BI_GET(Commons.class, "getFieldFromBiRows", RowHandler.class, int.class,
            Object.class, Object.class),

    /** See {@link ExecutionContext#rowHandler()}. */
    CONTEXT_ROW_HANDLER(ExecutionContext.class, "rowHandler"),

    /** See {@link ExecutionContext#correlatedVariable(int)}. */
    CONTEXT_GET_CORRELATED_VALUE(ExecutionContext.class, "correlatedVariable", int.class),

    /** See {@link ExecutionContext#nullBound()}. */
    CONTEXT_NULL_BOUND(ExecutionContext.class, "nullBound"),

    /** See {@link ExecutionContext#getParameter(String, Type)}. */
    CONTEXT_GET_PARAMETER_VALUE(ExecutionContext.class, "getParameter", String.class, Type.class),

    /** See {@link SingleScalar#execute(ExecutionContext, Object, Object)}. */
    SCALAR_EXECUTE(SingleScalar.class, "execute", ExecutionContext.class, Object.class, Object.class),

    /** See {@link BiScalar#execute(ExecutionContext, Object, Object, Object)}. */
    BI_SCALAR_EXECUTE(BiScalar.class, "execute", ExecutionContext.class, Object.class, Object.class, Object.class),

    SYSTEM_RANGE2(IgniteSqlFunctions.class, "systemRange", Object.class, Object.class),

    SYSTEM_RANGE3(IgniteSqlFunctions.class, "systemRange", Object.class, Object.class, Object.class),

    STRING_TO_TIMESTAMP(IgniteSqlFunctions.class, "timestampStringToNumeric", String.class),

    /** See {@link SqlParserUtil#intervalToMonths(String, SqlIntervalQualifier)}. */
    PARSE_INTERVAL_YEAR_MONTH(SqlParserUtil.class, "intervalToMonths", String.class, SqlIntervalQualifier.class),

    /** See {@link SqlParserUtil#intervalToMillis(String, SqlIntervalQualifier)}. */
    PARSE_INTERVAL_DAY_TIME(SqlParserUtil.class, "intervalToMillis", String.class, SqlIntervalQualifier.class),

    /** See {@link IgniteSqlFunctions#toString(ByteString)}. */
    BYTESTRING_TO_STRING(IgniteSqlFunctions.class, "toString", ByteString.class),

    /** See {@link IgniteSqlFunctions#toByteString(String)}. */
    STRING_TO_BYTESTRING(IgniteSqlFunctions.class, "toByteString", String.class),

    /** See {@link IgniteSqlFunctions#currentTime(DataContext)}. */
    CURRENT_TIME(IgniteSqlFunctions.class, "currentTime", DataContext.class),

    /** See {@link IgniteSqlFunctions#least2(Object, Object)}. */
    LEAST2(IgniteSqlFunctions.class, "least2", Object.class, Object.class),

    /** See {@link IgniteSqlFunctions#greatest2(Object, Object)}. */
    GREATEST2(IgniteSqlFunctions.class, "greatest2", Object.class, Object.class),

    /** See {@link Objects#equals(Object, Object)}. */
    IS_NOT_DISTINCT_FROM(Objects.class, "equals", Object.class, Object.class),

    /** See {@link UUID#randomUUID()}. */
    RAND_UUID(UUID.class, "randomUUID"),

    /** See {@link IgniteSqlFunctions#genRandomUuid()}. */
    // TODO This function should removed when https://issues.apache.org/jira/browse/IGNITE-19103 is complete.
    GEN_RANDOM_UUID(IgniteSqlFunctions.class, "genRandomUuid"),

    /** See {@link IgniteSqlFunctions#consumeFirstArgument(Object, Object)}. **/
    CONSUME_FIRST_ARGUMENT(IgniteSqlFunctions.class, "consumeFirstArgument", Object.class, Object.class),

    SUBSTR(SqlFunctions.class, "substring", String.class, int.class, int.class);

    private final Method method;

    /**
     * Constructor.
     *
     * @param clazz         Class where to lookup method.
     * @param methodName    Method name.
     * @param argumentTypes Method parameters types.
     */
    IgniteMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

    /**
     * Get method.
     */
    public Method method() {
        return method;
    }
}
