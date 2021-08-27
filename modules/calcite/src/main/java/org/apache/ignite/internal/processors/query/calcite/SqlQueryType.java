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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;

/**
 * Possible query types.
 */
public enum SqlQueryType {
    /** Query. */
    QUERY((byte)0),

    /** Fragment. */
    FRAGMENT((byte)1),

    /** DML. */
    DML((byte)2),

    /** DDL. */
    DDL((byte)3),

    /** Explain. */
    EXPLAIN((byte)4);

    private static final Map<Byte, SqlQueryType> QRY_TYPE_INDEX;

    static {
        QRY_TYPE_INDEX = Arrays.stream(values()).collect(Collectors.toMap(SqlQueryType::id, Function.identity()));
    }

    /**
     * @param qryTypeId
     * @return
     */
    public static SqlQueryType getQryType(byte qryTypeId) {
        SqlQueryType value = QRY_TYPE_INDEX.get(qryTypeId);
        assert value != null;
        return value;
    }

    public static SqlQueryType mapPlanTypeToSqlType(QueryPlan.Type type) {
        switch (type) {
            case QUERY:
                return SqlQueryType.QUERY;
            case FRAGMENT:
                return SqlQueryType.FRAGMENT;
            case DML:
                return SqlQueryType.DML;
            case DDL:
                return SqlQueryType.DDL;
            case EXPLAIN:
                return SqlQueryType.EXPLAIN;
            default:
                throw new UnsupportedOperationException("Unexpected query plan type: " + type);
        }
    }

    /** Id. */
    private byte id;

    /**
     * @param id Id.
     */
    SqlQueryType(byte id) {
        this.id = id;
    }

    /**
     * @return id.
     */
    public byte id() {
        return id;
    }
}
