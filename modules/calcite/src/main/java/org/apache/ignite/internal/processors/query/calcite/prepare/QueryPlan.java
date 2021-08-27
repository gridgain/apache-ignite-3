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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;

/**
 *
 */
public interface QueryPlan {
    /** Query type */
    enum Type {
        QUERY,
        FRAGMENT,
        DML,
        DDL,
        EXPLAIN;

        /**
         * @return Associated SqlQueryType.
         */
        public SqlQueryType mapPlanTypeToSqlType() {
            switch (this) {
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
                    throw new UnsupportedOperationException("Unexpected query plan type: " + name());
            }
        }
    }

    /**
     * @return Query type.
     */
    Type type();

    /**
     * Clones this plan.
     */
    QueryPlan copy();
}
