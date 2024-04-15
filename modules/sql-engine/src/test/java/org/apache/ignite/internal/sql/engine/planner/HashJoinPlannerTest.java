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

package org.apache.ignite.internal.sql.engine.planner;

import static org.apache.ignite.internal.sql.engine.planner.CorrelatedSubqueryPlannerTest.createTestTable;

import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;

import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class HashJoinPlannerTest extends AbstractPlannerTest {
    private static final String[] disabledRules = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin"};

    @ParameterizedTest()
    @MethodSource("joinConditions")
    @SuppressWarnings("ThrowableNotThrown")
    public void hashJoinAppliedConditions(String sql, boolean canBePlanned) throws Exception {
        IgniteTable tbl = createTestTable("ID", "C1");

        IgniteSchema schema = createSchema(tbl);

        if (canBePlanned) {
            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), disabledRules);
        } else {
            IgniteTestUtils.assertThrowsWithCause(() -> physicalPlan(sql, schema, disabledRules),
                    CannotPlanException.class,
                    "There are not enough rules");
        }
    }

    private static Stream<Arguments> joinConditions() {
        return Stream.of(
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = t2.c1;", true),
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = ?;", false),
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = OCTET_LENGTH('TEST');", false),
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = t2.c1 and t1.ID > t2.ID;", false),
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = 1;", false),
                Arguments.of("select t1.c1 from t1 full outer join t1 t2 on t1.c1 = t2.c1 and t1.c1 = 1;", false)
        );
    }
}
