/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.IntFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Restart one node test.
 */
public class ItRestartTest extends AbstractBasicIntegrationTest {

    /** {@inheritDoc} */
    @Override
    protected int nodes() {
        return 4;
    }

    /**
     * The test.
     */
    @Test
    public void test() {
        createTableWithData(CLUSTER_NODES.get(0), "t1", String::valueOf);

//        assertQry("select * from t1")
//                .returns(0, "0")
//                .check();

        String igniteName = CLUSTER_NODES.get(nodes() - 1).name();

        log.info("Stopping the node.");
        IgnitionManager.stop(igniteName);

        createTableWithData(CLUSTER_NODES.get(0), "t2", String::valueOf);

        log.info("Starting the node.");
        Ignite newNode = IgnitionManager.start(igniteName, null, WORK_DIR.resolve(igniteName));

        CLUSTER_NODES.set(nodes() - 1, newNode);

        checkTableWithData(CLUSTER_NODES.get(0), "t1", String::valueOf);
        checkTableWithData(CLUSTER_NODES.get(0), "t1", String::valueOf);

        checkTableWithData(CLUSTER_NODES.get(nodes() - 1), "t1", String::valueOf);
        checkTableWithData(CLUSTER_NODES.get(nodes() - 1), "t2", String::valueOf);

        /*assertQry("select * from t1")
            .returns(0, "0")
            .check();

        assertQry("select * from t2")
            .returns(0, "0")
            .check();*/
    }

    /**
     * Execute an SQL query.
     *
     * @param qry SQL query.
     * @return Checker.
     */
    private QueryChecker assertQry(String qry) {
        return new QueryChecker(qry) {
            @Override
            protected QueryProcessor getEngine() {
                return ((IgniteImpl) CLUSTER_NODES.get(nodes() - 1)).queryEngine();
            }
        };
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     */
    private void createTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", name).columns(
                SchemaBuilders.column("id", ColumnType.INT32).build(),
                SchemaBuilders.column("name", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("id")
                        .build()
        ).build();

        Table table = ignite.tables().createTable(
                scmTbl1.canonicalName(),
                tbl -> SchemaConfigurationConverter.convert(scmTbl1, tbl).changePartitions(10).changeReplicas(nodes()));

        for (int i = 0; i < 1; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", valueProducer.apply(i));

            table.keyValueView().put(null, key, val);
        }
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite        Ignite.
     * @param valueProducer Producer to predict a value.
     */
    private void checkTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        Table table = ignite.tables().table("PUBLIC." + name);

        assertNotNull(table);

        for (int i = 0; i < 1; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i));

            assertEquals(valueProducer.apply(i), row.stringValue("name"));
        }
    }
}
