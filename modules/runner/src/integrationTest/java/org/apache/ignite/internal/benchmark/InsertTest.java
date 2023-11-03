package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.newBuilder;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class InsertTest extends IgniteIntegrationTest {
    private static final int PORT = NetworkConfigurationSchema.DEFAULT_PORT;

    private static final String NODE_NAME = "node" + PORT;

    protected static final String FIELD_VAL = "a".repeat(100);

    protected static final String TABLE_NAME = "usertable";

    protected static IgniteImpl clusterNode;

    private static boolean fsync = false;

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @BeforeAll
    public static final void nodeSetUp() throws IOException {
        Path workDir = Files.createTempDirectory("tmpDirPrefix").toFile().toPath();

        @Language("HOCON")
        String config = "network: {\n"
                + "  nodeFinder:{\n"
                + "    netClusterNodes: [ \"localhost:" + PORT + "\"] \n"
                + "  }\n"
                + "},"
                + "raft.fsync = " + fsync;

        var fut =  TestIgnitionManager.start(NODE_NAME, config, workDir.resolve(NODE_NAME));

        TestIgnitionManager.init(new InitParametersBuilder()
                .clusterName("cluster")
                .destinationNodeName(NODE_NAME)
                .cmgNodeNames(List.of(NODE_NAME))
                .metaStorageNodeNames(List.of(NODE_NAME))
                .build()
        );

        clusterNode = (IgniteImpl) fut.join();

        var queryEngine = clusterNode.queryEngine();

        SessionId sessionId = queryEngine.createSession(newBuilder()
                .set(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                .set(QueryProperty.QUERY_TIMEOUT, TimeUnit.SECONDS.toMillis(60))
                .build()
        );

        var sql = "CREATE TABLE " + TABLE_NAME + "(\n"
                + "    ycsb_key int PRIMARY KEY,\n"
                + "    field1   varchar(100),\n"
                + "    field2   varchar(100),\n"
                + "    field3   varchar(100),\n"
                + "    field4   varchar(100),\n"
                + "    field5   varchar(100),\n"
                + "    field6   varchar(100),\n"
                + "    field7   varchar(100),\n"
                + "    field8   varchar(100),\n"
                + "    field9   varchar(100),\n"
                + "    field10  varchar(100)\n"
                + ");";

        try {
            var context = QueryContext.create(SqlQueryType.SINGLE_STMT_TYPES);

            getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, clusterNode.transactions(), sql))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    @Test
    void test() throws Exception {
        Tuple tuple = Tuple.create();

        for (int i = 1; i < 11; i++) {
            tuple.set("field" + i, FIELD_VAL);
        }

        AtomicInteger id = new AtomicInteger();

        KeyValueView<Tuple, Tuple> kvView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        // warmup
        for (int i = 1; i < 1000; i++) {
            kvView.put(null, Tuple.create().set("ycsb_key", id.getAndIncrement()), tuple);
        }

        for (int i = 1; i < 10; i++) {
            rootSpan("put", (parentSpan) -> {
                var begin = System.nanoTime();
                kvView.put(null, Tuple.create().set("ycsb_key", id.getAndIncrement()), tuple);
                System.out.println("With tracing " + (System.nanoTime() - begin)/1000.0 + " us");
                return null;
            });
        }

        for (int i = 1; i < 10; i++) {
            var begin = System.nanoTime();
            kvView.put(null, Tuple.create().set("ycsb_key", id.getAndIncrement()), tuple);
            System.out.println("Without tracing " + (System.nanoTime() - begin)/1000.0 + " us");
        }
    }

    @AfterAll
    public static final void nodeTearDown() {
        IgnitionManager.stop(NODE_NAME);
    }
}
