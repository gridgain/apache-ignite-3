package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class AbstractOneNodeBenchmark {
    private static final int PORT = 3344;

    private static String nodeName = "node" + PORT;

    protected static String fieldVal = "a".repeat(100);

    protected static String tableName = "usertable";

    protected static IgniteImpl clusterNode;

    @Setup
    public final void nodeSetUp() throws IOException {
        Path workDir = Files.createTempDirectory("tmpDirPrefix").toFile().toPath();

        String config = "{\n"
                + "  network: {\n"
                + "    port: " + PORT + ",\n"
                + "    nodeFinder:{\n"
                + "      netClusterNodes: [ \"localhost:" + PORT + "\"] \n"
                + "    }\n"
                + "  }\n"
                + "}";

        var fut =  IgnitionManager.start(nodeName, config, workDir.resolve(nodeName));

        IgnitionManager.init(nodeName, List.of(nodeName), "cluster");

        clusterNode = (IgniteImpl) fut.join();

        var queryEngine = clusterNode.queryEngine();

        SessionId sessionId = queryEngine.createSession(TimeUnit.SECONDS.toMillis(60), PropertiesHolder.fromMap(
                Map.of(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
        ));

        var sql = "CREATE TABLE " + tableName + "(\n"
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
            var context = QueryContext.of();

            getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, sql))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    @TearDown
    public final void nodeTearDown() throws Exception {
        IgnitionManager.stop(nodeName);
    }
}
