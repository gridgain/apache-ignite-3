package org.apache.ignite.internal.runner.app;


import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class ItBenchmarkTest {
    private static final int[] PORTS = {3344};

    private final List<Ignite> clusterNodes = new ArrayList<>();

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo, @WorkDirectory Path workDir) {
        var futures = new ArrayList<CompletableFuture<Ignite>>();

        for (int port : PORTS) {
            String nodeName = testNodeName(testInfo, port);

            String config = "{\n"
                    + "  network: {\n"
                    + "    port: " + port + ",\n"
                    + "    nodeFinder:{\n"
                    + "      netClusterNodes: [ \"localhost:3344\"] \n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

            futures.add(IgnitionManager.start(nodeName, config, workDir.resolve(nodeName)));
        }

        String metaStorageNode = testNodeName(testInfo, PORTS[0]);

        IgnitionManager.init(metaStorageNode, List.of(metaStorageNode), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown(TestInfo testInfo) throws Exception {
        List<AutoCloseable> closeables = Arrays.stream(PORTS)
                .mapToObj(port -> testNodeName(testInfo, port))
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    @Test
    void testYCSBLikeKV() {
        ((IgniteImpl) clusterNodes.get(0)).queryEngine().queryAsync("PUBLIC", ""
                + "CREATE TABLE usertable (\n"
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
                + ");").get(0).join();
        String[] fields = new String[10];
        for (int i = 0; i < 10; i++) {
            fields[i] = "a".repeat(100);
        }

        Table tbl1 = clusterNodes.get(0).tables().table("PUBLIC.usertable");
        KeyValueView<Tuple, Tuple> kvView1 = tbl1.keyValueView();

        int id = 0;
        for (int i = 0; i < 10000; i++) {
            Tuple t = Tuple.create();
            for (int j = 0; j < 10; j++) {
                t.set("field" + 1, fields[j]);
            }
            kvView1.put(null, Tuple.create().set("ycsb_key", id++), t);
        }

        measure("Key-Value reads: ", () -> {
            final int[] idx = {0};
            for (int i = 0; i < 10000; i++) {
                measure("Read: ", () -> {
                    kvView1.get(null, Tuple.create().set("ycsb_key", idx[0]++));
                });
            }
        });

        measure("Sql reads: ", () -> {
            final int[] idx = {0};
            for (int i = 0; i < 10000; i++) {
                measure("SQL Read: ", () -> {
                    var selectSql = "select * from usertable where ycsb_key = " + idx[0]++;
                    getAllFromCursor(
                            await(((IgniteImpl) clusterNodes.get(0)).queryEngine().queryAsync("PUBLIC", selectSql).get(0))).get(0).get(0);
                });
            }
        });
    }


    @Test
    void testReadDegradationSqlApi() {
        ((IgniteImpl) clusterNodes.get(0)).queryEngine().queryAsync("PUBLIC", ""
                + "create table tbl1(key int primary key, val varchar)").get(0).join();

        Table tbl1 = clusterNodes.get(0).tables().table("PUBLIC.tbl1");
        KeyValueView<Tuple, Tuple> kvView1 = tbl1.keyValueView();

        var longStr = "a".repeat(16_000);

        var selectSql = "select val from tbl1 where key = 1";

        final int[] id = {0};

        for (int k = 0; k < 1000; k++) {
            final var j = k;
            measure("Inserts take: ", () -> {
                        for (int i = 0; i < 1000; i++) {
                            kvView1.put(null, Tuple.create().set("key", id[0]++), Tuple.create().set("val", longStr));
                        }
                    });

            measure("Key-value get takes: ", () -> {
                assert kvView1.get(null, Tuple.create().set("key", 1)).value("val").equals(longStr);
            });

            measure("SQL select takes: ", () -> {
                assert getAllFromCursor(
                        await(((IgniteImpl) clusterNodes.get(0)).queryEngine().queryAsync("PUBLIC", selectSql).get(0))).get(0).get(0).equals(
                        longStr);
            });
        }
    }

    private void measure(String prefix, Runnable block) {
        var begin = System.nanoTime();
        block.run();
        System.out.println(prefix + (System.nanoTime() - begin)/1000000.0 + " ms");
    }
}
