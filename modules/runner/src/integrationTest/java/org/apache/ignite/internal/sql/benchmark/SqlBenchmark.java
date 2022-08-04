package org.apache.ignite.internal.sql.benchmark;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.Session;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(20)
public class SqlBenchmark {
    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    private Session session;

    @TearDown
    public void tearDown() {
        for(int i = CLUSTER_NODES.size(); i > 0; i--) {
            try {
                CLUSTER_NODES.get(i-1).close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Setup
    public void setup() {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            String nodeName = "benchnode_" + i;

            String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + i, connectNodeAddr);

            futures.add(IgnitionManager.start(nodeName, config, Paths.get("target", "work", nodeName)));
        }

        String metaStorageNodeName = "benchnode_0";

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(future.join());
        }

        session = CLUSTER_NODES.get(0).sql().createSession();

        var applied = session.execute(null,
                "create table if not exists t("
                        + "id int primary key, val_1 int, val_2 int, val_3 int, val_4 int, val_5 int, val_6 int"
                        + ", val_7 int) engine pagememory with partitions=4,replicas=2").wasApplied();

        if (applied) {
            System.out.println("Going to fill the table");
            for (int i = 0; i < 1_000; i++) {
                session.execute(null, "insert into t values (?, ?, ?, ?, ?, ?, ?, ?)", i, i, i, i, i, i, i);

                if (i > 0 && i % 1000 == 0) {
                    System.out.println(i + " rows has been processed");
                }
            }
        }
    }

    @Benchmark
    public Object test() {
        return session.execute(null, "select count(*) from (select val_1 + val_2 + val_3 + val_4 + val_5 + val_6 + val_7 from t)  as t1").next();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(SqlBenchmark.class.getSimpleName()) //
                .warmupIterations(20) //
                .warmupTime(TimeValue.seconds(1)) //
                .measurementIterations(10) //
                .measurementTime(TimeValue.seconds(1)) //
                .forks(1) //
                .build();

        new Runner(opt).run();
    }
}
