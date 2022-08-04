package org.apache.ignite.internal.configuration;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
@Fork(0)
public class IgniteBenchmarkClientOnly {

    KeyValueView<Tuple, Tuple> kvView1;

    QueryProcessor q;

    @State(Scope.Benchmark)
    public static class SqlState {
        public String queryStr = query();

        private int[] id = new int[]{0};

        public int id() {
            return id[0]++;
        }

        public static String query() {
            String val = "a".repeat(100);
            var fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + String.valueOf(i)).collect(joining(","));
            var valQ = IntStream.range(1, 11).mapToObj(i -> "'" + val + "'").collect(joining(","));

            return String.format("insert into usertable(%s, %s)", "ycsb_key", fieldsQ) + "values(%s, " + String.format("%s);", valQ);
        }
    }

    @State(Scope.Benchmark)
    public static class JDBCState {
        public String queryStr = query();

        private int[] id = new int[]{0};

        public Connection conn;

        public PreparedStatement stmt;

        public JDBCState() {
            try {
                conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

                stmt = conn.prepareStatement(queryStr);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public int id() {
            return id[0]++;
        }

        public static String query() {
            String val = "a".repeat(10);
            var fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + String.valueOf(i)).collect(joining(","));
            var valQ = IntStream.range(1, 11).mapToObj(i -> "'" + val + "'").collect(joining(","));

            return String.format("insert into usertable(%s, %s)", "ycsb_key", fieldsQ) + "values(?, " + String.format("%s);", valQ);
        }

    }

    @State(Scope.Benchmark)
    public static class KVState {
        public Tuple tuple = tuple();

        private int[] id = new int[]{0};

        public int id() {
            return id[0]++;
        }

        public static Tuple tuple() {
            String[] fields = new String[10];
            Tuple t = Tuple.create();
            for (int i = 0; i < 10; i++) {
                fields[i] = "a".repeat(100);
                t.set("field" + 1, "a".repeat(100));
            }
            return t;
        }
    }

    /**
     * Before each.
     */
    @Setup
    public void setUp() throws IOException {

        try (var conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/")) {

            try (var stmt = conn.prepareStatement("DROP TABLE IF EXISTS usertable")) {
                if (!stmt.execute()) {
//                    throw new RuntimeException("Can't drop table");
                }
            }

            var createStr =
                    "CREATE TABLE usertable (\n"
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
            try (var stmt = conn.prepareStatement(createStr)) {
                if (!stmt.execute()) {
//                    throw new RuntimeException("Couldn't create the table");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * After each.
     */
    @TearDown
    public void tearDown() throws Exception {
    }


    @Benchmark
//    @Fork(warmups = 1)
    @Warmup(iterations = 2)
    public void sqlInsert(SqlState state) {
        q.queryAsync("PUBLIC", String.format(state.queryStr, state.id())).get(0).join();
    }

    @Benchmark
    @Fork(warmups = 1)
    @Warmup(iterations = 2)
    public void kvInsert(KVState state) {
        kvView1.put(null, Tuple.create().set("ycsb_key", state.id()), state.tuple);
    }

    @Benchmark
//    @Fork(warmups = 1)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    public void jdbcInsert(JDBCState state) throws SQLException {
        state.stmt.setInt(1, state.id());
        state.stmt.executeUpdate();
    }
}
