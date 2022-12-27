package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SelectBenchmark extends AbstractOneNodeBenchmark {

    private Random r = new Random();

    private KeyValueView<Tuple, Tuple> keyValueView;

    @Setup
    public void setUp() throws IOException {
        int id = 0;

        keyValueView = clusterNode.tables().table(tableName).keyValueView();

        for (int i = 0; i < 1000; i++) {
            Tuple t = Tuple.create();
            for (int j = 1; j <= 10; j++) {
                t.set("field" + j, fieldVal);
            }

            keyValueView.put(null, Tuple.create().set("ycsb_key", id++), t);
        }
    }

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
    public void sqlGet(SqlState sqlState) {
        sqlState.sql("select * from usertable where ycsb_key = " + r.nextInt(1000));
    }

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
    public void jdbcGet(JDBCState state) throws SQLException {
        state.stmt.setInt(1, r.nextInt(1000));
        try (ResultSet r = state.stmt.executeQuery()) {
            r.next();
        }
    }

    @Benchmark
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 60, timeUnit = TimeUnit.SECONDS)
    public void kvGet() {
        keyValueView.get(null, Tuple.create().set("ycsb_key", r.nextInt(1000)));
    }

    public static void main(String[] args) throws RunnerException, InterruptedException {
        Options opt = new OptionsBuilder()
                .include(".*" + SelectBenchmark.class.getSimpleName() + ".kvGet*")
                .forks(0)
                .threads(1)
                .mode(Mode.SampleTime)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class SqlState {
        QueryProcessor queryProcessor = clusterNode.queryEngine();
        SessionId sessionId = clusterNode.queryEngine().createSession(TimeUnit.SECONDS.toMillis(60), PropertiesHolder.fromMap(
                Map.of(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                ));

        private List<List<Object>> sql(String sql, Object... args) {
            var context = QueryContext.of();

            return getAllFromCursor(
                    await(queryProcessor.querySingleAsync(sessionId, context, sql, args))
            );
        }
    }

    @State(Scope.Benchmark)
    public static class JDBCState {
        public String queryStr = "select * from " + tableName + "where ycsb_key = ?";

        public Connection conn;

        public PreparedStatement stmt;

        @Setup
        public void setUp() {
            try {
                conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");

                stmt = conn.prepareStatement(queryStr);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @TearDown
        public void tearDown() throws SQLException {
            stmt.close();
            conn.close();
        }
    }


}
