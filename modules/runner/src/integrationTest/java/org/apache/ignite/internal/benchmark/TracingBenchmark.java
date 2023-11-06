package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;

import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for insertion operation, comparing noop tracing with enabled tracing.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TracingBenchmark extends AbstractOneNodeBenchmark {
    /**
     * Benchmark for KV insert via embedded client.
     */
    @Benchmark
    public void kvInsert(KvState state) {
        state.executeQuery();
    }

    /**
     * Benchmark for KV insert via embedded client.
     */
    @Benchmark
    public void kvInsertWithTracing(KvState state) {
        rootSpan("put", (parentSpan) -> {
            state.executeQuery();

            return null;
        });
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + TracingBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .threads(1)
                .mode(Mode.AverageTime)
                .param("fsync", "false")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark state for {@link #kvInsert(KvState)}.
     *
     * <p>Holds {@link Tuple} and {@link KeyValueView} for the table.
     */
    @State(Scope.Benchmark)
    public static class KvState {
        private final Tuple tuple = Tuple.create();

        private int id = 0;

        private final KeyValueView<Tuple, Tuple> kvView = clusterNode.tables().table(TABLE_NAME).keyValueView();

        /**
         * Initializes the tuple.
         */
        @Setup
        public void setUp() {
            for (int i = 1; i < 11; i++) {
                tuple.set("field" + i, FIELD_VAL);
            }
        }

        void executeQuery() {
            kvView.put(null, Tuple.create().set("ycsb_key", id++), tuple);
        }
    }
}
