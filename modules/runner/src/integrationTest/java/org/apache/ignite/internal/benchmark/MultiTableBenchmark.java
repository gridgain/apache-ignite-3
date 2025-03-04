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

package org.apache.ignite.internal.benchmark;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create table in each iteration.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(2)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MultiTableBenchmark extends AbstractMultiNodeBenchmark {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    private static final Logger log = LoggerFactory.getLogger(MultiTableBenchmark.class);

    private static final int TABLES_COUNT = 300;

    @Param({"false", "true"})
    private boolean useHeartbeatCoalescing;

    @Param({"1", "3"})
    private int replicaCount;

    @Override
    public void nodeSetUp() throws Exception {
        System.setProperty("IGNITE_USE_HEARTBEAT_COALESCING", Boolean.toString(useHeartbeatCoalescing));

        super.nodeSetUp();

        for (int i = 0; i < TABLES_COUNT; i++) {
            String tableName = "test" + i;

            createTable(tableName);
        }
    }

    @Override
    protected @Nullable String clusterConfiguration() {
        return "ignite {"
                + "  replication {"
                + "    idleSafeTimePropagationDuration: 10000, "
                + "    leaseExpirationInterval: 10000"
                + "  }"
                + "}";
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + MultiTableBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void test() {
        int id = nextId();

        String tableName = "test" + (id % TABLES_COUNT);

        insertIn(tableName, id);
    }

    static void insertIn(String tableName, int id) {
        RecordView<Tuple> view = publicIgnite.tables().table(tableName).recordView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        view.insert(null, Tuple.copy(payload).set("ycsb_key", id));
    }

    private int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    @Override
    protected int replicaCount() {
        return replicaCount;
    }

//    @Override
//    protected Path workDir() throws Exception {
//        return Path.of("D:\\tmpDirPrefix" + ThreadLocalRandom.current().nextInt());
//    }
}
