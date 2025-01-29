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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PublicApiThreadingTable;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(32)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PutGetKvBenchmark extends AbstractMultiNodeBenchmark {
    private final Tuple tuple = Tuple.create();

    private static KeyValueView<Tuple, Tuple> kvView;

    @Param({"10000"})
    private int batch;

    @Param({"false"})
    private boolean fsync;

    @Param({"32"})
    private int partitionCount;

    @Param({"190000"})
    private int keys;

    @Param({"aimem"})
    private String engine;

    @Param({"268435456"})
    //@Param({"1073741824"})
    private int cacheSize;

    /**
     * Initializes the tuple.
     */
    @Setup
    public void setUp() {
        // TODO try with public
        Table table = igniteImpl.tables().table(TABLE_NAME);
        kvView = table.keyValueView();
        for (int i = 1; i < 11; i++) {
            tuple.set("field" + i, FIELD_VAL);
        }

        int i = 0;
        while (i < keys) {
            Map<Tuple, Tuple> map = new HashMap<>();

            for (int j = 0; j < batch; j++) {
                map.put(Tuple.create().set("ycsb_key", i + j), tuple);
            }

            kvView.putAll(null, map);
            System.out.println("Loaded batch " + i);

            i += batch;
        }

        for (int p = 0; p < partitionCount(); p++) {
            System.out.println("Sync partition " + p + " ...");
            ((PublicApiThreadingTable)table).unwrap(TableImpl.class).internalTable().storage().getMvPartition(0).flush().join();
            System.out.println("Done");
        }
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void get() {
        int randomId = ThreadLocalRandom.current().nextInt(0, keys);

        Tuple val = kvView.get(null, Tuple.create().set("ycsb_key", randomId));

        if (val == null) {
            throw new IllegalStateException("No value for " + randomId);
        }
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + PutGetKvBenchmark.class.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected boolean fsync() {
        return fsync;
    }

    @Override
    protected int nodes() {
        return 1;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return 1;
    }

    @Override
    protected String engine() {
        return engine;
    }

    @Override
    protected int cacheSize() {
        return cacheSize;
    }
}
