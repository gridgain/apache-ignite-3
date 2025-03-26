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

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that compares sequential scanning of index against full table scan.
 */
@State(Scope.Benchmark)
@Fork(value = 1/*, jvmArgsAppend = {"-XX:+PrintGCDetails", "-Xloggc:D:/GC_logs/gc.log", "-Xlog:safepoint"}*/)
@Threads(32)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SqlIndexesBenchmark extends AbstractMultiNodeBenchmark {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    private static final LocalDate INITIAL_DATE = LocalDate.of(1970, 1, 1);

    private static final String STR10 = "qwertyuiop";
    private static final String STR100 = "qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiop"
            + "qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiop";

    @Param({"0", "2", "4", "8", "10"})
    private int idxes;

    @Param({/*"INT",*/ "STR10", "STR100"})
    private String idxType;

    private IgniteSql sql;

    private RecordView<Tuple> recordView;

    /** Initializes a schema and fills tables with data. */
    @Setup
    public void setUp() throws Exception {
        try {
            sql = publicIgnite.sql();

            String query = "INT".equals(idxType) ? queryForIntIndexes() : queryForStringIndexes();

            switch (idxes) {
                case 10:
                    query += "CREATE INDEX test_val9_idx ON test(val9);";
                case 9:
                    query += "CREATE INDEX test_val8_idx ON test(val8);";
                case 8:
                    query += "CREATE INDEX test_val7_idx ON test(val7);";
                case 7:
                    query += "CREATE INDEX test_val6_idx ON test(val6);";
                case 6:
                    query += "CREATE INDEX test_val5_idx ON test(val5);";
                case 5:
                    query += "CREATE INDEX test_val4_idx ON test(val4);";
                case 4:
                    query += "CREATE INDEX test_val3_idx ON test(val3);";
                case 3:
                    query += "CREATE INDEX test_val2_idx ON test(val2);";
                case 2:
                    query += "CREATE INDEX test_val1_idx ON test(val1);";
                case 1:
                    query += "CREATE INDEX test_val_idx ON test(val);";
            }

            sql.executeScript(query);

            recordView = publicIgnite.tables().table("test").recordView();
        } catch (Exception e) {
            nodeTearDown();

            throw e;
        }
    }

    private static @NotNull String queryForStringIndexes() {
        String query = "CREATE ZONE single_partition_zone WITH STORAGE_PROFILES='default', replicas = 1, partitions = 32;"
                + "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR, val4 VARCHAR,"
                + " val5 VARCHAR, val6 VARCHAR, val7 VARCHAR, val8 VARCHAR, val9 VARCHAR) ZONE single_partition_zone;";

        return query;
    }

    private static @NotNull String queryForIntIndexes() {
        String query = "CREATE ZONE single_partition_zone WITH STORAGE_PROFILES='default', replicas = 1, partitions = 32;"
                + "CREATE TABLE test (id INT PRIMARY KEY, val INT, val1 INT, val2 INT, val3 INT, val4 INT, val5 INT,"
                + " val6 INT, val7 INT, val8 INT, val9 INT) ZONE single_partition_zone;";

        return query;
    }

    @Benchmark
    public void put() {
        int val = ThreadLocalRandom.current().nextInt(0, 1_500_000);

        recordView.upsert(null, Tuple.create()
                .set("id", nextId())
                .set("val", /*INITIAL_DATE.plusDays(val)*/getVal(val))
                .set("val1", getVal(val))
                .set("val2", getVal(val))
                .set("val3", getVal(val))
                .set("val4", getVal(val))
                .set("val5", getVal(val))
                .set("val6", getVal(val))
                .set("val7", getVal(val))
                .set("val8", getVal(val))
                .set("val9", getVal(val))
        );
    }

    private Object getVal(int val) {
        switch (idxType) {
            case "INT":
                return val;
            case "STR10": {
                String str = STR10 + val;

                return str.substring(str.length() - STR10.length());
            }
            case "STR100": {
                String str = STR100 + val;

                return str.substring(str.length() - STR100.length());
            }
            default:
                throw new IllegalArgumentException("Unsupported index type: " + idxType);
        }
    }

    //@Benchmark
    public void get(Blackhole bh) {
        int id = ThreadLocalRandom.current().nextInt(0, 100_000);

        Tuple val = recordView.get(null, Tuple.create().set("id", nextId()));

        bh.consume(val);
    }

    private int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    @Override
    protected int nodes() {
        return 1;
    }

//    @Override
//    protected Path workDir() throws Exception {
//        return Path.of("D:", "tmpDirPrefix" + ThreadLocalRandom.current().nextInt());
//    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
//                .jvmArgsAppend(
//                        " -Xlog:gc*:D:\\GC_logs:time:filecount=10,filesize=25M "
//                        + "-Xlog:safepoint*:D:\\GC_logs:time:filecount=10,filesize=25M "
//                        + "-verbose:gc "
//                )
                .include(".*" + SqlIndexesBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}


