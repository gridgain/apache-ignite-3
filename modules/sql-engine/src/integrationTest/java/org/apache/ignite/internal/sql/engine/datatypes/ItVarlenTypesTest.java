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

package org.apache.ignite.internal.sql.engine.datatypes;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * TODO: https://issues.apache.org/jira/browse/IGNITE-24154
 */
public class ItVarlenTypesTest extends BaseSqlIntegrationTest {

    @Override
    protected int initialNodes() {
        return Integer.parseInt(System.getenv().getOrDefault("TEST_CLUSTER_SIZE", "1"));
    }

    private static long getDataRegionSize() {
        return parseRecordSize(System.getenv().getOrDefault("TEST_DATA_REGION_SIZE", "1_g"));
    }

    @Override
    protected Path getWorkDir(Path autoWorkDir) {
        String testDataDir = System.getenv().getOrDefault("TEST_DATA_DIR", autoWorkDir.toAbsolutePath().toString());

        log.info("Params: Data dir is {}", testDataDir);

        return Paths.get(testDataDir);
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        long dataRegionSize = getDataRegionSize();

        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  storage: {"
                + "    profiles: {\n"
                + "      default: {\n"
                + "        engine: \"aipersist\",\n"
                + "        replacementMode: \"CLOCK\",\n"
                + "        size: " + dataRegionSize
                + "      }\n"
                + "    }"
                + "  },"
                + "  clientConnector.port: {},\n"
                + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
                + "  rest.port: {},\n"
                + "  compute.threadPoolSize: 1,\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }

    private void randomDelay() {
        int millis = ThreadLocalRandom.current().nextInt(5, 10);
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private enum Mode {
        KEY,
        VALUE
    }

    private enum DataType {
        STRING,
        BINARY;

        Generator newGenerator(int recordSize, Mode mode) {
            switch (this) {
                case STRING:
                    return new StringGenerator(recordSize, mode == Mode.KEY);
                case BINARY:
                    return new ByteArrayGenerator(recordSize, mode == Mode.KEY);
                default:
                    throw new IllegalArgumentException("Unexpected type: " + this);
            }
        }
    }

    @Test
    public void testKv() {
        int clusterSize = initialNodes();
        long dataRegionSize = getDataRegionSize();

        long durationSeconds = parseDeadline(System.getenv().getOrDefault("TEST_DURATION", "30_s"));
        int recordSize = parseRecordSize(System.getenv().getOrDefault("TEST_RECORD_SIZE", "55_m"));
        long maxDataSizeGb = Long.parseLong(System.getenv().getOrDefault("TEST_MAX_DATA_SIZE_GB", "120"));
        int numWriters = Integer.parseInt(System.getenv().getOrDefault("TEST_NUM_WRITERS", "2"));
        int numReaders = Integer.parseInt(System.getenv().getOrDefault("TEST_NUM_READERS", "1"));
        Mode largeValueMode = Mode.valueOf(System.getenv().getOrDefault("TEST_MODE", "KEY"));
        DataType largeType = DataType.valueOf(System.getenv().getOrDefault("TEST_TYPE", "STRING"));

        long MB = 1024 * 1024;
        long GB = MB * 1024;

        long maxDateSetSize = maxDataSizeGb * GB;

        executeLoad(clusterSize, dataRegionSize, largeType, recordSize, largeValueMode,
                durationSeconds, maxDateSetSize, numWriters, numReaders);
    }

    @ParameterizedTest
    @CsvSource({
            "STRING,KEY",
            "STRING,VALUE",
            "BINARY,KEY",
            "BINARY,VALUE",
    })
    public void test(DataType dataType, Mode mode) {
        executeLoad(1, 10000, dataType, 1024*1024, mode, 30, 100000, 1, 1);
    }

    private void executeLoad(
            int clusterSize,
            long dataRegionSize,
            DataType largeType,
            int recordSize,
            Mode largeValueMode,
            long durationSeconds,
            long maxDateSetSize,
            int numWriters,
            int numReaders
    ) {
        Generator generator = largeType.newGenerator(recordSize, largeValueMode);

        log.info("Params: Cluster size is {}", clusterSize);
        log.info("Params: mode: {}", largeValueMode);
        log.info("Params: Data type: {}", largeType);
        log.info("Params: Data region size is {}", IgniteUtils.readableSize(dataRegionSize, false));
        log.info("Params: Record size is {}", recordSize);
        log.info("Params: Duration in seconds: {}", durationSeconds);
        log.info("Params: Max dataset size: {}", maxDateSetSize);
        log.info("Params: Num writers: {}", numWriters);
        log.info("Params: Num readers: {}", numReaders);

        ExecutorService executorService = Executors.newCachedThreadPool();
        {
            Ignite ignite = CLUSTER.node(0);
            IgniteSql sql = ignite.sql();

            try (ResultSet<SqlRow> rs = sql.execute(null, "DROP TABLE IF EXISTS t")) {
                assertNotNull(rs);
            }

            String ddl = format("CREATE TABLE t (id {} PRIMARY KEY, val {})", generator.keyType(), generator.valueType());

            log.info("DDL: {}", ddl);

            try (ResultSet<SqlRow> rs = sql.execute(null, ddl)) {
                assertTrue(rs.wasApplied());
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
            SharedState sharedState = new SharedState(log, recordSize, deadline, maxDateSetSize);
            List<CompletableFuture<Void>> tasks = new ArrayList<>();

            // Writers
            for (int i = 0; i < numWriters; i++) {
                int index = i;
                Runnable task = () -> {
                    // Starting multiple workers can overwhelm a node.
                    try {
                        TimeUnit.SECONDS.sleep(20L * index);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    new Writer(ignite, sharedState, index, generator.newValueGenerator()).run();
                };
                tasks.add(CompletableFuture.runAsync(task, executorService));
            }

            // Readers
            for (int i = 0; i < numReaders; i++) {
                int index = i;
                Runnable task = () -> {
                    // Start readers after a delay when there is some data available.
                    try {
                        TimeUnit.MINUTES.sleep(0);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    new Reader(ignite, sharedState, index, generator.newValueGenerator()).run();
                };
                tasks.add(CompletableFuture.runAsync(task, executorService));
            }

            CompletableFutures.allOf(tasks).join();
        }
    }

    abstract static class Generator {

        abstract String keyType();

        abstract String valueType();

        abstract Generator newValueGenerator();

        abstract Object generateWriteKey(SharedState sharedState, int i);

        abstract Object generateValue(SharedState sharedState, int i);

        abstract Object generateReadKey(SharedState sharedState, int i);

        abstract KeyValueView<Object, Object> keyValueView(Table table);
    }

    private static class SharedState {

        final IgniteLogger log;

        final int recordSize;

        final long deadlineNanos;

        final AtomicInteger writerIndex = new AtomicInteger();

        final AtomicLong dataSetSize = new AtomicLong();

        final long maxDateSetSize;

        private SharedState(IgniteLogger log, int recordSize, long deadlineNanos, long maxDateSetSize) {
            this.log = log;
            this.recordSize = recordSize;
            this.deadlineNanos = deadlineNanos;
            this.maxDateSetSize = maxDateSetSize;
        }

        private boolean shouldStop(long s) {
            if (s > deadlineNanos) {
                log.info("DONE: Deadline has been reached.");
                return true;
            }

            if (dataSetSize.get() > maxDateSetSize) {
                log.info("DONE: Max dataset has been reached.");
                return true;
            }
            return false;
        }
    }

    private class Writer implements Runnable {

        private final Ignite ignite;

        private final SharedState sharedState;

        private final int writerId;

        private final Generator generator;

        private Writer(Ignite ignite, SharedState sharedState, int writerId, Generator generator) {
            this.ignite = ignite;
            this.sharedState = sharedState;
            this.writerId = writerId;
            this.generator = generator;
        }

        @Override
        public void run() {
            Table table = ignite.tables().table("public.t");
            KeyValueView<Object, Object> kv = generator.keyValueView(table);

            long time = 0;
            int reportInterval = 100;
            long MB = 1024 * 1024;
            int i = 0;

            while (!Thread.currentThread().isInterrupted()) {
                long s = System.nanoTime();

                if (sharedState.shouldStop(s)) {
                    break;
                }

                Object key = generator.generateWriteKey(sharedState, i);
                Object value = generator.generateValue(sharedState, i);

                try {
                    Transaction tx = ignite.transactions().begin(new TransactionOptions().readOnly(false));
                    kv.put(tx, key, value);
                    tx.commit();
                } catch (Exception e) {
                    log.warn("{}: Writer failed to write a KV pair. Wait for 10 ms.", writerId, e);
                }

                sharedState.dataSetSize.addAndGet(sharedState.recordSize);

                long dt = System.nanoTime() - s;
                time += dt;
                i += 1;

                if (i % reportInterval == 0) {
                    long avgMicros = TimeUnit.NANOSECONDS.toMicros(time / reportInterval);
                    long dataSetSize = sharedState.dataSetSize.get();
                    long maxDateSetSize = sharedState.maxDateSetSize;

                    log.info("{}: Writer avg {} micros per {} ops. i: {}. Dataset: {} MB / {} / MB.",
                            writerId, avgMicros, reportInterval, i, dataSetSize / MB, maxDateSetSize / MB
                    );
                    time = 0;
                }

                randomDelay();
            }
        }
    }

    private class Reader implements Runnable {

        private final Ignite ignite;

        private final SharedState sharedState;

        private final int readerId;

        private final Generator generator;

        private Reader(Ignite ignite, SharedState sharedState, int readerId, Generator generator) {
            this.ignite = ignite;
            this.sharedState = sharedState;
            this.readerId = readerId;
            this.generator = generator;
        }

        @Override
        public void run() {
            Table table = ignite.tables().table("public.t");
            KeyValueView<Object, Object> kv = generator.keyValueView(table);

            long time = 0;
            int reportInterval = 100;
            int i = 0;

            while (!Thread.currentThread().isInterrupted()) {
                long s = System.nanoTime();

                if (sharedState.shouldStop(s)) {
                    break;
                }

                Object key = generator.generateReadKey(sharedState, i);
                try {
                    Object data = kv.get(null, key);
                    assert data != null || data == null;
                } catch (Exception e) {
                    log.warn("{}: Reader failed to read key", readerId, e);
                }

                long dt = System.nanoTime() - s;
                time += dt;
                i += 1;

                if (i % reportInterval == 0) {
                    long avgMicros = TimeUnit.NANOSECONDS.toMicros(time / reportInterval);
                    log.info("{}: Reader avg {} micros per {} ops. i: {}.", readerId, avgMicros, reportInterval, i);
                    time = 0;
                }

                randomDelay();
            }
        }
    }

    static class ByteArrayGenerator extends Generator {

        private final byte[] bytes;

        private final boolean byteKey;

        ByteArrayGenerator(int recordSize, boolean byteKey) {
            this.bytes = new byte[recordSize];
            this.byteKey = byteKey;
        }

        @Override
        Generator newValueGenerator() {
            return new ByteArrayGenerator(bytes.length, byteKey);
        }

        @Override
        String keyType() {
            if (byteKey) {
                return "VARBINARY(" + Integer.MAX_VALUE + ")";
            } else {
                return "INT";
            }
        }

        @Override
        String valueType() {
            if (byteKey) {
                return "INT";
            } else {
                return "VARBINARY(" + Integer.MAX_VALUE + ")";
            }
        }

        @Override
        Object generateWriteKey(SharedState sharedState, int i) {
            if (byteKey) {
                ThreadLocalRandom.current().nextBytes(bytes);
                return bytes;
            } else {
                return sharedState.writerIndex.incrementAndGet();
            }
        }

        @Override
        Object generateValue(SharedState sharedState, int i) {
            if (byteKey) {
                return i;
            } else {
                ThreadLocalRandom.current().nextBytes(bytes);
                return bytes;
            }
        }

        @Override
        Object generateReadKey(SharedState sharedState, int i) {
            if (byteKey) {
                ThreadLocalRandom.current().nextBytes(bytes);
                return bytes;
            } else {
                return ThreadLocalRandom.current().nextInt(sharedState.writerIndex.get() + 1);
            }
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        KeyValueView<Object, Object> keyValueView(Table table) {
            if (byteKey) {
                return (KeyValueView) table.keyValueView(byte[].class, Integer.class);
            } else {
                return (KeyValueView) table.keyValueView(Integer.class, byte[].class);
            }
        }
    }

    static class StringGenerator extends Generator {

        private final int recordSize;

        private final boolean stringKey;

        private final Random random = new Random();

        StringGenerator(int recordSize, boolean stringKey) {
            this.recordSize = recordSize;
            this.stringKey = stringKey;
        }

        @Override
        Generator newValueGenerator() {
            return new StringGenerator(recordSize, stringKey);
        }

        @Override
        String keyType() {
            if (stringKey) {
                return "VARCHAR(" + Integer.MAX_VALUE + ")";
            } else {
                return "INT";
            }
        }

        @Override
        String valueType() {
            if (stringKey) {
                return "INT";
            } else {
                return "VARCHAR(" + Integer.MAX_VALUE + ")";
            }
        }

        @Override
        Object generateWriteKey(SharedState sharedState, int i) {
            if (stringKey) {
                return IgniteTestUtils.randomString(random, recordSize);
            } else {
                return sharedState.writerIndex.incrementAndGet();
            }
        }

        @Override
        Object generateValue(SharedState sharedState, int i) {
            if (stringKey) {
                return i;
            } else {
                return IgniteTestUtils.randomString(random, recordSize);
            }
        }

        @Override
        Object generateReadKey(SharedState sharedState, int i) {
            if (stringKey) {
                return IgniteTestUtils.randomString(random, recordSize);
            } else {
                return ThreadLocalRandom.current().nextInt(sharedState.writerIndex.get() + 1);
            }
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        KeyValueView<Object, Object> keyValueView(Table table) {
            if (stringKey) {
                return (KeyValueView) table.keyValueView(String.class, Integer.class);
            } else {
                return (KeyValueView) table.keyValueView(Integer.class, String.class);
            }
        }
    }

    private static int parseRecordSize(String s) {
        String[] valUnit = s.split("_");
        if (valUnit.length == 1) {
            return Integer.parseInt(s);
        }
        int value = Integer.parseInt(valUnit[0]);
        switch (valUnit[1].toUpperCase()) {
            case "K":
                return value * 1024;
            case "M":
                return value * 1024 * 1024;
            case "G":
                return value * 1024 * 1024 * 1024;
            default:
                throw new IllegalArgumentException("Unexpected unit for value: " + s);
        }
    }

    private static int parseDeadline(String s) {
        String[] valUnit = s.split("_");
        if (valUnit.length == 1) {
            return Integer.parseInt(s);
        }
        int value = Integer.parseInt(valUnit[0]);
        switch (valUnit[1].toUpperCase()) {
            case "S":
                return value;
            case "M":
                return value * 60;
            default:
                throw new IllegalArgumentException("Unexpected unit for value: " + s);
        }
    }
}
/*

// Thin
// 6   128 OK 44G     modules/sql-engine/build/
// 12  128 OK 161G    modules/sql-engine/build/ id: 54600. Dataset: 81901 / 81920
// 56  238 OK 161G    modules/sql-engine/build/ 71534 micros. id: 11700. Dataset: 81907 / 81920
// 128 128 OK 161G    modules/sql-engine/build/ 130452 micros. id: 5100. Dataset: 81616 / 81920
// 256 128 OK 160G    modules/sql-engine/build/ 274796 micros. id: 2500. Dataset: 80032 / 81920
// 512 128 OK 161G    /Volumes/WorkDisk/Users/mzhuravkov/test_data 562314 micros. id: 800. Dataset: 51264 / 81920
//
// Embedded:
// 512 FAIL 30G
// 256 OK 161G    /Volumes/WorkDisk/Users/mzhuravkov/test_data
// 300 OK         /Volumes/WorkDisk/Users/mzhuravkov/test_data


[2025-02-14T20:17:52,315][INFO ][%ivtt_n_3344%compaction-thread][Compactor] Starting new compaction round [compactionId=7ce14301-cb84-45c8-b8d4-1fe3ff037c0f, files=25]
Exception in thread "Thread-589" java.lang.IllegalArgumentException: Illegal value provided for FlushReason: 13
	at org.rocksdb.FlushReason.fromValue(FlushReason.java:51)
	at org.rocksdb.FlushJobInfo.<init>(FlushJobInfo.java:41)
Exception in thread "Thread-590" java.lang.IllegalArgumentException: Illegal value provided for FlushReason: 13
	at org.rocksdb.FlushReason.fromValue(FlushReason.java:51)
	at org.rocksdb.FlushJobInfo.<init>(FlushJobInfo.java:41)
[2025-02-14T20:17:53,128][INFO ][%ivtt_n_3344%checkpoint-thread][Checkp


[2025-02-15T06:54:36,105][INFO ][%ivtt_n_3344%checkpoint-thread][Checkpointer] Checkpoint started [checkpointId=c550bc9b-ee6d-4831-83a4-9cb4608ff41b, beforeWriteLockTime=0ms, writeLockWait=1us, listenersExecuteTime=78us, writeLockHoldTime=172us, splitAndSortPagesDuration=5ms, pages=12375, reason='too many dirty pages']
Exception in thread "Thread-544" java.lang.IllegalArgumentException: Illegal value provided for FlushReason: 13
	at org.rocksdb.FlushReason.fromValue(FlushReason.java:51)
	at org.rocksdb.FlushJobInfo.<init>(FlushJobInfo.java:41)
[2025-02-15T06:54:36,157][INFO ][Thread-545][LoggingRocksDbFlushListener] Starting rocksdb flush process [name='table data log', reason=WRITE_BUFFER_FULL]
Exception in thread "Thread-546" java.lang.IllegalArgumentException: Illegal value provided for FlushReason: 13
	at org.rocksdb.FlushReason.fromValue(FlushReason.java:51)
	at org.rocksdb.FlushJobInfo.<init>(FlushJobInfo.java:41)
[2025-02-15T06:54:36,617][INFO ][Thread-548][LoggingRocksDbFlushListener] Starting rocksdb compaction process [name='table data log', reason=kLevelL0FilesNum, input=[000621.sst, 000595.sst, 000586.sst, 000588.sst, 000591.sst, 000592.sst, 000597.sst, 000598.sst], output=[]]
[2025-02-15T06:54:36,617][INFO ][Thread-547][LoggingRocksDbFlushListener] Finishing rocksdb flush process [name='table data log', duration=459ms]
[2025-02-15T06:54:37,476][INFO ][Thread-550][LoggingRocksDbFlushListener] Finishing rocksdb compaction process [name='table data log', duration=858ms]
 */