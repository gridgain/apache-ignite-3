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

import static org.apache.ignite.internal.benchmark.AbstractMultiNodeBenchmark.FIELD_VAL;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_COMPLETED;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompressionOptions;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.WriteOptions;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage.
 */
@State(Scope.Benchmark)
@Fork(0)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class UpsertKvBenchmarkRocksDb {
    static {
        RocksDB.loadLibrary();
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    org.rocksdb.Options options;

    RocksDB rocksDB;

    WriteOptions writeOptions;

    CompressionOptions compressionOptions;

    TestListener testListener;

    ThreadLocal<ByteBuffer> key = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(Integer.BYTES));

    ThreadLocal<ByteBuffer> val = ThreadLocal.withInitial(() -> {
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            for (int i = 1; i < 11; i++) {
                dos.writeBytes(FIELD_VAL);
            }

            dos.flush();
            dos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] data = bos.toByteArray();

        ByteBuffer val = ByteBuffer.allocateDirect(data.length);
        val.put(data);

        return val;
    });

    /**
     * Initializes the tuple.
     */
    @Setup
    public void setUp() throws Exception {
        testListener = new TestListener("testlistener");

        compressionOptions = new CompressionOptions().setEnabled(true);
        options = new org.rocksdb.Options()
                .setMemTableConfig(new SkipListMemTableConfig())
                .setWriteBufferSize(20L * 1024 * 1024 * 1024)
                .setDisableAutoCompactions(true)
                .setAllowConcurrentMemtableWrite(true)
                .setEnableWriteThreadAdaptiveYield(true)
                .setCreateIfMissing(true)
                .setListeners(List.of(testListener))
                .setCompressionOptions(compressionOptions);
        rocksDB = RocksDB.open(options, "./tmpdb");

        writeOptions = new WriteOptions().setDisableWAL(true);
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void upsert() throws RocksDBException {
        ByteBuffer keyBuf = key.get();
        keyBuf.rewind();
        keyBuf.putInt(nextId());
        keyBuf.rewind();

        ByteBuffer valBuf = val.get();
        valBuf.rewind();

        assert keyBuf.position() == 0;
        assert valBuf.position() == 0;

        rocksDB.put(writeOptions, keyBuf, valBuf);
    }

    private int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    @TearDown
    public final void tearDown() throws Exception {
        ReadOptions ro = new ReadOptions();

        int k = 1;
        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
        tmp.putInt(k);
        tmp.rewind();
        byte[] bytes = rocksDB.get(ro, tmp.array());

        System.out.println("READ k=" + k + " len=" + bytes.length);

        FlushOptions fo = new FlushOptions().setWaitForFlush(true);
        rocksDB.flush(fo);
        rocksDB.close();
        options.close();
        writeOptions.close();
        fo.close();
        ro.close();
        compressionOptions.close();
        testListener.close();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + UpsertKvBenchmarkRocksDb.class.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc")
                .build();

        new Runner(opt).run();
    }

    private static class TestListener extends AbstractEventListener {
        /** Logger. */
        private static final IgniteLogger LOG = Loggers.forClass(TestListener.class);

        /** Listener name, for logs. */
        private final String name;

        /**
         * Type of last processed flush event. Real amount of events doesn't matter in atomic flush mode. All "completed" events go after all
         * "begin" events, and vice versa.
         */
        private final AtomicReference<EnabledEventCallback> lastFlushEventType = new AtomicReference<>(ON_FLUSH_COMPLETED);

        /** Type of last processed compaction event. */
        private final AtomicReference<EnabledEventCallback> lastCompactionEventType = new AtomicReference<>(ON_COMPACTION_COMPLETED);

        /** This field is used for determining flush duration. */
        private volatile long lastFlushStartTimeNanos;

        /** This field is used for determining compaction duration. */
        private volatile long lastCompactionStartTimeNanos;

        /**
         * Constructor.
         *
         * @param name Listener name, for logs.
         */
        public TestListener(String name) {
            super(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED, ON_COMPACTION_BEGIN, ON_COMPACTION_COMPLETED);

            this.name = name;
        }

        @Override
        public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
            if (lastFlushEventType.compareAndSet(ON_FLUSH_COMPLETED, ON_FLUSH_BEGIN)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Starting rocksdb flush process [name='{}', reason={}]", name, flushJobInfo.getFlushReason());

                    lastFlushStartTimeNanos = System.nanoTime();
                }

                onFlushBeginCallback(db, flushJobInfo);
            }
        }

        @Override
        public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
            if (lastFlushEventType.compareAndSet(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED)) {
                if (LOG.isInfoEnabled()) {
                    long duration = System.nanoTime() - lastFlushStartTimeNanos;

                    LOG.info("Finishing rocksdb flush process [name='{}', duration={}ms]", name, TimeUnit.NANOSECONDS.toMillis(duration));
                }

                onFlushCompletedCallback(db, flushJobInfo);
            }
        }

        protected void onFlushBeginCallback(RocksDB db, FlushJobInfo flushJobInfo) {
            // No-op.
        }

        protected void onFlushCompletedCallback(RocksDB db, FlushJobInfo flushJobInfo) {
            // No-op.
        }

        @Override
        public void onCompactionBegin(RocksDB db, CompactionJobInfo compactionJobInfo) {
            if (lastCompactionEventType.compareAndSet(ON_COMPACTION_COMPLETED, ON_COMPACTION_BEGIN)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Starting rocksdb compaction process [name='{}', reason={}, input={}, output={}]",
                            name,
                            compactionJobInfo.compactionReason(),
                            // Extract file names from full paths.
                            compactionJobInfo.inputFiles().stream().map(path -> Paths.get(path).getFileName()).collect(Collectors.toList()),
                            compactionJobInfo.outputFiles().stream().map(path -> Paths.get(path).getFileName()).collect(Collectors.toList())
                    );

                    lastCompactionStartTimeNanos = System.nanoTime();
                }
            }
        }

        @Override
        public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
            if (lastCompactionEventType.compareAndSet(ON_COMPACTION_BEGIN, ON_COMPACTION_COMPLETED)) {
                if (LOG.isInfoEnabled()) {
                    long duration = System.nanoTime() - lastCompactionStartTimeNanos;

                    LOG.info("Finishing rocksdb compaction process [name='{}', duration={}ms]", name, TimeUnit.NANOSECONDS.toMillis(duration));
                }
            }
        }
    }
}
