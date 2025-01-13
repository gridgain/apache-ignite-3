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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.IgniteUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.CompressionOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage.
 */
@State(Scope.Benchmark)
@Fork(0)
@Threads(1)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 2, time = 2)
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

    ThreadLocal<ByteBuffer> key = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(Integer.BYTES));

    ThreadLocal<ByteBuffer> val = ThreadLocal.withInitial(() -> {
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            for (int i = 1; i < 11; i++) {
                dos.writeChars(FIELD_VAL);
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
        options = new org.rocksdb.Options().setCreateIfMissing(true).setCompressionOptions(new CompressionOptions().setEnabled(false));
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
}
