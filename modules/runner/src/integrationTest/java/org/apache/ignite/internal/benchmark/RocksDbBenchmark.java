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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.benchmark.AbstractMultiNodeBenchmark.FIELD_VAL;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.toStringName;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_COMPACTION_COMPLETED;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.ColumnFamilyType;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.jetbrains.annotations.Nullable;
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
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RocksDbBenchmark {
    static {
        RocksDB.loadLibrary();
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    private RocksDB rocksDB;
    private WriteOptions writeOptions;
    private ColumnFamily meta = null;
    private ColumnFamily partitionCf = null;
    private ColumnFamily gcQueueCf = null;
    private ColumnFamily dataCf = null;
    private ColumnFamily hashIndexCf = null;
    private ArrayList<ColumnFamily> sortedIndexCfs = new ArrayList<>();

    private final List<AutoCloseable> resources = new ArrayList<>();

    ThreadLocal<ByteBuffer> rowIdBuf0 = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4 + 2 + 8 + 8));

    ThreadLocal<ByteBuffer> keyBuf0 = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4 + 2 + 4));

    ThreadLocal<ByteBuffer> metaKeyBuf0 = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(4 + 2 + 8 + 8));

    ThreadLocal<ByteBuffer> metaStateBuf0 = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(8));

    ThreadLocal<ByteBuffer> valBuf0 = ThreadLocal.withInitial(() -> {
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
        Path path = Path.of("./tmpdb");

        List<ColumnFamilyDescriptor> cfDescriptors = getExistingCfDescriptors(path);

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

        TestListener testListener = new TestListener("testlistener");
        add(testListener);

        DBOptions dbOptions = add(new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setAllowConcurrentMemtableWrite(true)
                //.setEnableWriteThreadAdaptiveYield(true)
                // Atomic flush must be enabled to guarantee consistency between different column families when WAL is disabled.
                .setAtomicFlush(true)
                .setListeners(List.of(testListener))
                //.setWriteBufferManager(profile.writeBufferManager())
                // Don't flush on shutdown to speed up node shutdown as on recovery we'll apply commands from log.
                .setAvoidFlushDuringShutdown(true)
        );

        rocksDB = add(RocksDB.open(dbOptions, path.toAbsolutePath().toString(), cfDescriptors, cfHandles));
        this.resources.addAll(cfHandles);

        writeOptions = new WriteOptions().setDisableWAL(true);
        this.resources.add(writeOptions);

        // Read all existing Column Families from the db and parse them according to type: meta, partition data or index.
        for (ColumnFamilyHandle cfHandle : cfHandles) {
            ColumnFamily cf = ColumnFamily.wrap(rocksDB, cfHandle);

            switch (ColumnFamilyType.fromCfName(cf.name())) {
                case META:
                    meta = cf;

                    break;

                case PARTITION:
                    partitionCf = cf;

                    break;

                case GC_QUEUE:
                    gcQueueCf = cf;

                    break;

                case DATA:
                    dataCf = cf;

                    break;

                case HASH_INDEX:
                    hashIndexCf = cf;

                    break;

                case SORTED_INDEX:
                    sortedIndexCfs.add(cf);

                    break;

                default:
                    throw new StorageException("Unidentified column family: [name={}, path={}]", cf.name(), path);
            }
        }
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void upsert() throws RocksDBException {
        UUID rowId = new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());

        ByteBuffer rowIdBuf = rowIdBuf0.get();
        rowIdBuf.rewind();
        rowIdBuf.putInt(0); // Table id.
        rowIdBuf.putShort((short) 0); // Partition id.
        rowIdBuf.putLong(rowId.getLeastSignificantBits());
        rowIdBuf.putLong(rowId.getMostSignificantBits());
        rowIdBuf.rewind();

        ByteBuffer keyBuf = keyBuf0.get();
        keyBuf.rewind();
        keyBuf.putInt(0); // Table id.
        keyBuf.putShort((short) 0); // Partition id.
        keyBuf.putInt(nextId());
        rowIdBuf.rewind();

        ByteBuffer valBuf = valBuf0.get();
        valBuf.rewind();

        ByteBuffer metaKeyBuf = metaKeyBuf0.get();
        metaKeyBuf.rewind();
        metaKeyBuf.putInt(0); // Table id.
        metaKeyBuf.putShort((short) 0); // Partition id.
        metaKeyBuf.putLong(rowId.getLeastSignificantBits());
        metaKeyBuf.putLong(rowId.getMostSignificantBits());
        metaKeyBuf.rewind();

        ByteBuffer metaStateBuf = metaStateBuf0.get();
        metaStateBuf.rewind();
        metaStateBuf.putLong(0); // Some dummy state.
        metaStateBuf.rewind();

        WriteBatchWithIndex writeBatch = new WriteBatchWithIndex();

        ByteBuffer cp = ByteBuffer.allocate(keyBuf.capacity());
        cp.put(keyBuf);
        keyBuf.rewind();

        byte[] bytes = hashIndexCf.get(cp.array());

        if (bytes != null) {
            throw new IllegalStateException();
        }

        writeBatch.put(hashIndexCf.cfHandle, keyBuf, rowIdBuf);
        writeBatch.put(partitionCf.cfHandle, metaKeyBuf, metaStateBuf);
        metaKeyBuf.rewind();
        writeBatch.put(dataCf.cfHandle, metaKeyBuf, valBuf);

        rocksDB.write(writeOptions, writeBatch);

        writeBatch.close();
    }

    private static int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    @TearDown
    public final void tearDown() throws Exception {
//        ReadOptions ro = new ReadOptions();
//
//        int k = 1;
//        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
//        tmp.putInt(k);
//        tmp.rewind();
//        byte[] bytes = rocksDB.get(dataCf.cfHandle, ro, tmp.array());
//
//        System.out.println("READ k=" + k + " len=" + bytes.length);
//
//        FlushOptions fo = new FlushOptions().setWaitForFlush(true);
//        rocksDB.flush(fo);

        for (AutoCloseable resource : resources) {
            resource.close();
        }

        rocksDB.close();

//        fo.close();
//        ro.close();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + RocksDbBenchmark.class.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc")
                .build();

        new Runner(opt).run();
    }

    public static class ColumnFamily {
        /** RocksDB instance. */
        private final RocksDB db;

        /** Column family name. */
        private final String cfName;

        /** Column family name as a byte array. */
        private final byte[] cfNameBytes;

        /** Column family handle. */
        private final ColumnFamilyHandle cfHandle;

        /** Private ColumnFamilyOptions owned exclusively by this CF, if any. */
        @Nullable
        private final ColumnFamilyOptions privateCfOptions;

        /**
         * Constructor.
         *
         * @param db Db.
         * @param handle Column family handle.
         */
        private ColumnFamily(RocksDB db, ColumnFamilyHandle handle, @Nullable ColumnFamilyOptions privateCfOptions) throws RocksDBException {
            this.db = db;
            this.cfHandle = handle;
            cfNameBytes = cfHandle.getName();
            this.cfName = new String(cfNameBytes, StandardCharsets.UTF_8);
            this.privateCfOptions = privateCfOptions;
        }

        /**
         * Creates a new Column Family in the provided RocksDB instance.
         * <b>Warning!!</b> This method assumes that the ColumnFamilyOptions in the descriptor are exclusive to this ColumnFamily, as such,
         * {@link #destroy()} will close them.
         *
         * @param db RocksDB instance.
         * @param descriptor Column Family descriptor.
         * @return new Column Family.
         * @throws RocksDBException If an error has occurred during creation.
         */
        public static ColumnFamily withPrivateOptions(RocksDB db, ColumnFamilyDescriptor descriptor) throws RocksDBException {
            ColumnFamilyHandle cfHandle = db.createColumnFamily(descriptor);

            return new ColumnFamily(db, cfHandle, descriptor.getOptions());
        }

        /**
         * Creates a wrapper around an already created Column Family.
         *
         * @param db RocksDB instance.
         * @param handle Column Family handle.
         * @return Column Family wrapper.
         * @throws RocksDBException If an error has occurred during creation.
         */
        public static ColumnFamily wrap(RocksDB db, ColumnFamilyHandle handle) throws RocksDBException {
            return new ColumnFamily(db, handle, null);
        }

        /**
         * Removes all data associated with this Column Family and frees its resources.
         *
         * @throws RocksDBException if an error has occurred during the destruction.
         */
        public void destroy() throws RocksDBException {
            db.dropColumnFamily(cfHandle);

            db.destroyColumnFamilyHandle(cfHandle);

            // If we are tracking the options then we also close them.
            if (this.privateCfOptions != null) {
                privateCfOptions.close();
            }
        }

        /**
         * Gets the value associated with the key from this column family.
         *
         * @param key Key.
         * @return Value.
         * @throws RocksDBException If failed.
         * @see RocksDB#get(ColumnFamilyHandle, byte[])
         */
        public byte @Nullable [] get(byte[] key) throws RocksDBException {
            return db.get(cfHandle, key);
        }

        /**
         * Puts a key-value pair into this column family.
         *
         * @param key Key.
         * @param value Value.
         * @throws RocksDBException If failed.
         * @see RocksDB#put(ColumnFamilyHandle, byte[], byte[])
         */
        public void put(byte[] key, byte[] value) throws RocksDBException {
            db.put(cfHandle, key, value);
        }

        /**
         * Puts a key-value pair into this column family with the given {@link WriteOptions}.
         *
         * @param writeOptions Write options to use.
         * @param key Key.
         * @param value Value.
         * @throws RocksDBException If failed.
         * @see RocksDB#put(ColumnFamilyHandle, byte[], byte[])
         */
        public void put(WriteOptions writeOptions, byte[] key, byte[] value) throws RocksDBException {
            db.put(cfHandle, writeOptions, key, value);
        }

        /**
         * Puts a key-value pair into this column family within the write batch.
         *
         * @param batch Write batch.
         * @param key Key.
         * @param value Value.
         * @throws RocksDBException If failed.
         * @see WriteBatch#put(ColumnFamilyHandle, byte[], byte[])
         */
        public void put(WriteBatch batch, byte[] key, byte[] value) throws RocksDBException {
            batch.put(cfHandle, key, value);
        }

        /**
         * Deletes the entry mapped by the key and associated with this column family.
         *
         * @param key Key.
         * @throws RocksDBException If failed.
         * @see RocksDB#delete(ColumnFamilyHandle, byte[])
         */
        public void delete(byte[] key) throws RocksDBException {
            db.delete(cfHandle, key);
        }

        /**
         * Deletes the entry mapped by the key and associated with this column family within the write batch.
         *
         * @param batch Write batch.
         * @param key Key.
         * @throws RocksDBException If failed.
         * @see WriteBatch#delete(ColumnFamilyHandle, byte[])
         */
        public void delete(WriteBatch batch, byte[] key) throws RocksDBException {
            batch.delete(cfHandle, key);
        }

        /**
         * Removes all data between {@code start} (inclusive) and {@code end} (exclusive) keys.
         *
         * @param start start of the range (inclusive)
         * @param end end of the range (exclusive)
         * @throws RocksDBException if RocksDB fails to perform the operation
         */
        public void deleteRange(byte[] start, byte[] end) throws RocksDBException {
            db.deleteRange(cfHandle, start, end);
        }

        /**
         * Creates a new iterator over this column family.
         *
         * @return Iterator.
         * @see RocksDB#newIterator(ColumnFamilyHandle)
         */
        public RocksIterator newIterator() {
            return db.newIterator(cfHandle);
        }

        /**
         * Creates a new iterator with given read options over this column family.
         *
         * @param options Read options.
         * @return Iterator.
         * @see RocksDB#newIterator(ColumnFamilyHandle, ReadOptions)
         */
        public RocksIterator newIterator(ReadOptions options) {
            return db.newIterator(cfHandle, options);
        }

        /**
         * Ingests external files into this column family.
         *
         * @param paths Paths to the external files.
         * @param options Ingestion options.
         * @throws RocksDBException If failed.
         * @see RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)
         */
        public void ingestExternalFile(List<String> paths, IngestExternalFileOptions options) throws RocksDBException {
            db.ingestExternalFile(cfHandle, paths, options);
        }

        /**
         * Returns column family handle.
         *
         * @return Column family handle.
         */
        public ColumnFamilyHandle handle() {
            return cfHandle;
        }

        /**
         * Returns the private column family options, if any.
         *
         * @return The ColumnFamilyOptions, if they are exclusive to this column family.
         */
        @Nullable
        public ColumnFamilyOptions privateOptions() {
            return privateCfOptions;
        }

        /**
         * Returns name of the column family.
         *
         * @return Name of the column family.
         */
        public String name() {
            return cfName;
        }

        /**
         * Returns the name of the column family, represented as a byte array.
         */
        public byte[] nameBytes() {
            return cfNameBytes;
        }

        /**
         * Returns the RocksDB instance that contains this Column Family.
         *
         * @return RocksDB instance that contains this Column Family.
         */
        public RocksDB db() {
            return db;
        }
    }

    /**
     * Returns a list of CF descriptors present in the RocksDB instance.
     */
    private List<ColumnFamilyDescriptor> getExistingCfDescriptors(Path path) throws RocksDBException {
        String absolutePathStr = path.toAbsolutePath().toString();

        List<byte[]> existingNames;

        try (org.rocksdb.Options opts = new org.rocksdb.Options()) {
            existingNames = RocksDB.listColumnFamilies(opts, absolutePathStr);

            // Even if the database is new (no existing Column Families), we return the names of mandatory column families, that
            // will be created automatically.
            if (existingNames.isEmpty()) {
                existingNames = ColumnFamilyUtils.DEFAULT_CF_NAMES;
            }
        }

        return existingNames.stream()
                .map(cfName -> new ColumnFamilyDescriptor(cfName, createCfOptions(cfName, path)))
                .collect(toList());
    }

    @SuppressWarnings("resource")
    private ColumnFamilyOptions createCfOptions(byte[] cfName, Path path) {
        String utf8cfName = toStringName(cfName);

        switch (ColumnFamilyType.fromCfName(utf8cfName)) {
            case META:
            case GC_QUEUE:
            case DATA:
                return add(defaultCfOptions());

            case PARTITION:
                return add(defaultCfOptions().useCappedPrefixExtractor(PartitionDataHelper.ROW_PREFIX_SIZE));

            case HASH_INDEX:
                return add(defaultCfOptions().useCappedPrefixExtractor(RocksDbHashIndexStorage.FIXED_PREFIX_LENGTH));

            case SORTED_INDEX:
                return add(sortedIndexCfOptions(cfName));
            default:
                throw new StorageException("Unidentified column family: [name={}, path={}]", cfName, path);
        }
    }

    @SuppressWarnings("resource")
    private static ColumnFamilyOptions defaultCfOptions() {
        return new ColumnFamilyOptions()
                .setMemtablePrefixBloomSizeRatio(0.125)
                .setTableFormatConfig(new BlockBasedTableConfig().setFilterPolicy(new BloomFilter()))
                .setWriteBufferSize(2L * 1024 * 1024 * 1024)
                .setMemTableConfig(new SkipListMemTableConfig());
    }

    @SuppressWarnings("resource")
    static ColumnFamilyOptions sortedIndexCfOptions(byte[] cfName) {
        return new ColumnFamilyOptions()
                .setComparator(ColumnFamilyUtils.comparatorFromCfName(cfName))
                .useCappedPrefixExtractor(AbstractRocksDbIndexStorage.PREFIX_WITH_IDS_LENGTH);
    }

    private <T extends AutoCloseable> T add(T value) {
        resources.add(value);

        return value;
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
