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

package org.apache.ignite.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.ItAbstractListenerSnapshotTest;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.command.HybridTimestampMessage;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.schema.TableRowConverter;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbTableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Persistent partitions raft group snapshots tests.
 */
//@Disabled("IGNITE-16644, IGNITE-17817 MvPartitionStorage hasn't supported snapshots yet")
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class ItTablePersistenceTest extends ItAbstractListenerSnapshotTest<PartitionListener> {
    /** Factory to create RAFT command messages. */
    private final TableMessagesFactory msgFactory = new TableMessagesFactory();

    /** Factory for creating replica command messages. */
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    @InjectConfiguration("mock.tables.foo = {}")
    private TablesConfiguration tablesCfg;

    @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16777216, writeBufferSize = 16777216}}")
    private RocksDbStorageEngineConfiguration engineConfig;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final BinaryConverter keyConverter = BinaryConverter.forKey(SCHEMA);
    private static final BinaryConverter valueConverter = BinaryConverter.forValue(SCHEMA);
    private static final BinaryConverter rowConverter = BinaryConverter.forRow(SCHEMA);

    private static final Row FIRST_KEY = createKeyRow(1);

    private static final Row FIRST_VALUE = createKeyValueRow(1, 1);

    private static final Row SECOND_KEY = createKeyRow(2);

    private static final Row SECOND_VALUE = createKeyValueRow(2, 2);

    /**
     * Paths for created partition listeners.
     */
    private final Map<PartitionListener, Path> paths = new ConcurrentHashMap<>();
    private final Map<Integer, PartitionListener> partListeners = new ConcurrentHashMap<>();

    private final Map<Integer, MvTableStorage> mvTableStorages = new ConcurrentHashMap<>();

    private final Map<Integer, MvPartitionStorage> mvPartitionStorages = new ConcurrentHashMap<>();

    private final Map<Integer, PartitionDataStorage> partitionDataStorages = new ConcurrentHashMap<>();

    private final Map<Integer, TxStateStorage> txStateStorages = new ConcurrentHashMap<>();

    private final Map<Integer, TxManager> txManagers = new ConcurrentHashMap<>();

    private ReplicaService replicaService;

    private final List<TxManager> managers = new ArrayList<>();

    private final Function<String, ClusterNode> consistentIdToNode = addr
            -> new ClusterNode("node1", "node1", new NetworkAddress(addr, 3333));

    private final HybridClock hybridClock = new HybridClockImpl();

    @BeforeEach
    @Override
    public void beforeTest(TestInfo testInfo) {
        super.beforeTest(testInfo);
    }

    @AfterEach
    @Override
    public void afterTest() throws Exception {
        super.afterTest();

    }

    /** {@inheritDoc} */
    @Override
    public void beforeFollowerStop(RaftGroupService service, RaftServer server) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        TableConfiguration tableCfg = tablesCfg.tables().get("foo");
/*        StorageEngine storageEngine = new RocksDbStorageEngine(engineConfig, workDir);
        storageEngine.start();
        MvTableStorage tableStorage = storageEngine.createMvTable(tableCfg, tablesCfg);*/
        MvTableStorage tableStorage = new TestMvTableStorage(tableCfg, tablesCfg);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(0, partitionDataStorages.get(0), Map::of);

        PartitionReplicaListener partitionReplicaListener = mock(PartitionReplicaListener.class);
        when(partitionReplicaListener.invoke(any())).thenAnswer(invocationOnMock -> {
            ReplicaRequest req = invocationOnMock.getArgument(0);
            if (req instanceof ReadWriteSingleRowReplicaRequest) {
                ReadWriteSingleRowReplicaRequest req0 = (ReadWriteSingleRowReplicaRequest) req;
                BinaryRow binaryRow = req0.requestType() == RequestType.RW_UPSERT ? req0.binaryRow() : null;
                TableRow tableRow = binaryRow == null ? null : TableRowConverter.fromBinaryRow(binaryRow, rowConverter);
                UpdateCommand cmd = msgFactory.updateCommand()
                        .txId(req0.transactionId())
                        .tablePartitionId(tablePartitionId(new TablePartitionId(UUID.randomUUID(), 0)))
                        .rowUuid(new RowId(0).uuid())
                        .rowBuffer(tableRow == null ? null : tableRow.byteBuffer())
                        .safeTime(hybridTimestamp(hybridClock.now()))
                        .build();

                return service.run(cmd);
            } else if (req instanceof TxFinishReplicaRequest) {
                TxFinishReplicaRequest req0 = (TxFinishReplicaRequest) req;
                FinishTxCommand cmd = msgFactory.finishTxCommand()
                        .txId(req0.txId())
                        .commit(req0.commit())
                        .commitTimestamp(hybridTimestamp(req0.commitTimestamp()))
                        .tablePartitionIds(asList(tablePartitionId(new TablePartitionId(UUID.randomUUID(), 0))))
                        .safeTime(hybridTimestamp(hybridClock.now()))
                        .build();

                return service.run(cmd)
                        .thenCompose(ignored -> {
                            TxCleanupCommand cleanupCmd = msgFactory.txCleanupCommand()
                                    .txId(req0.txId())
                                    .commit(req0.commit())
                                    .commitTimestamp(hybridTimestamp(req0.commitTimestamp()))
                                    .safeTime(hybridTimestamp(hybridClock.now()))
                                    .build();

                            return service.run(cleanupCmd);
                        });
            }

            throw new AssertionError();
        });

        replicaService = mock(ReplicaService.class);
        when(replicaService.invoke(any(), any()))
                .thenAnswer(invocationOnMock -> partitionReplicaListener.invoke(invocationOnMock.getArgument(1)));

        for (int i = 0; i <= 2; i++) {
            TxManager txManager = new TxManagerImpl(replicaService, new HeapLockManager(), hybridClock);
            txManagers.put(i, txManager);
        }

        TxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                consistentIdToNode,
                txManagers.get(0),
                tableStorage,
                txStateTableStorage,
                replicaService,
                hybridClock
        );

        table.upsert(FIRST_VALUE, null).get();
    }

    /**
     * Method to convert from {@link HybridTimestamp} object to NetworkMessage-based {@link HybridTimestampMessage} object.
     *
     * @param tmstmp {@link HybridTimestamp} object to convert to {@link HybridTimestampMessage}.
     * @return {@link HybridTimestampMessage} object obtained from {@link HybridTimestamp}.
     */
    private HybridTimestampMessage hybridTimestamp(HybridTimestamp tmstmp) {
        return tmstmp != null ? replicaMessagesFactory.hybridTimestampMessage()
                .physical(tmstmp.getPhysical())
                .logical(tmstmp.getLogical())
                .build()
                : null;
    }

    /**
     * Method to convert from {@link TablePartitionId} object to command-based {@link TablePartitionIdMessage} object.
     *
     * @param tablePartId {@link TablePartitionId} object to convert to {@link TablePartitionIdMessage}.
     * @return {@link TablePartitionIdMessage} object converted from argument.
     */
    private TablePartitionIdMessage tablePartitionId(TablePartitionId tablePartId) {
        return msgFactory.tablePartitionIdMessage()
                .tableId(tablePartId.tableId())
                .partitionId(tablePartId.partitionId())
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public void afterFollowerStop(RaftGroupService service, RaftServer server, int stoppedNodeIndex) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                consistentIdToNode,
                txManagers.get(0),
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                replicaService,
                mock(HybridClock.class)
        );

        // Remove the first key
        table.delete(FIRST_KEY, null).get();

        // Put deleted data again
        table.upsert(FIRST_VALUE, null).get();

        mvTableStorages.get(stoppedNodeIndex).stop();
        paths.remove(partListeners.get(stoppedNodeIndex));
    }

    /** {@inheritDoc} */
    @Override
    public void afterSnapshot(RaftGroupService service) throws Exception {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
        var table = new InternalTableImpl(
                "table",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, service),
                1,
                consistentIdToNode,
                txManagers.get(0),
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                replicaService,
                mock(HybridClock.class)
        );

        table.upsert(SECOND_VALUE, null).get();

        //assertNotNull(table.get(SECOND_KEY, null).join());
    }

    /** {@inheritDoc} */
    @Override
    public BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot) {
        MvPartitionStorage storage = getListener(restarted, raftGroupId()).getMvStorage();
        Map<ByteBuffer, RowId> primaryIndex = rowsToRowIds(storage);

        Row key = interactedAfterSnapshot ? SECOND_KEY : FIRST_KEY;
        Row value = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

        return () -> {
            RowId rowId = primaryIndex.get(key.keySlice());

            assertNotNull(rowId, "No rowId in storage");

            ReadResult read = storage.read(rowId, HybridTimestamp.MAX_VALUE);

            if (read == null) {
                return false;
            }

            return Arrays.equals(value.bytes(), rowConverter.fromTuple(read.tableRow().tupleSlice()).bytes());
        };
    }

    private static Map<ByteBuffer, RowId> rowsToRowIds(MvPartitionStorage storage) {
        Map<ByteBuffer, RowId> result = new HashMap<>();

        RowId rowId = storage.closestRowId(RowId.lowestRowId(0));

        while (rowId != null) {
            BinaryRow binaryRow = rowConverter.fromTuple(storage.read(rowId, HybridTimestamp.MAX_VALUE).tableRow().tupleSlice());
            if (binaryRow != null) {
                result.put(binaryRow.keySlice(), rowId);
            }

            RowId incremented = rowId.increment();
            if (incremented == null) {
                break;
            }

            rowId = storage.closestRowId(incremented);
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override
    public Path getListenerPersistencePath(PartitionListener listener, RaftServer server) {
        return paths.get(listener);
    }

    /** {@inheritDoc} */
    @Override
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17817 Use Replica layer with new transaction protocol.
    public RaftGroupListener createListener(TestInfo testInfo, ClusterService service, Path workDir, int index) {
        return paths.entrySet().stream()
                .filter(entry -> entry.getValue().equals(workDir))
                .map(Map.Entry::getKey)
                .findAny()
                .orElseGet(() -> {
                    TableConfiguration tableCfg = tablesCfg.tables().get("foo");

                    tableCfg.change(t -> t.changePartitions(1)).join();

                    Path path = workDir.resolve(testNodeName(testInfo, index));

                    RocksDbStorageEngine storageEngine = new RocksDbStorageEngine(engineConfig, path);
                    storageEngine.start();
                    tableCfg.dataStorage().change(ds -> ds.convert(storageEngine.name())).join();

                    MvTableStorage mvTableStorage = storageEngine.createMvTable(tableCfg, tablesCfg);
                    mvTableStorage.start();
                    mvTableStorages.put(index, mvTableStorage);
                    MvPartitionStorage mvPartitionStorage = mvTableStorage.getOrCreateMvPartition(0);
                    mvPartitionStorages.put(index, mvPartitionStorage);
                    PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartitionStorage);
                    partitionDataStorages.put(index, partitionDataStorage);

                    TxStateStorage txStateStorage = new TestTxStateStorage();
                    txStateStorages.put(index, txStateStorage);

                    StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(0, partitionDataStorage, Map::of);

                    PartitionListener listener = new PartitionListener(
                            partitionDataStorage,
                            storageUpdateHandler,
                            new TestTxStateStorage(),
                            new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0))
                    );

                    paths.put(listener, workDir);
                    partListeners.put(index, listener);

                    return listener;
                });
    }

    /** {@inheritDoc} */
    @Override
    public TestReplicationGroupId raftGroupId() {
        return new TestReplicationGroupId("partitions");
    }

    /**
     * Creates a {@link Row} with the supplied key.
     *
     * @param id Key.
     * @return Row.
     */
    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates a {@link Row} with the supplied key and value.
     *
     * @param id    Key.
     * @param value Value.
     * @return Row.
     */
    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
