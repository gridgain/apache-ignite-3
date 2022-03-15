package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableTxManagerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class ItRebalanceDistributedTest {

    public static final int BASE_PORT = 20_000;

    public static final String HOST = "localhost";

    private static TestScaleCubeClusterServiceFactory testScaleCubeClusterServiceFactory = new TestScaleCubeClusterServiceFactory();

    private static StaticNodeFinder finder = new StaticNodeFinder(List.of(new NetworkAddress(HOST, BASE_PORT), new NetworkAddress(HOST, BASE_PORT + 1), new NetworkAddress(HOST, BASE_PORT + 2)));

    private static List<Node> nodes;

    @BeforeEach
    private void before(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Files.createDirectory(workDir.resolve("" + i));
            var node = new Node(testInfo, workDir.resolve("" + i), HOST, BASE_PORT + i);
            nodes.add(node);
            node.start();
        }
    }

    @AfterEach
    private void after() throws Exception {
        for (Node node: nodes) {
            node.stop();
        }
    }

    @Test
    void test1Rebalance(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getAssignments(0).get(0).size());
        assertEquals(2, getAssignments(1).get(0).size());
        assertEquals(2, getAssignments(2).get(0).size());
    }

    @Test
    void test2Rebalance(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getAssignments(0).get(0).size());
        assertEquals(3, getAssignments(1).get(0).size());
        assertEquals(3, getAssignments(2).get(0).size());
    }

    @Test
    void test3Rebalance(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tableManager.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));
        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getAssignments(0).get(0).size());
        assertEquals(2, getAssignments(1).get(0).size());
        assertEquals(2, getAssignments(2).get(0).size());
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) {
        while (!List.of(0, 1, 2).stream().allMatch(n -> getAssignments(n).get(partNum).size() == replicasNum)) {
            LockSupport.parkNanos(100_000_000);
        }
    }

    private List<List<ClusterNode>> getAssignments(int nodeNum) {
        return (List<List<ClusterNode>>) ByteUtils.fromBytes(
                ((ExtendedTableConfiguration) nodes.get(nodeNum).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1")).assignments().value());
    }

    private static class Node {
        private final String name;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final Loza raftManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final TableManager tableManager;

        private final BaselineManager baselineMgr;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, Path workDir, String host, int port) {
            var addr = new NetworkAddress(host, port);

            name = testNodeName(testInfo, addr.port());

            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault" + port)));

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                    NodeConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY),
                    Map.of(),
                    new LocalConfigurationStorage(vaultManager),
                    List.of(),
                    List.of()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder,
                    testScaleCubeClusterServiceFactory
            );

            lockManager = new HeapLockManager();

            raftManager = new Loza(clusterService, workDir);

            txManager = new TableTxManagerImpl(clusterService, lockManager);

            List<RootKey<?, ?>> rootKeys = List.of(
                    NodeConfiguration.KEY,
                    TablesConfiguration.KEY,
                    DataStorageConfiguration.KEY);

            metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    nodeCfgMgr,
                    clusterService,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage()
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(ClusterConfiguration.KEY,
                            TablesConfiguration.KEY,
                    DataStorageConfiguration.KEY),
                    Map.of(),
                    cfgStorage,
                    List.of(ExtendedTableConfigurationSchema.class),
                    List.of(RocksDbDataRegionConfigurationSchema.class, HashIndexConfigurationSchema.class)
            );

            Consumer<Consumer<Long>> registry = (c) -> {
                clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(newStorageRevision -> {
                    c.accept(newStorageRevision);

                    return CompletableFuture.completedFuture(null);
                });
            };

            TablesConfiguration tablesCfg = clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

            DataStorageConfiguration dataStorageCfg = clusterCfgMgr.configurationRegistry().getConfiguration(DataStorageConfiguration.KEY);

            baselineMgr = new BaselineManager(clusterCfgMgr.configurationRegistry().getConfiguration(ClusterConfiguration.KEY), clusterService);

            tableManager = new TableManager(
                    registry,
                    tablesCfg,
                    dataStorageCfg,
                    raftManager,
                    baselineMgr,
                    clusterService.topologyService(),
                    workDir.resolve("store"),
                    txManager,
                    metaStorageManager);
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            nodeCfgMgr.start();

            // metastorage configuration
            var config = String.format("{\"node\": {\"metastorageNodes\": [ \"%s\" ]}}", nodes.get(0).name);

            nodeCfgMgr.bootstrap(config);


            Stream.of(clusterService, clusterCfgMgr, raftManager, txManager, metaStorageManager, baselineMgr, tableManager).forEach(IgniteComponent::start);


            CompletableFuture.allOf(
                    nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                    clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners()
            ).get();


            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components =
                    List.of(tableManager, metaStorageManager, raftManager, txManager, clusterService, nodeCfgMgr, vaultManager);

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            for (IgniteComponent component : components) {
                component.stop();
            }
        }
    }

}
