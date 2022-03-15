package org.apache.ignite.internal.configuration.storage;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableTxManagerImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class ItRebalanceDistributedTest {

    public static final int BASE_PORT = 10000;
    public static final String HOST = "localhost";
    private static TestScaleCubeClusterServiceFactory testScaleCubeClusterServiceFactory = new TestScaleCubeClusterServiceFactory();

    private static String firstName;

    @Test
    void test(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Files.createDirectory(workDir.resolve("" + i));
            var node = new Node(testInfo, workDir.resolve("" + i), HOST, BASE_PORT + i);
            if (firstName == null)
                firstName = node.name;
            nodes.add(node);
            node.start();
        }

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

        nodes.get(0).tableManager.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));

        UUID tableId = ((TableImpl) nodes.get(0).tableManager.table("PUBLIC.TBL1")).internalTable().tableId();

        assertEquals(3, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        System.out.println("AAND");
//        for (byte i = 0; i < 256; i++)
//            nodes.get(0).metaStorageManager.prefix(ByteArray.fromString(tableId.toString())).forEach(e -> System.out.println(new ByteArray(e.key()).toString()));

        Thread.sleep(5000);
        nodes.get(0).metaStorageManager.prefix(ByteArray.fromString(tableId.toString())).forEach(e -> System.out.println(new ByteArray(e.key()).toString()));
        for (Node node : nodes) {
            node.stop();
        }
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

            var modules = loadConfigurationModules(defaultServiceClassLoader());

            name = testNodeName(testInfo, addr.port());

            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault" + port)));

            nodeCfgMgr = new ConfigurationManager(
                    modules.local().rootKeys(),
                    modules.local().validators(),
                    new LocalConfigurationStorage(vaultManager),
                    modules.local().internalSchemaExtensions(),
                    modules.local().polymorphicSchemaExtensions()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(List.of(new NetworkAddress(host, BASE_PORT), new NetworkAddress(host, BASE_PORT + 1), new NetworkAddress(host, BASE_PORT + 2))),
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
                    modules.distributed().rootKeys(),
                    modules.distributed().validators(),
                    cfgStorage,
                    modules.distributed().internalSchemaExtensions(),
                    modules.distributed().polymorphicSchemaExtensions()
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

        private ClassLoader defaultServiceClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        private ConfigurationModules loadConfigurationModules(ClassLoader classLoader) {
            var modulesProvider = new ServiceLoaderModulesProvider();
            List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

            if (modules.isEmpty()) {
                throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                        + "Please make sure that the classloader for loading services is correct.");
            }

            var configModules = new ConfigurationModules(modules);
            return configModules;
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            nodeCfgMgr.start();

            // metastorage configuration
            var config = String.format("{\"node\": {\"metastorageNodes\": [ \"%s\" ]}}", firstName);

            nodeCfgMgr.bootstrap(config);

            Stream.of(clusterService, clusterCfgMgr, raftManager, txManager, metaStorageManager, baselineMgr, tableManager).forEach(IgniteComponent::start);

            CompletableFuture.allOf(
                    nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                    clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners()
            ).get();

            // this is needed to avoid assertion errors
            cfgStorage.registerConfigurationListener(changedEntries -> completedFuture(null));

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
