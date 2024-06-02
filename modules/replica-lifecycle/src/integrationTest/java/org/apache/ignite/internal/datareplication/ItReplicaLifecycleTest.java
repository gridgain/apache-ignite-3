package org.apache.ignite.internal.datareplication;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.BaseIgniteRestartTest.createVault;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.placementdriver.ZoneBasedPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class ItReplicaLifecycleTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItReplicaLifecycleTest.class);

    private static final int NODE_COUNT = 1;

    private static final int BASE_PORT = 20_000;

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    private StaticNodeFinder finder;

    private static final String HOST = "localhost";

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = \"aipersist\", test.engine=\"test\"}")
    private static StorageConfiguration storageConfiguration;

    @WorkDirectory
    private Path workDir;

    private List<Node> nodes;

    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        nodes = new ArrayList<>();

        List<NetworkAddress> nodeAddresses = new ArrayList<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);

        for (NetworkAddress addr : nodeAddresses) {
            var node = new Node(testInfo, addr);

            nodes.add(node);

            node.start();
        }

        Node node0 = getNode(0);

        node0.cmgManager.initCluster(List.of(node0.name), List.of(node0.name), "cluster");

        nodes.forEach(Node::waitWatches);

        assertThat(
                allOf(nodes.stream().map(n -> n.cmgManager.onJoinReady()).toArray(CompletableFuture[]::new)),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(
                () -> {
                    CompletableFuture<LogicalTopologySnapshot> logicalTopologyFuture = node0.cmgManager.logicalTopology();

                    assertThat(logicalTopologyFuture, willCompleteSuccessfully());

                    return logicalTopologyFuture.join().nodes().size() == NODE_COUNT;
                },
                AWAIT_TIMEOUT_MILLIS
        ));
    }

    @AfterEach
    void after() {
        nodes.forEach(Node::stop);
    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    @Test
    public void testEmptyReplicaListener() throws NodeStoppingException {
        Node node = getNode(0);

        createZone(node, "test_zone", 1, 1);
        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        createTable(node, "test_zone", "test_table");
        int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

        node.converter.put(new TablePartitionId(tableId, 0), new ZonePartitionId(zoneId, 0));

        KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

        assertDoesNotThrow(() -> keyValueView.put(null, 1L, 1));

        // Actually we are testing not the fair put value, but the hardcoded one from temporary noop replica listener
        assertEquals(-1, keyValueView.get(null, 1L));
    }

    private static void createZone(Node node, String zoneName, int partitions, int replicas) {
        createZone(node, zoneName, partitions, replicas, false);
    }

    private static void createTable(Node node, String zoneName, String tableName) {
        node.waitForMetadataCompletenessAtNow();

        TableTestUtils.createTable(
                node.catalogManager,
                DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT64).build(),
                        ColumnParams.builder().name("val").type(INT32).nullable(true).build()
                ),
                List.of("key")
        );
    }

    private static void createZone(Node node, String zoneName, int partitions, int replicas, boolean testStorageProfile) {
        DistributionZonesTestUtil.createZoneWithStorageProfile(
                node.catalogManager,
                zoneName,
                partitions,
                replicas,
                testStorageProfile ? DEFAULT_TEST_PROFILE_NAME : DEFAULT_STORAGE_PROFILE
        );
    }

    class Node {
        final String name;

        final Loza raftManager;

        final ThreadPoolsManager threadPoolsManager;

        final ReplicaManager replicaManager;

        final MetaStorageManager metaStorageManager;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TableManager tableManager;

        private final DistributionZoneManager distributionZoneManager;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        private final CatalogManager catalogManager;

        private final ReplicaLifecycleManager replicaLifecycleManager;

        private final SchemaSyncService schemaSyncService;

        private final ClockWaiter clockWaiter;

        private final List<IgniteComponent> nodeComponents = new CopyOnWriteArrayList<>();

        private final ConfigurationTreeGenerator nodeCfgGenerator;

        private final ConfigurationTreeGenerator clusterCfgGenerator;

        private final Map<TablePartitionId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
                = new ConcurrentHashMap<>();

        private final NetworkAddress networkAddress;

        private final LowWatermarkImpl lowWatermark;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private CompletableFuture<Void> deployWatchesFut;

        /** Hybrid clock. */
        private final HybridClock hybridClock = new HybridClockImpl();

        /** Index manager. */
        private final IndexManager indexManager;

        /** Failure processor. */
        private final FailureProcessor failureProcessor;

        private final ScheduledExecutorService rebalanceScheduler;

        private final Map<ReplicationGroupId, ReplicationGroupId> converter = new ConcurrentHashMap<>();

        private final LogStorageFactory logStorageFactory;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {
            networkAddress = addr;

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(dir);

            nodeCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(
                            NetworkConfiguration.KEY,
                            StorageConfiguration.KEY),
                    List.of(
                            PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                            VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class
                    ),
                    List.of(
                            PersistentPageMemoryProfileConfigurationSchema.class,
                            VolatilePageMemoryProfileConfigurationSchema.class,
                            UnsafeMemoryAllocatorConfigurationSchema.class
                    )
            );

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            TestIgnitionManager.addDefaultsToConfigurationFile(configPath);

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            StorageConfiguration.KEY),
                    new LocalFileConfigurationStorage(configPath, nodeCfgGenerator, null),
                    nodeCfgGenerator,
                    new TestConfigurationValidator()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            logStorageFactory = SharedLogStorageFactoryUtils.create(clusterService.nodeName(), dir, raftConfiguration);

            raftManager = new Loza(
                    clusterService,
                    new NoOpMetricManager(),
                    raftConfiguration,
                    dir,
                    hybridClock,
                    raftGroupEventsClientListener,
                    logStorageFactory
            );

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            failureProcessor = new FailureProcessor(name);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureProcessor
            );

            LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            KeyValueStorage keyValueStorage = new SimpleInMemoryKeyValueStorage(name);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    keyValueStorage,
                    hybridClock,
                    topologyAwareRaftGroupServiceFactory,
                    new NoOpMetricManager(),
                    metaStorageConfiguration
            );

            var placementDriver = new ZoneBasedPlacementDriver(clusterService);

            threadPoolsManager = new ThreadPoolsManager(name);

            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock,
                    threadPoolsManager.partitionOperationsExecutor(),
                    replicationConfiguration,
                    threadPoolsManager.commonScheduler()
            );

            var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            clockWaiter = new ClockWaiter(name, hybridClock);

            ClockService clockService = new ClockServiceImpl(
                    hybridClock,
                    clockWaiter,
                    () -> TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS
            );

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService);

            cfgStorage = new DistributedConfigurationStorage("test", metaStorageManager);

            clusterCfgGenerator = new ConfigurationTreeGenerator(GcConfiguration.KEY);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(
                            GcConfiguration.KEY
                    ),
                    cfgStorage,
                    clusterCfgGenerator,
                    new TestConfigurationValidator()
            );

            ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

            Consumer<LongFunction<CompletableFuture<?>>> registry = (LongFunction<CompletableFuture<?>> function) ->
                    metaStorageManager.registerRevisionUpdateListener(function::apply);

            GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new PersistentPageMemoryDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");


            dataStorageMgr = new DataStorageManager(
                    dataStorageModules.createStorageEngines(
                            name,
                            nodeCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null,
                            failureProcessor,
                            raftManager.getLogSyncer()
                    ),
                    storageConfiguration
            );

            lowWatermark = new LowWatermarkImpl(
                    name,
                    gcConfig.lowWatermark(),
                    clockService,
                    vaultManager,
                    failureProcessor,
                    clusterService.messagingService()
            );

            txManager = new TxManagerImpl(
                    txConfiguration,
                    clusterService,
                    replicaSvc,
                    lockManager,
                    clockService,
                    new TransactionIdGenerator(addr.port()),
                    placementDriver,
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new TestLocalRwTxCounter(),
                    resourcesRegistry,
                    transactionInflights,
                    lowWatermark
            );

            replicaManager = new ReplicaManager(
                    name,
                    clusterService,
                    cmgManager,
                    clockService,
                    Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    threadPoolsManager.partitionOperationsExecutor(),
                    new NoOpFailureProcessor(),
                    t -> (converter.get(t) != null) ? converter.get(t) : t
            );

            LongSupplier delayDurationMsSupplier = () -> 10L;

            catalogManager = new CatalogManagerImpl(
                    new UpdateLogImpl(metaStorageManager),
                    clockService,
                    delayDurationMsSupplier,
                    partitionIdleSafeTimePropagationPeriodMsSupplier
            );

            schemaManager = new SchemaManager(registry, catalogManager);

            schemaSyncService = new SchemaSyncServiceImpl(metaStorageManager.clusterTime(), delayDurationMsSupplier);

            rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                    NamedThreadFactory.create(name, "test-rebalance-scheduler", logger()));

            distributionZoneManager = new DistributionZoneManager(
                    name,
                    registry,
                    metaStorageManager,
                    logicalTopologyService,
                    catalogManager,
                    rebalanceScheduler
            );

            replicaLifecycleManager = new ReplicaLifecycleManager(
                    catalogManager,
                    raftManager,
                    replicaManager,
                    topologyAwareRaftGroupServiceFactory,
                    distributionZoneManager,
                    metaStorageManager,
                    clusterService.topologyService(),
                    clusterService.serializationRegistry(),
                    clockService,
                    placementDriver
            );

            StorageUpdateConfiguration storageUpdateConfiguration = clusterConfigRegistry.getConfiguration(StorageUpdateConfiguration.KEY);

            HybridClockImpl clock = new HybridClockImpl();

            tableManager = new TableManager(
                    name,
                    registry,
                    gcConfig,
                    txConfiguration,
                    storageUpdateConfiguration,
                    clusterService.messagingService(),
                    clusterService.topologyService(),
                    clusterService.serializationRegistry(),
                    raftManager,
                    replicaManager,
                    mock(LockManager.class),
                    replicaSvc,
                    txManager,
                    dataStorageMgr,
                    storagePath,
                    metaStorageManager,
                    schemaManager,
                    view -> new LocalLogStorageFactory(),
                    threadPoolsManager.tableIoExecutor(),
                    threadPoolsManager.partitionOperationsExecutor(),
                    clock,
                    clockService,
                    new OutgoingSnapshotsManager(clusterService.messagingService()),
                    topologyAwareRaftGroupServiceFactory,
                    distributionZoneManager,
                    schemaSyncService,
                    catalogManager,
                    new HybridTimestampTracker(),
                    placementDriver,
                    () -> mock(IgniteSql.class),
                    resourcesRegistry,
                    rebalanceScheduler,
                    lowWatermark,
                    transactionInflights
            ) ;

            indexManager = new IndexManager(
                    schemaManager,
                    tableManager,
                    catalogManager,
                    threadPoolsManager.tableIoExecutor(),
                    registry,
                    lowWatermark
            );
        }

        private void waitForMetadataCompletenessAtNow() {
            assertThat(schemaSyncService.waitForMetadataCompleteness(hybridClock.now()), willCompleteSuccessfully());
        }

        /**
         * Starts the created components.
         */
        void start() {
            List<IgniteComponent> firstComponents = List.of(
                    threadPoolsManager,
                    vaultManager,
                    nodeCfgMgr,
                    failureProcessor,
                    clusterService,
                    logStorageFactory,
                    raftManager,
                    cmgManager
            );

            ComponentContext componentContext = new ComponentContext();

            List<CompletableFuture<?>> componentFuts =
                    firstComponents.stream()
                            .map(component -> component.startAsync(componentContext))
                            .collect(Collectors.toList());

            nodeComponents.addAll(firstComponents);

            deployWatchesFut = CompletableFuture.supplyAsync(() -> {
                List<IgniteComponent> secondComponents = List.of(
                        lowWatermark,
                        metaStorageManager,
                        clusterCfgMgr,
                        clockWaiter,
                        catalogManager,
                        distributionZoneManager,
                        replicaManager,
                        txManager,
                        dataStorageMgr,
                        schemaManager,
                        replicaLifecycleManager,
                        tableManager,
                        indexManager
                );

                componentFuts.addAll(secondComponents.stream()
                        .map(component -> component.startAsync(componentContext)).collect(Collectors.toList()));

                nodeComponents.addAll(secondComponents);

                var configurationNotificationFut = metaStorageManager.recoveryFinishedFuture().thenCompose(rev -> {
                    return allOf(
                            nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart()
                    );
                });

                assertThat(configurationNotificationFut, willSucceedIn(1, TimeUnit.MINUTES));

                lowWatermark.scheduleUpdates();

                return metaStorageManager.deployWatches();
            }).thenCombine(allOf(componentFuts.toArray(CompletableFuture[]::new)), (deployWatchesFut, unused) -> null);
        }

        /**
         * Waits for watches deployed.
         */
        void waitWatches() {
            assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
        }

        /**
         * Stops the created components.
         */
        void stop() {
            List<IgniteComponent> components = new ArrayList<>(nodeComponents);
            reverse(components);

            for (IgniteComponent component : components) {
                try {
                    component.beforeNodeStop();
                } catch (Exception e) {
                    LOG.error("Unable to execute before node stop [component={}]", e, component);
                }
            }

            assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());

            nodeCfgGenerator.close();
            clusterCfgGenerator.close();
        }
    }
}
