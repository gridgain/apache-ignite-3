package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationListenerHolder;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
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
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class ItRebalanceDistributedTest1 {

    public static final int BASE_PORT = 10000;
    public static final String HOST = "localhost";
    private static TestScaleCubeClusterServiceFactory testScaleCubeClusterServiceFactory = new TestScaleCubeClusterServiceFactory();

    private static StaticNodeFinder finder = new StaticNodeFinder(List.of(new NetworkAddress(HOST, BASE_PORT), new NetworkAddress(HOST, BASE_PORT + 1), new NetworkAddress(HOST, BASE_PORT + 2)));

    private static String firstName;

    @Test
    void test() throws Exception {

        var nodes = startGrid();

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        nodes.get(0).distributedTblMgr.createTable(
                "PUBLIC.tbl1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeReplicas(1)
                        .changePartitions(1));

        assertEquals(1, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        nodes.get(0).distributedTblMgr.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(2));
        nodes.get(0).distributedTblMgr.alterTable("PUBLIC.TBL1", ch -> ch.changeReplicas(3));

        UUID tableId = ((TableImpl) nodes.get(0).distributedTblMgr.table("PUBLIC.TBL1")).internalTable().tableId();

        assertEquals(3, nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1").replicas().value());

        System.out.println("AAND");

        Supplier<List<List<ClusterNode>>> getNodes = () -> {
            return (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableConfiguration) nodes.get(0).clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get("PUBLIC.TBL1")).assignments().value());
        };
        while (getNodes.get().get(0).size() != 3) {
            LockSupport.parkNanos(1000_000_000);
//            System.out.println(((List<List<ClusterNode>>) ByteUtils.fromBytes(nodes.get(0).metaStorageManager.get(RebalanceUtil.partAssignmentsStableKey(tableId + "_part_" + 0)).join().value())).get(0).stream().map(ClusterNode::toString).collect(
//                    Collectors.joining(",")));
//            System.out.println(((List<List<ClusterNode>>) ByteUtils.fromBytes(nodes.get(0).metaStorageManager.get(RebalanceUtil.partAssignmentsPlannedKey(tableId + "_part_" + 0)).join().value())).get(0).stream().map(ClusterNode::toString).collect(
//                    Collectors.joining(",")));
        }
        assertEquals(3, getNodes.get().size());
        nodes.get(0).metaStorageMgr.prefix(ByteArray.fromString(tableId.toString())).forEach(e -> System.out.println(new ByteArray(e.key()).toString()));
//        for (Node node : nodes) {
//            node.stop();
//        }
    }

    @NotNull
    protected List<IgniteImpl> startGrid() {
        nodesBootstrapCfg.forEach((nodeName, configStr) ->
                clusterNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        assertEquals(3, clusterNodes.size());

        return clusterNodes.stream().map(i -> (IgniteImpl) i).collect(Collectors.toList());
    }


    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<Ignite> clusterNodes = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, PORTS[0]);
        String node1Name = testNodeName(testInfo, PORTS[1]);
        String node2Name = testNodeName(testInfo, PORTS[2]);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[0] + ",\n"
                        + "    nodeFinder:{\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[1] + ",\n"
                        + "    nodeFinder:{\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node2Name,
                "{\n"
                        + "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[2] + ",\n"
                        + "    nodeFinder:{\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(ItUtils.reverse(clusterNodes));
    }
}
