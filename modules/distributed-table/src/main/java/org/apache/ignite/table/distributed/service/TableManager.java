package org.apache.ignite.table.distributed.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.DistributedTableUtils;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.configuration.MetastoreManagerConfiguration;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.apache.ignite.table.distributed.configuration.TableView;
import org.apache.ignite.table.distributed.service.command.GetCommand;
import org.apache.ignite.table.distributed.service.command.PutCommand;
import org.apache.ignite.table.distributed.service.command.response.TableRowResponse;
import org.jetbrains.annotations.NotNull;

public class TableManager {
    /** Internal prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    private LogWrapper log = new LogWrapper(TableManager.class);

    /** Meta storage service. */
    private MetaStorageService metaStorageService;

    private NetworkCluster networkCluster;

    private ConfigurationModule configurationModule;

    private Map<UUID, Map<Integer, RaftGroupService>> tablePartitionMap;

    public TableManager() {
        int startRevision = 0;
        tablePartitionMap = new HashMap<>();

        String[] metastoragePeerNames = configurationModule.configurationRegistry()
            .getConfiguration(MetastoreManagerConfiguration.KEY).names().value();

        NetworkMember localMember = networkCluster.localMember();

        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastoragePeerNames) {
            if (name.equals(localMember.name())) {
                isLocalNodeHasMetasorage = true;

                break;
            }
        }

        if (isLocalNodeHasMetasorage) {
            configurationModule.configurationRegistry()
                .getConfiguration(DistributedTableConfiguration.KEY).tables()
                .listen(ctx -> {
                    HashSet<String> tblNamesToStart = new HashSet<>(ctx.newValue().namedListKeys());

                    long revision = ctx.storageRevision();

                    if (ctx.oldValue() != null)
                        tblNamesToStart.removeAll(ctx.oldValue().namedListKeys());

                    for (String tblName : tblNamesToStart) {
                        TableView tableView = ctx.newValue().get(tblName);
                        long update = 0;

                        UUID tblId = new UUID(revision, update);

                        String tableInternalPrefix = INTERNAL_PREFIX + tblId.toString();

                        CompletableFuture<Boolean> fut = metaStorageService.invoke(
                            new Key(INTERNAL_PREFIX + tblId.toString()),
                            Conditions.value().eq(null),
                            Operations.put(tableView.name().getBytes(StandardCharsets.UTF_8)),
                            Operations.noop());

                        try {
                            if (fut.get()) {
                                metaStorageService.put(new Key(tableInternalPrefix + ".assignment"), null);

                                log.info("Table manager created a table [name={}, revision={}]",
                                    tableView.name(), revision);
                            }
                        }
                        catch (InterruptedException | ExecutionException e) {
                            log.error("Table was not fully initialized [name={}, revision={}]",
                                tableView.name(), revision, e);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                });
        }

        String tableInternalPrefix = INTERNAL_PREFIX + "#.assignment";

        metaStorageService.watch(new Key(tableInternalPrefix), startRevision, new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    if (evt.newEntry().value() != null) {
                        UUID tblId = UUID.fromString(Pattern.compile("#").matcher(evt.newEntry().key().toString()).group());

                        try {
                            String name = new String(metaStorageService.get(
                                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                                .value(), StandardCharsets.UTF_8);
                            int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                                .tables().get(name).partitions().value();

                            List<List<NetworkMember>> assignment = (List<List<NetworkMember>>)DistributedTableUtils.fromBytes(
                                evt.newEntry().value());

                            HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                            for (int p = 0; p < partitions; p++) {
                                List<Peer> peers = new ArrayList<>();

                                for (NetworkMember member : assignment.get(p))
                                    peers.add(new Peer(member));

                                partitionMap.put(p, new RaftGroupServiceImpl("name" + "_part_" + p,
                                    networkCluster, FACTORY, TIMEOUT, peers, true, DELAY));
                            }

                            tablePartitionMap.put(tblId, partitionMap);
                        }
                        catch (InterruptedException | ExecutionException e) {
                            log.error("Failed to start table [key={}]",
                                evt.newEntry().key().toString(), e);
                        }
                    }
                }

                return false;
            }

            @Override public void onError(@NotNull Throwable e) {
                log.error("Metastorage listener issue", e);
            }
        });
    }

    public TableRow put(UUID tblId, TableRow row) {
        try {
            String name = new String(metaStorageService.get(
                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                .value(), StandardCharsets.UTF_8);

            int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                .tables().get(name).partitions().value();

            return tablePartitionMap.get(tblId).get(row.keyChunk().hashCode() % partitions).<TableRowResponse>run(new PutCommand(row)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to put some value [tblId={}, row={}]", tblId, row);
        }

        //TODO: Throw exception.
        return null;
    }

    public TableRow get(UUID tblId, TableRow keyRow) {
        try {
            String name = new String(metaStorageService.get(
                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                .value(), StandardCharsets.UTF_8);

            int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                .tables().get(name).partitions().value();

            return tablePartitionMap.get(tblId).get(keyRow.keyChunk().hashCode() % partitions).<TableRowResponse>run(new GetCommand(keyRow)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get some value [tblId={}, keyRow={}]", tblId, keyRow);
        }

        //TODO: Throw exception.
        return null;
    }
}
