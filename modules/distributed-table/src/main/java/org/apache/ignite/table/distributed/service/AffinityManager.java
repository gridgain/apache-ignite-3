package org.apache.ignite.table.distributed.service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.DistributedTableUtils;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.configuration.MetastoreManagerConfiguration;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.table.distributed.affinity.RendezvousAffinityFunction;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.jetbrains.annotations.NotNull;

public class AffinityManager {

    /** Tables prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    private MetaStorageService metaStorageService;

    private NetworkCluster networkCluster;

    private ConfigurationModule configurationModule;

    private LogWrapper log = new LogWrapper(TableManager.class);

    public AffinityManager() {
        int startRevision =0;

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
            String tableInternalPrefix = INTERNAL_PREFIX + "#.assignment";

            metaStorageService.watch(new Key(tableInternalPrefix), startRevision, new WatchListener() {
                @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                    for (WatchEvent evt : events) {
                        if (evt.newEntry().value() == null) {
                            UUID tblId = UUID.fromString(Pattern.compile("#").matcher(evt.newEntry().key().toString()).group());

                            try {
                                String name = new String(metaStorageService.get(
                                    new Key(INTERNAL_PREFIX + tblId.toString())).get()
                                    .value(), StandardCharsets.UTF_8);

                                int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                                    .tables().get(name).partitions().value();
                                int backups = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                                    .tables().get(name).backups().value();

                                metaStorageService.put(evt.newEntry().key(), DistributedTableUtils.toBytes(
                                    new RendezvousAffinityFunction(
                                        false,
                                        partitions
                                    ).assignPartitions(
                                        networkCluster.allMembers(),
                                        backups
                                    ))
                                );

                                log.info("Affinity manager calculated assignment for the table [name={}, tblId={}]",
                                    name, tblId);
                            }
                            catch (InterruptedException | ExecutionException e) {
                                log.error("Failed to initialize affinity [key={}]",
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
    }
}
