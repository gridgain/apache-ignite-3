package org.apache.ignite.internal.affinity.ditributed;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.DistributedTableUtils;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.configuration.MetastoreManagerConfiguration;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.jetbrains.annotations.NotNull;

public class AffinityManager {
    /** Tables prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Meta storage service. */
    private MetaStorageService metaStorageService;

    /** Network cluster. */
    private NetworkCluster networkCluster;

    /** Configuration module. */
    private ConfigurationModule configurationModule;

    /** Logger. */
    private LogWrapper log = new LogWrapper(AffinityManager.class);

    /**
     * @param configurationModule Configuration module.
     * @param networkCluster Network cluster.
     * @param metaStorageService Meta storage service.
     */
    public AffinityManager(
        ConfigurationModule configurationModule,
        NetworkCluster networkCluster,
        MetaStorageService metaStorageService
    ) {
        int startRevision =0;

        this.configurationModule = configurationModule;
        this.networkCluster = networkCluster;
        this.metaStorageService = metaStorageService;

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
                            String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                            String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                            UUID tblId = UUID.fromString(placeholderValue);

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
