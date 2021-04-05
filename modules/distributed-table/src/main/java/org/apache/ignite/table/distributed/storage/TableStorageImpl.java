package org.apache.ignite.table.distributed.storage;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.apache.ignite.table.distributed.service.TableManagerImpl;
import org.apache.ignite.table.distributed.service.command.GetCommand;
import org.apache.ignite.table.distributed.service.command.PutCommand;
import org.apache.ignite.table.distributed.service.command.response.TableRowResponse;

/**
 * Storage of table rows.
 */
public class TableStorageImpl implements TableStorage {
    /** Internal prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Logger. */
    private LogWrapper log = new LogWrapper(TableManagerImpl.class);

    /** Meta storage service. */
    private MetaStorageService metaStorageService;

    /** Configuration module. */
    private ConfigurationModule configurationModule;

    /** Table id. */
    private UUID tblId;

    /** Partition map. */
    Map<Integer, RaftGroupService> partitionMap;

    /**
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     */
    public TableStorageImpl(
        ConfigurationModule configurationModule,
        MetaStorageService metaStorageService,
        UUID tableId,
        Map<Integer, RaftGroupService> partMap
    ) {
        this.configurationModule = configurationModule;
        this.metaStorageService = metaStorageService;
        this.tblId = tableId;
        this.partitionMap = partMap;
    }

    /** {@inheritDoc} */
    public TableRow put(TableRow row) {
        try {
            String name = new String(metaStorageService.get(
                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                .value(), StandardCharsets.UTF_8);

            int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                .tables().get(name).partitions().value();

            return partitionMap.get(row.keyChunk().hashCode() % partitions).<TableRowResponse>run(new PutCommand(row)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to put some value [tblId={}, row={}]", tblId, row);
        }

        //TODO: Throw exception.
        return null;
    }

    /** {@inheritDoc} */
    public TableRow get(TableRow keyRow) {
        try {
            String name = new String(metaStorageService.get(
                new Key(INTERNAL_PREFIX + tblId.toString())).get()
                .value(), StandardCharsets.UTF_8);

            int partitions = configurationModule.configurationRegistry().getConfiguration(DistributedTableConfiguration.KEY)
                .tables().get(name).partitions().value();

            return partitionMap.get(keyRow.keyChunk().hashCode() % partitions).<TableRowResponse>run(new GetCommand(keyRow)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get some value [tblId={}, keyRow={}]", tblId, keyRow);
        }

        //TODO: Throw exception.
        return null;
    }
}
