package org.apache.ignite.internal;

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.apache.ignite.table.distributed.configuration.TableInit;
import org.apache.ignite.table.distributed.service.TableManager;
import org.apache.ignite.table.distributed.storage.TableStorageImpl;

/**
 *
 */
public class IgniteImpl implements Ignite {

    private ConfigurationModule configurationModule;

    private TableManager tableManager;

    LogWrapper log = new LogWrapper(IgniteImpl.class);

    public IgniteImpl() {
        //TODO: Need to watch all keys with table prefix.
//        metaStorageService.watch()

    }

    @Override public Table createTable(String name, Consumer<TableInit> tableInitChange) {
//        DistributedTableView distributedTableView = configurationModule.configurationRegistry()
//            .getConfiguration(DistributedTableConfiguration.KEY).value();

        configurationModule.configurationRegistry()
            .getConfiguration(DistributedTableConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        configurationModule.configurationRegistry()
//            .getConfiguration(DistributedTableConfiguration.KEY).change(change ->
//            change.changeTables((tblCfg) ->
//                tblCfg.create(tableConfiguration.name(), (t) -> {
//                    t.initBackups(tableConfiguration.backups());
//                    t.initName(tableConfiguration.name());
//                    t.initPartitions(tableConfiguration.partitions());
//                })));

        //TODO: Get it from table manager, when table is prepared to use.
        UUID tblId = UUID.randomUUID();

        return new TableImpl(new TableStorageImpl(tableManager, tblId));
    }
}
