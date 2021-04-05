package org.apache.ignite.internal;

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.apache.ignite.table.distributed.configuration.TableInit;
import org.apache.ignite.table.distributed.service.TableManagerImpl;
import org.apache.ignite.table.manager.TableManager;

/**
 * Ignite interface.
 */
public class IgniteImpl implements Ignite {
    /** Logger. */
    LogWrapper log = new LogWrapper(IgniteImpl.class);

    /** Configuration module. */
    private ConfigurationModule configurationModule;

    /** Table manager implementation. */
    private TableManagerImpl tableManagerImpl;

    /**
     * @param tableManagerImpl Table manager implementation.
     * @param configurationModule Configuration module.
     */
    public IgniteImpl(TableManagerImpl tableManagerImpl, ConfigurationModule configurationModule) {
        this.tableManagerImpl = tableManagerImpl;
        this.configurationModule = configurationModule;
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableInit> tableInitChange) {
        configurationModule.configurationRegistry()
            .getConfiguration(DistributedTableConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        this.createTable("tbl1", change -> {
//            change.initBackups(2);
//            change.initName("tbl1");
//            change.initPartitions(1_000);
//        });

        //TODO: Get it honestly using some future.
        Table tbl = null;

        while (tbl == null) {
            try {
                Thread.sleep(50);

                tbl = tableManagerImpl.table(name);
            }
            catch (InterruptedException e) {
                log.error("Waiting of creation of table was interrupted.", e);
            }
        }

        return tbl;
    }

    /** {@inheritDoc} */
    @Override public TableManager tableManager() {
        return tableManagerImpl;
    }
}
