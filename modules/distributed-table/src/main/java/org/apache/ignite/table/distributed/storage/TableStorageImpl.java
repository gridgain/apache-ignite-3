package org.apache.ignite.table.distributed.storage;

import java.util.UUID;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.table.distributed.service.TableManager;

/**
 * Storage of table rows.
 */
public class TableStorageImpl implements TableStorage {

    /** Table manager. */
    private TableManager tableManager;

    /** Table id. */
    private UUID tblId;

    /**
     * @param tableManager Table manager.
     * @param tableId Table id.
     */
    public TableStorageImpl(TableManager tableManager, UUID tableId) {
        this.tableManager = tableManager;
        this.tblId = tableId;
    }

    /** {@inheritDoc} */
    @Override public TableRow get(TableRow keyRow) {
        return tableManager.get(tblId, keyRow);
    }

    /** {@inheritDoc} */
    @Override public TableRow put(TableRow row) {
        return tableManager.put(tblId, row);
    }
}
