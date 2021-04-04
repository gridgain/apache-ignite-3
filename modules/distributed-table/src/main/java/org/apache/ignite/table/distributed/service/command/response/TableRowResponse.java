package org.apache.ignite.table.distributed.service.command.response;

import org.apache.ignite.internal.table.TableRow;

public class TableRowResponse {
    private TableRow row;

    public TableRowResponse(TableRow row) {
        this.row = row;
    }

    public TableRow getValue() {
        return row;
    }
}
