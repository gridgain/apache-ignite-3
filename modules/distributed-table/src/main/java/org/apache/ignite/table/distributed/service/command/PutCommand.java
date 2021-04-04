package org.apache.ignite.table.distributed.service.command;

import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.raft.client.WriteCommand;

public class PutCommand implements WriteCommand {
    TableRow row;

    public PutCommand(TableRow row) {
        this.row = row;
    }
}
