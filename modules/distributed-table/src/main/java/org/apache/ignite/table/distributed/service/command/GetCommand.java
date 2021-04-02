package org.apache.ignite.table.distributed.service.command;

import org.apache.ignite.raft.client.ReadCommand;

public class GetCommand<K> implements ReadCommand {
    K key;

    public GetCommand(K key) {
        this.key = key;
    }
}
