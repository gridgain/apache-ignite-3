package org.apache.ignite.table.distributed.service.command;

import org.apache.ignite.raft.client.WriteCommand;

public class PutCommand<K,V> implements WriteCommand {
    K key;
    V value;

    public PutCommand(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
