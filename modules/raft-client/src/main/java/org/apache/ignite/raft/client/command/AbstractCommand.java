package org.apache.ignite.raft.client.command;

import org.apache.ignite.lang.IgniteUuid;

public abstract class AbstractCommand implements Command {
    private final IgniteUuid id;

    public AbstractCommand(IgniteUuid id) {
        this.id = id;
    }

    @Override public IgniteUuid id() {
        return id;
    }
}
