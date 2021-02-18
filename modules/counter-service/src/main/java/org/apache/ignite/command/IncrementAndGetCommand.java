package org.apache.ignite.command;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.command.AbstractCommand;
import org.apache.ignite.raft.client.command.CustomCommand;
import org.apache.ignite.raft.client.command.WriteCommand;

public class IncrementAndGetCommand extends AbstractCommand implements CustomCommand, WriteCommand {

    private final long delta;

    public IncrementAndGetCommand(IgniteUuid id, long delta) {
        super(id);
        this.delta = delta;
    }

    /**
     * @return Key.
     */
    public long delta() {
        return delta;
    }
}
