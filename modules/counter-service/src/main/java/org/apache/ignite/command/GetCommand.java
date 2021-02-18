package org.apache.ignite.command;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.command.AbstractCommand;
import org.apache.ignite.raft.client.command.CustomCommand;
import org.apache.ignite.raft.client.command.ReadCommand;

public class GetCommand extends AbstractCommand implements CustomCommand, ReadCommand {

    public GetCommand(IgniteUuid id) {
        super(id);
    }
}
