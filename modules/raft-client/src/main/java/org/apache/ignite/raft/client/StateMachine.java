package org.apache.ignite.raft.client;

import org.apache.ignite.raft.client.command.ReadCommand;
import org.apache.ignite.raft.client.command.WriteCommand;

public interface StateMachine {
    <R, T extends WriteCommand> R applyWrite(T cmd);

    <R, S extends ReadCommand> R applyRead(S cmd);
}