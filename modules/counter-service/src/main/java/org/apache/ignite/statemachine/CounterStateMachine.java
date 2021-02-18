package org.apache.ignite.statemachine;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.command.GetCommand;
import org.apache.ignite.command.IncrementAndGetCommand;
import org.apache.ignite.raft.client.StateMachine;
import org.apache.ignite.raft.client.command.ReadCommand;
import org.apache.ignite.raft.client.command.WriteCommand;

public class CounterStateMachine implements StateMachine {
    private final AtomicLong data = new AtomicLong();

    @Override public Long applyWrite(WriteCommand cmd) {
        if (cmd instanceof IncrementAndGetCommand)
            return data.addAndGet(((IncrementAndGetCommand)cmd).delta());
        else
            throw new UnsupportedOperationException("Command=[" + cmd + "] is not supported.");
    }

    @Override public Long applyRead(ReadCommand cmd) {
        if (cmd instanceof GetCommand)
            return data.get();
        else
            throw new UnsupportedOperationException("Command=[" + cmd + "] is not supported.");
    }
}
