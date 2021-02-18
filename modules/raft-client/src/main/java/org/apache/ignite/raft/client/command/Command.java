package org.apache.ignite.raft.client.command;

import org.apache.ignite.lang.IgniteUuid;

public interface Command {
    IgniteUuid id();
}
