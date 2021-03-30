package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

public interface Watcher {
    void register(@NotNull Watch watch);

    void notify(@NotNull Entry e);

    //TODO: implement
    void cancel(@NotNull Watch watch);
}

