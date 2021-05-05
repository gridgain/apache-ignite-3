package org.apache.ignite.metastorage.common;

import java.util.List;

public interface MetastoreEventListener {
    /**
     * @param evtMarker Event marker for identifying the change.
     * @param entries New entries.
     * @param oldEntries Old entries.
     * @param revision The revision
     * @param ctx Listener context for event propagation.
     * @return {@code True} to stop listening and remove this listener.
     */
    boolean onUpdate(int evtMarker, List<Entry> entries, List<Entry> oldEntries, long revision, MetastoreEventListenerContext ctx);
}
