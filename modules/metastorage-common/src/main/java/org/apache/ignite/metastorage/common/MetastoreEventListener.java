package org.apache.ignite.metastorage.common;

import java.util.List;

public interface MetastoreEventListener {
    boolean onUpdate(int evtMarker, List<Entry> entries, List<Entry> oldEntries, long revision, MetastoreEventListenerContext ctx);
}
