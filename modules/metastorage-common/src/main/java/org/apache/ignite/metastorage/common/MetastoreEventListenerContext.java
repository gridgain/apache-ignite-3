package org.apache.ignite.metastorage.common;

public interface MetastoreEventListenerContext {
    void addEvent(MetastoreEvent event);
}
