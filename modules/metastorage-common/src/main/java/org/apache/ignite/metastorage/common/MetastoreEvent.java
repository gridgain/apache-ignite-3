package org.apache.ignite.metastorage.common;

import java.io.Serializable;

public abstract class MetastoreEvent implements Serializable {
    private final long revision;

    public MetastoreEvent(long revision) {
        this.revision = revision;
    }

    public long getRevision() {
        return revision;
    }
}
