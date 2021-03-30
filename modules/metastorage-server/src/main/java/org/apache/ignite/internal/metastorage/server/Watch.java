package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Comparator;

public class Watch {
    private static final Comparator<byte[]> CMP = Arrays::compare;

    private static final long ANY_REVISION = -1;

    @Nullable
    private byte[] startKey;

    @Nullable
    private byte[] endKey;

    long rev = ANY_REVISION;

    public void startKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public void endKey(byte[] endKey) {
        this.endKey = endKey;
    }

    public void revision(long rev) {
        this.rev = rev;
    }

    public void notify(Entry e) {
        if (startKey != null && CMP.compare(e.key(), startKey) < 0)
            return;

        if (endKey != null && CMP.compare(e.key(), endKey) > 0)
            return;

        if (rev != ANY_REVISION && e.revision() <= rev)
            return;

        System.out.println("Entry: key=" + new String(e.key()) + ", value=" + new String(e.value()) + ", rev=" + e.revision());
    }
}
