package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

public class Value {
    public static final byte[] TOMBSTONE = new byte[0];

    private final byte[] bytes;
    private final long updCntr;

    public Value(@NotNull byte[] bytes, long updCntr) {
        this.bytes = bytes;
        this.updCntr = updCntr;
    }

    public byte[] bytes() {
        return bytes;
    }

    public long updateCounter() {
        return updCntr;
    }

    boolean tombstone() {
        return bytes == TOMBSTONE;
    }
}
