package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public interface KeyValueStorage {

    long revision();

    @NotNull
    Entry put(byte[] key, byte[] value);

    @NotNull
    Entry get(byte[] key);

    @NotNull
    Entry get(byte[] key, long rev);

    @NotNull
    Entry remove(byte[] key);

    Iterator<Entry> iterate(byte[] key);

    //Iterator<Entry> iterate(long rev);

    void compact();
}
