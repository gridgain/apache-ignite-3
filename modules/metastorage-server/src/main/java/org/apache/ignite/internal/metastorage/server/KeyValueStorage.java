package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public interface KeyValueStorage {

    long revision();

    long updateCounter();

    @NotNull
    Entry get(byte[] key);

    @NotNull
    Entry get(byte[] key, long rev);

    void put(byte[] key, byte[] value);

    @NotNull
    Entry getAndPut(byte[] key, byte[] value);

    void remove(byte[] key);

    @NotNull
    Entry getAndRemove(byte[] key);

    Iterator<Entry> iterate(byte[] key);

    //Iterator<Entry> iterate(long rev);

    void compact();
}
