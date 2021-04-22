package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface KeyValueStorage {

    long revision();

    long updateCounter();

    @NotNull
    Entry get(byte[] key);

    @NotNull
    Entry get(byte[] key, long rev);

    @NotNull
    Collection<Entry> getAll(List<byte[]> keys);

    @NotNull
    Collection<Entry> getAll(List<byte[]> keys, long revUpperBound);

    void put(byte[] key, byte[] value);

    @NotNull
    Entry getAndPut(byte[] key, byte[] value);

    void putAll(List<byte[]> keys, List<byte[]> values);

    @NotNull
    Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values);

    void remove(byte[] key);

    @NotNull
    Entry getAndRemove(byte[] key);

    void removeAll(List<byte[]> key);

    @NotNull
    Collection<Entry> getAndRemoveAll(List<byte[]> keys);

    Iterator<Entry> range(byte[] keyFrom, byte[] keyTo);

    Iterator<Entry> iterate(byte[] key);

    //Iterator<Entry> iterate(long rev);

    void compact();
}
