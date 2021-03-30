package org.apache.ignite.internal.metastorage.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * WARNING: Only for test purposes and only for non-distributed (one static instance) storage.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    private static final Comparator<byte[]> LEXICOGRAPHIC_COMPARATOR = Arrays::compare;

    private static final byte[] TOMBSTONE = new byte[0];

    private static final long LATEST_REV = -1;

    private final Watcher watcher;

    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

    private NavigableMap<Long, NavigableMap<byte[], byte[]>> revsIdx = new TreeMap<>();

    private long grev = 0;

    private final Object mux = new Object();

    public SimpleInMemoryKeyValueStorage(Watcher watcher) {
        this.watcher = watcher;
    }

    @Override public long revision() {
        return grev;
    }

    @NotNull
    @Override public Entry put(byte[] key, byte[] val) {
        synchronized (mux) {
            long crev = ++grev;

            // Update keysIdx.
            List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

            long lrev = revs.isEmpty() ? 0 : lastRevision(revs);

            revs.add(crev);

            // Update revsIdx.
            NavigableMap<byte[], byte[]> entries = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

            entries.put(key, val);

            revsIdx.put(crev, entries);

            // Return previous value.
            if (lrev == 0)
                return Entry.empty(key);

            NavigableMap<byte[], byte[]> lastVal = revsIdx.get(lrev);

            Entry res = new Entry(key, lastVal.get(key), lrev);

            //TODO: notify watchers

            return res;
        }
    }

    @NotNull
    @Override public Entry get(byte[] key) {
        synchronized (mux) {
            return doGet(key, LATEST_REV);
        }
    }

    @NotNull
    @TestOnly
    @Override public Entry get(byte[] key, long rev) {
        synchronized (mux) {
            return doGet(key, rev);
        }
    }

    @NotNull
    @Override public Entry remove(byte[] key) {
        synchronized (mux) {
            Entry e = doGet(key, LATEST_REV);

            if (e.value() == null)
                return e;

            return put(key, TOMBSTONE);
        }
    }

    @Override public Iterator<Entry> iterate(byte[] keyFrom) {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> tailMap = keysIdx.tailMap(keyFrom, true);

            final Iterator<Map.Entry<byte[], List<Long>>> it = tailMap.entrySet().iterator();

            return new Iterator<>() {
                private Map.Entry<byte[], List<Long>> curr;
                private boolean hasNext;

                private void advance() {
                    if (it.hasNext()) {
                        Map.Entry<byte[], List<Long>> e = it.next();

                        byte[] key = e.getKey();

                        if (!isPrefix(keyFrom, key))
                            hasNext = false;
                        else {
                            curr = e;

                            hasNext = true;
                        }
                    } else
                        hasNext = false;
                }

                @Override
                public boolean hasNext() {
                    synchronized (mux) {
                        if (curr == null)
                            advance();

                        return hasNext;
                    }
                }

                @Override
                public Entry next() {
                    synchronized (mux) {
                        if (!hasNext())
                            throw new NoSuchElementException();

                        Map.Entry<byte[], List<Long>> e = curr;

                        curr = null;

                        byte[] key = e.getKey();

                        List<Long> revs = e.getValue();

                        long rev = revs == null || revs.isEmpty() ? 0 : lastRevision(revs);

                        if (rev == 0) {
                            throw new IllegalStateException("rev == 0");
                            //return new AbstractMap.SimpleImmutableEntry<>(key, null);
                        }

                        NavigableMap<byte[], byte[]> vals = revsIdx.get(rev);

                        if (vals == null || vals.isEmpty()) {
                            throw new IllegalStateException("vals == null || vals.isEmpty()");
                            //return new AbstractMap.SimpleImmutableEntry<>(key, null);
                        }

                        byte[] val = vals.get(key);

                        return val == TOMBSTONE ? Entry.tombstone(key, rev) : new Entry(key, val, rev);
                    }
                }
            };
        }
    }

    @Override public void compact() {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

            NavigableMap<Long, NavigableMap<byte[], byte[]>> compactedRevsIdx = new TreeMap<>();

            keysIdx.forEach((key, revs) -> compactForKey(key, revs, compactedKeysIdx, compactedRevsIdx));

            keysIdx = compactedKeysIdx;

            revsIdx = compactedRevsIdx;
        }
    }

    private void compactForKey(
            byte[] key,
            List<Long> revs,
            NavigableMap<byte[], List<Long>> compactedKeysIdx,
            NavigableMap<Long, NavigableMap<byte[], byte[]>> compactedRevsIdx
    ) {
        Long lrev = lastRevision(revs);

        NavigableMap<byte[], byte[]> kv = revsIdx.get(lrev);

        byte[] lastVal = kv.get(key);

        if (lastVal != TOMBSTONE) {
            compactedKeysIdx.put(key, listOf(lrev));

            NavigableMap<byte[], byte[]> compactedKv = compactedRevsIdx.computeIfAbsent(
                    lrev,
                    k -> new TreeMap<>(LEXICOGRAPHIC_COMPARATOR)
            );

            compactedKv.put(key, lastVal);
        }
    }

    /**
     * Returns entry for given key.
     *
     * @param key Key.
     * @param rev Revision.
     * @return Entry for given key.
     */
    @NotNull private Entry doGet(byte[] key, long rev) {
        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty())
            return Entry.empty(key);

        long lrev = rev == LATEST_REV ? lastRevision(revs) : rev;

        NavigableMap<byte[], byte[]> entries = revsIdx.get(lrev);

        if (entries == null || entries.isEmpty())
            return Entry.empty(key);

        byte[] val = entries.get(key);

        if (val == TOMBSTONE)
            return Entry.tombstone(key, lrev);

        return new Entry(key, val , lrev);
    }

    private static boolean isPrefix(byte[] pref, byte[] term) {
        if (pref.length > term.length)
            return false;

        for (int i = 0; i < pref.length - 1; i++) {
            if (pref[i] != term[i])
                return false;
        }

        return true;
    }

    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    private static  List<Long> listOf(long val) {
        List<Long> res = new ArrayList<>();

        res.add(val);

        return res;
    }

}
