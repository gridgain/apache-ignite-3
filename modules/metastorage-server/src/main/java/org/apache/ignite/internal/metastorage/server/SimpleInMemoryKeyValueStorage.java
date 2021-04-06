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

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * WARNING: Only for test purposes and only for non-distributed (one static instance) storage.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    private static final Comparator<byte[]> LEXICOGRAPHIC_COMPARATOR = Arrays::compare;

    private static final long LATEST_REV = -1;

    private final Watcher watcher;

    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

    private NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new TreeMap<>();

    private long rev;

    private long updCntr;

    private final Object mux = new Object();

    public SimpleInMemoryKeyValueStorage(Watcher watcher) {
        this.watcher = watcher;
    }

    @Override public long revision() {
        return rev;
    }

    @Override public long updateCounter() {
        return updCntr;
    }

    @Override public void put(byte[] key, byte[] value) {
        synchronized (mux) {
            doPut(key, value);
        }
    }

    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] bytes) {
        synchronized (mux) {
            long lastRev = doPut(key, bytes);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
    }

    @NotNull
    @Override public Entry get(byte[] key) {
        synchronized (mux) {
            return doGet(key, LATEST_REV, false);
        }
    }

    @NotNull
    @TestOnly
    @Override public Entry get(byte[] key, long rev) {
        synchronized (mux) {
            return doGet(key, rev, true);
        }
    }

    @Override
    public void remove(byte[] key) {
        synchronized (mux) {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return;

            doPut(key, TOMBSTONE);
        }
    }

    @NotNull
    @Override public Entry getAndRemove(byte[] key) {
        synchronized (mux) {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return e;

            return getAndPut(key, TOMBSTONE);
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

                        NavigableMap<byte[], Value> vals = revsIdx.get(rev);

                        if (vals == null || vals.isEmpty()) {
                            throw new IllegalStateException("vals == null || vals.isEmpty()");
                            //return new AbstractMap.SimpleImmutableEntry<>(key, null);
                        }

                        Value val = vals.get(key);

                        return val.tombstone() ?
                                Entry.tombstone(key, rev, val.updateCounter()) :
                                new Entry(key, val.bytes(), rev, val.updateCounter());
                    }
                }
            };
        }
    }

    @Override public void compact() {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx = new TreeMap<>();

            keysIdx.forEach((key, revs) -> compactForKey(key, revs, compactedKeysIdx, compactedRevsIdx));

            keysIdx = compactedKeysIdx;

            revsIdx = compactedRevsIdx;
        }
    }

    private void compactForKey(
            byte[] key,
            List<Long> revs,
            NavigableMap<byte[], List<Long>> compactedKeysIdx,
            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx
    ) {
        Long lrev = lastRevision(revs);

        NavigableMap<byte[], Value> kv = revsIdx.get(lrev);

        Value lastVal = kv.get(key);

        if (!lastVal.tombstone()) {
            compactedKeysIdx.put(key, listOf(lrev));

            NavigableMap<byte[], Value> compactedKv = compactedRevsIdx.computeIfAbsent(
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
    @NotNull
    private Entry doGet(byte[] key, long rev, boolean exactRev) {
        assert rev == LATEST_REV && !exactRev || rev > LATEST_REV :
                "Invalid arguments: [rev=" + rev + ", exactRev=" + exactRev + ']';

        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty())
            return Entry.empty(key);

        long lastRev;

        if (rev == LATEST_REV)
            lastRev = lastRevision(revs);
        else
            lastRev = exactRev ? rev : maxRevision(revs, rev);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1)
            return Entry.empty(key);

        return doGetValue(key, lastRev);
    }

    /**
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then
     * {@code -1} will be returned.
     *
     * @param revs Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Appropriate revision or {@code -1} if there is no such revision.
     */
    private static long maxRevision(List<Long> revs, long upperBoundRev) {
        int i = revs.size() - 1;

        for (; i >= 0 ; i--) {
            long rev = revs.get(i);

            if (rev <= upperBoundRev)
                return rev;
        }

        return -1;
    }

    @NotNull
    private Entry doGetValue(byte[] key, long lastRev) {
        if (lastRev == 0)
            return Entry.empty(key);

        NavigableMap<byte[], Value> lastRevVals = revsIdx.get(lastRev);

        if (lastRevVals == null || lastRevVals.isEmpty())
            return Entry.empty(key);

        Value lastVal = lastRevVals.get(key);

        if (lastVal.tombstone())
            return Entry.tombstone(key, lastRev, lastVal.updateCounter());

        return new Entry(key, lastVal.bytes() , lastRev, lastVal.updateCounter());
    }

    private long doPut(byte[] key, byte[] bytes) {
        long curRev = ++rev;

        long curUpdCntr = ++updCntr;

        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

        revs.add(curRev);

        // Update revsIdx.
        NavigableMap<byte[], Value> entries = new TreeMap<>(LEXICOGRAPHIC_COMPARATOR);

        Value val = new Value(bytes, curUpdCntr);

        entries.put(key, val);

        revsIdx.put(curRev, entries);

        return lastRev;
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
