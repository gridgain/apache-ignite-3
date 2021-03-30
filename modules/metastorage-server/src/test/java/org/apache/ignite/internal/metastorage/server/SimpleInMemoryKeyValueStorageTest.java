package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleInMemoryKeyValueStorageTest {
    private KeyValueStorage storage;

    @BeforeEach
    public void setUp() {
        storage = new SimpleInMemoryKeyValueStorage(new NoOpWatcher());
    }

    @Test
    void putGetRemoveCompact() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 1);
        byte[] val1_3 = kv(1, 3);

        byte[] key2 = k(2);
        byte[] val2_2 = kv(2, 2);

        assertEquals(0, storage.revision());

        // Previous entry is empty.
        Entry emptyEntry = storage.put(key1, val1_1);

        assertEquals(1, storage.revision());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 1.
        Entry e1_1 = storage.get(key1);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(1, storage.revision());

        // Previous entry is empty.
        emptyEntry = storage.put(key2, val2_2);

        assertEquals(2, storage.revision());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 2.
        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2_2, e2.value());
        assertEquals(2, e2.revision());
        assertEquals(2, storage.revision());

        // Previous entry is not empty.
        e1_1 = storage.put(key1, val1_3);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(3, storage.revision());

        // Entry with rev == 3.
        Entry e1_3 = storage.get(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(3, storage.revision());

        // Remove existing entry.
        Entry e2_2 = storage.remove(key2);

        assertFalse(e2_2.empty());
        assertFalse(e2_2.tombstone());
        assertArrayEquals(key2, e2_2.key());
        assertArrayEquals(val2_2, e2_2.value());
        assertEquals(2, e2_2.revision());
        assertEquals(4, storage.revision()); // Storage revision is changed.

        // Remove already removed entry.
        Entry tombstoneEntry = storage.remove(key2);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(4, storage.revision()); // Storage revision is not changed.

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(4, storage.revision());
        assertTrue(storage.remove(key2).empty());
        assertTrue(storage.get(key2).empty());

        // Remove existing entry.
        e1_3 = storage.remove(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(5, storage.revision()); // Storage revision is changed.

        // Remove already removed entry.
        tombstoneEntry = storage.remove(key1);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(5, storage.revision()); // // Storage revision is not changed.

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(5, storage.revision());
        assertTrue(storage.remove(key1).empty());
        assertTrue(storage.get(key1).empty());
    }

    @Test
    void compact() {
        assertEquals(0, storage.revision());

        // Compact empty.
        storage.compact();

        assertEquals(0, storage.revision());

        // Compact non-empty.
        fill(storage, 1, 1);

        assertEquals(1, storage.revision());

        fill(storage, 2, 2);

        assertEquals(3, storage.revision());

        fill(storage, 3, 3);

        assertEquals(6, storage.revision());

        storage.remove(k(3));

        assertEquals(7, storage.revision());
        assertTrue(storage.get(k(3)).tombstone());

        storage.compact();

        assertEquals(7, storage.revision());

        Entry e1 = storage.get(k(1));

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertArrayEquals(k(1), e1.key());
        assertArrayEquals(kv(1,1), e1.value());
        assertEquals(1, e1.revision());

        Entry e2 = storage.get(k(2));

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(k(2), e2.key());
        assertArrayEquals(kv(2,2), e2.value());
        assertTrue(storage.get(k(2), 2).empty());
        assertEquals(3, e2.revision());

        Entry e3 = storage.get(k(3));

        assertTrue(e3.empty());
        assertTrue(storage.get(k(3), 5).empty());
        assertTrue(storage.get(k(3), 6).empty());
        assertTrue(storage.get(k(3), 7).empty());
    }

    @Test
    void iterate() {
        TreeMap<String, String> expFooMap = new TreeMap<>();
        TreeMap<String, String> expKeyMap = new TreeMap<>();
        TreeMap<String, String> expZooMap = new TreeMap<>();

        fill("foo", storage, expFooMap);
        fill("key", storage, expKeyMap);
        fill("zoo", storage, expZooMap);

        assertEquals(300, storage.revision());

        assertIterate("key", storage, expKeyMap);
        assertIterate("zoo", storage, expZooMap);
        assertIterate("foo", storage, expFooMap);
    }

    private void assertIterate(String pref,  KeyValueStorage storage, TreeMap<String, String> expMap) {
        Iterator<Entry> it = storage.iterate((pref + "_").getBytes());
        Iterator<Map.Entry<String, String>> expIt = expMap.entrySet().iterator();

        // Order.
        while (it.hasNext()) {
            Entry entry = it.next();
            Map.Entry<String, String> expEntry = expIt.next();

            assertEquals(expEntry.getKey(), new String(entry.key()));
            assertEquals(expEntry.getValue(), new String(entry.value()));
        }

        // Range boundaries.
        it = storage.iterate((pref + '_').getBytes());

        while (it.hasNext()) {
            Entry entry = it.next();

            assertTrue(expMap.containsKey(new String(entry.key())));
        }
    }

    private static void fill(String pref, KeyValueStorage storage, TreeMap<String, String> expMap) {
        for (int i = 0; i < 100; i++) {
            String keyStr = pref + '_' + i;

            String valStr = "val_" + i;

            expMap.put(keyStr, valStr);

            byte[] key = keyStr.getBytes();

            byte[] val = valStr.getBytes();

            storage.put(key, val);
        }
    }

    private static void fill(KeyValueStorage storage, int keySuffix, int num) {
        for (int i = 0; i < num; i++)
            storage.put(k(keySuffix), kv(keySuffix, i + 1));
    }

    private static byte[] k(int k) {
        return ("key" + k).getBytes();
    }

    private static byte[] kv(int k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes();
    }

    private static class NoOpWatcher implements Watcher {
        @Override public void register(@NotNull Watch watch) {
            // No-op.
        }

        @Override public void notify(@NotNull Entry e) {
            // No-op.
        }

        @Override public void cancel(@NotNull Watch watch) {
            // No-op.
        }
    }
}