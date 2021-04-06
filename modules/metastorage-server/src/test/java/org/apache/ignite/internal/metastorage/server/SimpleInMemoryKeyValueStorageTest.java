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
    public void put() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());

        storage.put(key, val);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void getAndPut() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        Entry e = storage.getAndPut(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());
        assertTrue(e.empty());
        assertFalse(e.tombstone());
        assertEquals(0, e.revision());
        assertEquals(0, e.updateCounter());

        e = storage.getAndPut(key, val);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());
    }

    @Test
    public void remove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        // Remove non-existent entry.
        storage.remove(key);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Remove existent entry.
        storage.remove(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());

        // Remove already removed entry (tombstone can't be removed).
        storage.remove(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void getAndRemove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        // Remove non-existent entry.
        Entry e = storage.getAndRemove(key);

        assertTrue(e.empty());
        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        storage.put(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Remove existent entry.
        e = storage.getAndRemove(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());

        // Remove already removed entry (tombstone can't be removed).
        e = storage.getAndRemove(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void getAfterRemove() {
        byte[] key = k(1);
        byte[] val = kv(1, 1);

        storage.getAndPut(key, val);

        storage.getAndRemove(key);

        Entry e = storage.get(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertEquals(2, e.revision());
        assertTrue(e.tombstone());
    }

    @Test
    public void getAndPutAfterRemove() {
        byte[] key = k(1);

        byte[] val = kv(1, 1);

        storage.getAndPut(key, val);

        storage.getAndRemove(key);

        Entry e = storage.getAndPut(key, val);

        assertEquals(3, storage.revision());

        assertEquals(3, storage.updateCounter());

        assertEquals(2, e.revision());

        assertTrue(e.tombstone());
    }

    @Test
    public void putGetRemoveCompact() {
        byte[] key1 = k(1);
        byte[] val1_1 = kv(1, 1);
        byte[] val1_3 = kv(1, 3);

        byte[] key2 = k(2);
        byte[] val2_2 = kv(2, 2);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Previous entry is empty.
        Entry emptyEntry = storage.getAndPut(key1, val1_1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 1.
        Entry e1_1 = storage.get(key1);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(1, e1_1.updateCounter());
        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Previous entry is empty.
        emptyEntry = storage.getAndPut(key2, val2_2);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());
        assertTrue(emptyEntry.empty());

        // Entry with rev == 2.
        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2_2, e2.value());
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        // Previous entry is not empty.
        e1_1 = storage.getAndPut(key1, val1_3);

        assertFalse(e1_1.empty());
        assertFalse(e1_1.tombstone());
        assertArrayEquals(key1, e1_1.key());
        assertArrayEquals(val1_1, e1_1.value());
        assertEquals(1, e1_1.revision());
        assertEquals(1, e1_1.updateCounter());
        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Entry with rev == 3.
        Entry e1_3 = storage.get(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(3, e1_3.updateCounter());
        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Remove existing entry.
        Entry e2_2 = storage.getAndRemove(key2);

        assertFalse(e2_2.empty());
        assertFalse(e2_2.tombstone());
        assertArrayEquals(key2, e2_2.key());
        assertArrayEquals(val2_2, e2_2.value());
        assertEquals(2, e2_2.revision());
        assertEquals(2, e2_2.updateCounter());
        assertEquals(4, storage.revision()); // Storage revision is changed.
        assertEquals(4, storage.updateCounter());

        // Remove already removed entry.
        Entry tombstoneEntry = storage.getAndRemove(key2);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(4, storage.revision()); // Storage revision is not changed.
        assertEquals(4, storage.updateCounter());

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(4, storage.revision());
        assertEquals(4, storage.updateCounter());
        assertTrue(storage.getAndRemove(key2).empty());
        assertTrue(storage.get(key2).empty());

        // Remove existing entry.
        e1_3 = storage.getAndRemove(key1);

        assertFalse(e1_3.empty());
        assertFalse(e1_3.tombstone());
        assertArrayEquals(key1, e1_3.key());
        assertArrayEquals(val1_3, e1_3.value());
        assertEquals(3, e1_3.revision());
        assertEquals(3, e1_3.updateCounter());
        assertEquals(5, storage.revision()); // Storage revision is changed.
        assertEquals(5, storage.updateCounter());

        // Remove already removed entry.
        tombstoneEntry = storage.getAndRemove(key1);

        assertFalse(tombstoneEntry.empty());
        assertTrue(tombstoneEntry.tombstone());
        assertEquals(5, storage.revision()); // // Storage revision is not changed.
        assertEquals(5, storage.updateCounter());

        // Compact and check that tombstones are removed.
        storage.compact();

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());
        assertTrue(storage.getAndRemove(key1).empty());
        assertTrue(storage.get(key1).empty());
    }

    @Test
    public void compact() {
        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Compact empty.
        storage.compact();

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Compact non-empty.
        fill(storage, 1, 1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        fill(storage, 2, 2);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        fill(storage, 3, 3);

        assertEquals(6, storage.revision());
        assertEquals(6, storage.updateCounter());

        storage.getAndRemove(k(3));

        assertEquals(7, storage.revision());
        assertEquals(7, storage.updateCounter());
        assertTrue(storage.get(k(3)).tombstone());

        storage.compact();

        assertEquals(7, storage.revision());
        assertEquals(7, storage.updateCounter());

        Entry e1 = storage.get(k(1));

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertArrayEquals(k(1), e1.key());
        assertArrayEquals(kv(1,1), e1.value());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());

        Entry e2 = storage.get(k(2));

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertArrayEquals(k(2), e2.key());
        assertArrayEquals(kv(2,2), e2.value());
        assertTrue(storage.get(k(2), 2).empty());
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());

        Entry e3 = storage.get(k(3));

        assertTrue(e3.empty());
        assertTrue(storage.get(k(3), 5).empty());
        assertTrue(storage.get(k(3), 6).empty());
        assertTrue(storage.get(k(3), 7).empty());
    }

    @Test
    public void iterate() {
        TreeMap<String, String> expFooMap = new TreeMap<>();
        TreeMap<String, String> expKeyMap = new TreeMap<>();
        TreeMap<String, String> expZooMap = new TreeMap<>();

        fill("foo", storage, expFooMap);
        fill("key", storage, expKeyMap);
        fill("zoo", storage, expZooMap);

        assertEquals(300, storage.revision());
        assertEquals(300, storage.updateCounter());

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

            storage.getAndPut(key, val);
        }
    }

    private static void fill(KeyValueStorage storage, int keySuffix, int num) {
        for (int i = 0; i < num; i++)
            storage.getAndPut(k(keySuffix), kv(keySuffix, i + 1));
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