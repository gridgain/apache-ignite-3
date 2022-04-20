package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 */
public class TupleStoreTest {
    @Test
    public void testInsertUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", true)));

        // Start txn 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        store.commit(head, txId1, Timestamp.nextVersion());

        // Start txn 2.
        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        assertThrows(IllegalArgumentException.class, () -> store.insert(t2, Timestamp.nextUUID()));
    }

    @Test
    public void testInsertNonUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", false)));

        // Start txn 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2.
        Tuple t2 = Tuple.create().set("name", "name2").set("email", "id1@some.org");
        UUID txId2 = Timestamp.nextUUID();
        VersionChain<Tuple> head2 = store.insert(t2, txId2);
        assertNotNull(head2);
        Timestamp commitTs2 = Timestamp.nextVersion();
        store.commit(head2, txId2, commitTs2);

        // Validate.
        validate(store, "id1@some.org", head, head2, commitTs2);
        validate(store, "id1@some.org", head, head2, null);
    }

    @Test
    public void testInsertInsertMergeUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", true)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: insert row 2.
        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id2@some.org");
        UUID txId2 = Timestamp.nextUUID();
        VersionChain<Tuple> head2 = store.insert(t2, txId2);
        assertNotNull(head);
        Timestamp commitTs2 = Timestamp.nextVersion();
        store.commit(head2, txId2, commitTs2);

        // Start txn 3: update row 1 by changing email to existing value.
        UUID txId3 = Timestamp.nextUUID();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, null);
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");

        assertThrows(IllegalArgumentException.class, () -> store.merge(head, oldRow, newRow, txId3));
    }

    @Test
    public void testInsertInsertMergeNonUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", false)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: insert row 2.
        Tuple t2 = Tuple.create().set("name", "name1").set("email", "id2@some.org");
        UUID txId2 = Timestamp.nextUUID();
        VersionChain<Tuple> head2 = store.insert(t2, txId2);
        assertNotNull(head);
        Timestamp commitTs2 = Timestamp.nextVersion();
        store.commit(head2, txId2, commitTs2);

        // Start txn 3: update row 1 by changing email to existing value.
        UUID txId3 = Timestamp.nextUUID();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, null);
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
        store.merge(head, oldRow, newRow, txId3);
        Timestamp commitTs3 = Timestamp.nextVersion();
        store.commit(head, txId3, commitTs3);

        validate(store, "id2@some.org", head, head2, commitTs3);
    }

    @Test
    public void testInsertMergeInsertUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", true)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: update row 1 by changing email.
        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
        UUID txId2 = Timestamp.nextUUID();
        Timestamp commitTs2 = Timestamp.nextVersion();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
        store.merge(head, oldRow, newRow, txId2);
        store.commit(head, txId2, commitTs2);

        iter = store.scan("email", Tuple.create().set("email", "id2@some.org"));
        assertTrue(iter.hasNext());
        assertEquals("id2@some.org", store.resolve(head, null, null).valueOrDefault("email", null));

        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));
        assertTrue(iter.hasNext());
        assertEquals("id1@some.org", store.resolve(head, commitTs, null).valueOrDefault("email", null));

        // Start txn 3: insert row 2 with existing email.
        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id2@some.org");
        UUID txId3 = Timestamp.nextUUID();
        assertThrows(IllegalArgumentException.class, () -> store.insert(t3, txId3));
    }

    @Test
    public void testInsertMergeInsertUnique2() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", true)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create().set("name", "name1").set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: update row 1 by changing email. This will produce new row version and two index records.
        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
        UUID txId2 = Timestamp.nextUUID();
        Timestamp commitTs2 = Timestamp.nextVersion();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
        store.merge(head, oldRow, newRow, txId2);
        store.commit(head, txId2, commitTs2);

        // Start txn 3: insert row 2 with existing email, but in earlier history.
        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id1@some.org");
        UUID txId3 = Timestamp.nextUUID();
        VersionChain<Tuple> head3 = store.insert(t3, txId3);
        assertNotNull(head3);
        Timestamp commitTs3 = Timestamp.nextVersion();
        store.commit(head3, txId3, commitTs3);

        // Validate.
        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));

        Set<VersionChain<Tuple>> rowIds = new HashSet<>();

        while (iter.hasNext()) {
            VersionChain<Tuple> next = iter.next();
            rowIds.add(next);
        }

        assertEquals(2, rowIds.size());
        assertTrue(rowIds.contains(head));
        assertTrue(rowIds.contains(head3));

        Timestamp readTs = commitTs3;  // Can use null for timestamp.

        assertNull(store.resolve(head, readTs, pred));
        assertEquals("id1@some.org", store.resolve(head3, readTs, pred).valueOrDefault("email", null));
    }

    @Test
    public void testInsertMergeInsertNonUnique() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", false)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create();
        t1.set("name", "name1");
        t1.set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: update row 1 by changing email.
        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
        UUID txId2 = Timestamp.nextUUID();
        Timestamp commitTs2 = Timestamp.nextVersion();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
        store.merge(head, oldRow, newRow, txId2);
        store.commit(head, txId2, commitTs2);

        // Start txn 3.
        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id2@some.org");
        UUID txId3 = Timestamp.nextUUID();
        VersionChain<Tuple> head3 = store.insert(t3, txId3);
        assertNotNull(head3);
        Timestamp commitTs3 = Timestamp.nextVersion();
        store.commit(head3, txId3, commitTs3);

        iter = store.scan("email", Tuple.create().set("email", "id2@some.org"));

        Set<VersionChain<Tuple>> rowIds = new HashSet<>();

        while (iter.hasNext()) {
            VersionChain<Tuple> next = iter.next();
            rowIds.add(next);
        }

        assertEquals(2, rowIds.size());
        assertTrue(rowIds.contains(head));
        assertTrue(rowIds.contains(head3));

        Timestamp readTs = commitTs3;  // Can use null for timestamp.

        Predicate<Tuple> pred2 = t -> "id2@some.org".equals(t.valueOrDefault("email", null));
        assertEquals("id2@some.org", store.resolve(head, readTs, pred2).valueOrDefault("email", null));
        assertEquals("id2@some.org", store.resolve(head3, readTs, pred2).valueOrDefault("email", null));
    }

    @Test
    public void testInsertMergeInsertNonUnique2() {
        TupleStore store = new TupleStore(Map.of("email", new TupleHashIndexImpl("email", false)));

        // Start txn 1: insert row 1.
        Tuple t1 = Tuple.create();
        t1.set("name", "name1");
        t1.set("email", "id1@some.org");
        UUID txId1 = Timestamp.nextUUID();
        VersionChain<Tuple> head = store.insert(t1, txId1);
        assertNotNull(head);
        Timestamp commitTs = Timestamp.nextVersion();
        store.commit(head, txId1, commitTs);

        // Start txn 2: update row 1 by changing email.
        Predicate<Tuple> pred = t -> "id1@some.org".equals(t.valueOrDefault("email", null));
        UUID txId2 = Timestamp.nextUUID();
        Timestamp commitTs2 = Timestamp.nextVersion();
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", "id1@some.org")); // TODO acquire locks on index.
        assertTrue(iter.hasNext());
        assertTrue(iter.next() == head); // Chain head is immutable.
        assertFalse(iter.hasNext());
        Tuple oldRow = store.resolve(head, null, pred); // TODO acquire lock on rowId.
        assertNotNull(oldRow);
        Tuple newRow = Tuple.create(oldRow).set("email", "id2@some.org");
        store.merge(head, oldRow, newRow, txId2);
        store.commit(head, txId2, commitTs2);

        // Start txn 3.
        Tuple t3 = Tuple.create().set("name", "name3").set("email", "id1@some.org");
        UUID txId3 = Timestamp.nextUUID();
        VersionChain<Tuple> head3 = store.insert(t3, txId3);
        assertNotNull(head3);
        Timestamp commitTs3 = Timestamp.nextVersion();
        store.commit(head3, txId3, commitTs3);

        iter = store.scan("email", Tuple.create().set("email", "id1@some.org"));

        Set<VersionChain<Tuple>> rowIds = new HashSet<>();

        while (iter.hasNext()) {
            VersionChain<Tuple> next = iter.next();
            rowIds.add(next);
        }

        assertEquals(2, rowIds.size());
        assertTrue(rowIds.contains(head));
        assertTrue(rowIds.contains(head3));

        Timestamp readTs = commitTs3;  // Can use null for timestamp.

        assertNull(store.resolve(head, readTs, pred));
        assertEquals("id1@some.org", store.resolve(head3, readTs, pred).valueOrDefault("email", null));
    }

    private void validate(TupleStore store, String email, VersionChain<Tuple> head, VersionChain<Tuple> head2, Timestamp commitTs) {
        Iterator<VersionChain<Tuple>> iter = store.scan("email", Tuple.create().set("email", email));

        Set<VersionChain<Tuple>> rowIds = new HashSet<>();

        while (iter.hasNext()) {
            VersionChain<Tuple> next = iter.next();
            rowIds.add(next);
        }

        assertEquals(2, rowIds.size());
        assertTrue(rowIds.contains(head));
        assertTrue(rowIds.contains(head2));

        Predicate<Tuple> pred = t -> email.equals(t.valueOrDefault("email", null));
        assertEquals(email, store.resolve(head, commitTs, pred).valueOrDefault("email", null));
        assertEquals(email, store.resolve(head2, commitTs, pred).valueOrDefault("email", null));
    }
}

interface HashIndex<T> {
    Iterator<T> scan(Tuple key);

    boolean insert(Tuple key, T rowId);

    boolean delete(Tuple key, T rowId);
}

/**
 * TODO multicolumn support.
 */
class TupleHashIndexImpl implements HashIndex<VersionChain<Tuple>> {
    private final boolean unique;
    private final String column;
    private ConcurrentMap<Tuple, Set<VersionChain<Tuple>>> data = new ConcurrentHashMap<>();

    TupleHashIndexImpl(String column, boolean unique) {
        this.column = column;
        this.unique = unique;
    }

    @Override
    public Iterator<VersionChain<Tuple>> scan(Tuple key) {
        Set<VersionChain<Tuple>> vals = data.get(key);

        if (vals == null)
            vals = Collections.emptySet();

        return vals.iterator();
    }

    @Override
    public boolean insert(Tuple key, VersionChain<Tuple> rowId) {
        // Must validate uniquess before insert.
        // TODO code generation ?
        if (unique) {
            // TODO the simplest solution is to iterate until a duplicate is found. this can be optimized.
            Iterator<VersionChain<Tuple>> iter = scan(key);

            while (iter.hasNext()) {
                VersionChain<Tuple> head = iter.next();

                // Look for latest committed value
                if (head.resolve(null, t -> t.valueOrDefault(column, null).equals(key.valueOrDefault(column, null))) != null)
                    return false;
            }
        }

        data.computeIfAbsent(key, k -> new HashSet<>()).add(rowId);

        return true;
    }

    @Override
    public boolean delete(Tuple key, VersionChain<Tuple> rowId) {
        return false;
    }
}

interface MVStore<ROWID, T> { // TODO generalize
    ROWID insert(Tuple row, UUID txId);

    boolean merge(ROWID rowId, T oldRow, T newRow, UUID txId);

    Iterator<ROWID> scan(String idxName, T key);

    T resolve(ROWID rowId, @Nullable Timestamp ts, Predicate<T> filter);

    void commit(ROWID inserted, UUID txId, Timestamp commitTs);

    // TODO abort
}

class TupleStore implements MVStore<VersionChain<Tuple>, Tuple> {
    private final Map<String, HashIndex<VersionChain<Tuple>>> hashIndexes;

    TupleStore(Map<String, HashIndex<VersionChain<Tuple>>> hashIndexes) {
        this.hashIndexes = hashIndexes;
    }

    @Override
    public VersionChain<Tuple> insert(Tuple t, UUID txId) {
        VersionChain<Tuple> head = new VersionChain<>(txId, null, null, t, null);

        // Index required fields.
        for (Entry<String, HashIndex<VersionChain<Tuple>>> entry : hashIndexes.entrySet()) {
            Object t1 = t.valueOrDefault(entry.getKey(), null); // Extract index field from the tuple.
            // TODO check t1 is not null.
            Tuple key = Tuple.create(Map.of(entry.getKey(), t1));

            if (!entry.getValue().insert(key, head))
                throw new IllegalArgumentException("Failed to insert index value [col=" + entry.getKey() + ']'); // TODO revert changes.
        }

        return head;
    }

    @Override
    public boolean merge(VersionChain<Tuple> rowId, Tuple oldRow, Tuple newRow, UUID txId) {
        rowId.addWrite(newRow, txId); // Head is immutable.

        assert oldRow != null;

        // Index required fields.
        for (Entry<String, HashIndex<VersionChain<Tuple>>> entry : hashIndexes.entrySet()) {
            Object oldVal = oldRow.valueOrDefault(entry.getKey(), null);
            Object newVal = newRow.valueOrDefault(entry.getKey(), null);

            if (!Objects.equals(oldVal, newVal)) { // Reindex only changed values.
                Tuple key = Tuple.create(Map.of(entry.getKey(), newVal));

                if (!entry.getValue().insert(key, rowId))
                    throw new IllegalArgumentException("Failed to insert index value [col=" + entry.getKey() + ']'); // TODO revert changes.

                // Do not remove old value on reindexing because it's multiversioned.
            }
        }

        return true;
    }

    @Override
    public Iterator<VersionChain<Tuple>> scan(String idxName, Tuple key) {
        HashIndex<VersionChain<Tuple>> idx = hashIndexes.get(idxName);

        return idx.scan(key);
    }

    @Nullable
    @Override
    public Tuple resolve(VersionChain<Tuple> rowId, @Nullable Timestamp ts, @Nullable Predicate<Tuple> filter) {
        return rowId.resolve(ts, filter);
    }

    @Override
    public void commit(VersionChain<Tuple> inserted, UUID txId, Timestamp commitTs) {
        inserted.commitWrite(commitTs); // TOD txId.
    }
}

class VersionChain<T> {
    @Nullable Timestamp begin;
    @Nullable Timestamp end;
    T value;
    @Nullable UUID txId; // Lock holder for uncommitted version.
    @Nullable VersionChain<T> next;

    VersionChain(UUID txId, @Nullable Timestamp begin, @Nullable Timestamp end, @Nullable T value, @Nullable VersionChain<T> next) {
        this.txId = txId;
        this.begin = begin;
        this.end = end;
        this.value = value;
        this.next = next;
    }

    public @Nullable Timestamp getBegin() {
        return begin;
    }

    public void setBegin(@Nullable Timestamp timestamp) {
        this.begin = timestamp;
    }

    public @Nullable Timestamp getEnd() {
        return end;
    }

    public void setEnd(@Nullable Timestamp end) {
        this.end = end;
    }

    public @Nullable T getValue() {
        return value;
    }

    public void setValue(@Nullable T value) {
        this.value = value;
    }

    public @Nullable VersionChain<T> getNext() {
        return next;
    }

    public void setNext(@Nullable VersionChain<T> next) {
        this.next = next;
    }

    public UUID getTxId() {
        return txId;
    }

    public void setTxId(@Nullable UUID txId) {
        this.txId = txId;
    }

    @Override
    public String toString() {
        return S.toString(VersionChain.class, this);
    }

    @Nullable T resolve(@Nullable Timestamp timestamp, @Nullable Predicate<T> filter) {
        if (timestamp == null) {
            // TODO lockManager.tryAcquireShared(key, txId);

            return filter == null ? value : filter.test(value) ? value : null;
        }

        VersionChain<T> cur = this;

        do {
            if (cur.begin != null && timestamp.compareTo(cur.begin) >= 0 && (cur.end == null || timestamp.compareTo(cur.end) < 0)) {
                return filter == null ? cur.value : filter.test(cur.value) ? cur.value : null;
            }

            cur = cur.next;
        } while(cur != null);

        return null;
    }

    /**
     * @param head The chain head.
     * @param val The value or null for tombstone.
     * @param txId Txn id.
     */
    public void addWrite(@Nullable T val, UUID txId) {
        assert begin != null : "Only one write intent is allowed: " + this;

        // Re-link.
        VersionChain<T> next0 = new VersionChain<>(txId, begin, end, value, next);
        setTxId(txId);
        setBegin(null);
        setEnd(null);
        setValue(val);
        setNext(next0);
    }

    public void printVersionChain() {
        System.out.println("head=" + (long)(hashCode() & (-1)));
        System.out.println("begin=" + begin + " end=" + end + ", value=" + value);

        VersionChain<T> next = this.next;

        while(next != null) {
            System.out.println("begin=" + next.begin + " end=" + next.end + ", value=" + next.value);

            next = next.next;
        }
    }

    public void commitWrite(Timestamp timestamp) {
        Objects.requireNonNull(timestamp);

        setBegin(timestamp);
        setTxId(null);

        if (next != null)
            next.end = timestamp;

        // TODO lockManager.tryRelease(key, txId);
    }
}