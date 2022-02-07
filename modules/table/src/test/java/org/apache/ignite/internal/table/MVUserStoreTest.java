package org.apache.ignite.internal.table;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests a multi versioned user store.
 * <p>A user object has two secondary indexes: by email and by department id.
 * <p>Implementation details:
 * <ul>
 *     <li>A primary index stores a map: PrimaryKey -> version chain.</li>
 *     <li>In the HEAD of each chain either current(last committed) version or uncommited version. Older versions are deeper in the chain</li>
 *     <li>A secondary index store a map: SecondaryKey -> PrimaryKey. Then the index is scanned, a corresponding version chain is
 *     traversed until corresponding value is not found.</li>
 * </ul>
 */
public class MVUserStoreTest {
    private PrimaryIndex<Integer, User> primIdx = new PrimaryIndex<>();

    private SecondaryIndex<String, Integer, User> emailIdx = new SecondaryIndex<>(primIdx, (email, user) -> email.equals(user.getEmail()));

    private SecondaryIndex<Integer, Integer, User> deptIdx = new SecondaryIndex<>(primIdx, (id, user) -> id.equals(user.getDepartmentId()));

    @Test
    public void testPrimaryIndexPutGet() {
        User user = new User(1, "test1@badmail.com", 1);

        UUID txId = UUID.randomUUID();

        Timestamp ver0 = Timestamp.nextVersion();

        // Create uncompleted version.
        primIdx.addWrite(user.id, user, txId);
        primIdx.printVersionChain(user.id);

        // Update to completed version.
        Timestamp ver1 = Timestamp.nextVersion();
        primIdx.commitWrite(user.id, ver1);
        primIdx.printVersionChain(user.id);

        // Change email.
        String email = "test" + user.id + "@goodmail.com";

        User updated = new User(user.id, email, user.deptId);
        primIdx.addWrite(user.id, updated, txId);
        primIdx.printVersionChain(user.id);

        Timestamp ver2 = Timestamp.nextVersion();

        // Update to new completed version.
        Timestamp ver3 = Timestamp.nextVersion();
        primIdx.commitWrite(user.id, ver3);
        primIdx.printVersionChain(user.id);

        Timestamp ver4 = Timestamp.nextVersion();

        User u0 = primIdx.read(user.id, ver0);
        System.out.println("Read: ver=" + ver0 + ", val=" + u0);
        assertNull(u0);

        User u1 = primIdx.read(user.id, ver1);
        System.out.println("Read: ver=" + ver1 + ", val=" + u1);
        assertEquals(user.email, u1.email);

        User u2 = primIdx.read(user.id, ver2);
        System.out.println("Read: ver=" + ver2 + ", val=" + u2);
        assertEquals(user, u1);

        User u3 = primIdx.read(user.id, ver3);
        System.out.println("Read: ver=" + ver3 + ", val=" + u3);
        assertEquals(email, u3.email);

        User u4 = primIdx.read(user.id, ver4);
        System.out.println("Read: ver=" + ver4 + ", val=" + u4);
        assertEquals(u3, u4);
    }

    @Test
    public void testPrimaryIndexPutScan() {
        List<User> users = IntStream.range(0, 10).mapToObj(i -> new User(i, "test" + i + "@badmail.com", i)).collect(toList());
        List<Timestamp> commits = new ArrayList<>();

        UUID txId = UUID.randomUUID();

        for (User user : users) {
            primIdx.addWrite(user.id, user, txId);
            Timestamp timestamp = Timestamp.nextVersion();
            commits.add(timestamp);
            primIdx.commitWrite(user.id, timestamp);
        }

        for (int i = 0; i < commits.size(); i++) {
            Timestamp timestamp = commits.get(i);
            Iterator<User> iter = primIdx.scan(null, false, null, false, timestamp);
            assertEquals(i + 1, stream(spliteratorUnknownSize(iter, ORDERED), false).count());
        }
    }

    @Test
    public void testPrimaryIndexPutScanLatest() {
        List<User> users = IntStream.range(0, 10).mapToObj(i -> new User(i, "test" + i + "@badmail.com", i)).collect(toList());
        List<Timestamp> commits = new ArrayList<>();

        UUID txId = UUID.randomUUID();

        for (User user : users) {
            primIdx.addWrite(user.id, user, txId);
            Timestamp timestamp = Timestamp.nextVersion();
            commits.add(timestamp);
            primIdx.commitWrite(user.id, timestamp);
        }

        Iterator<User> iter = primIdx.scan(null, false, null, false, null);

        // New transaction updates a committed value.
        UUID txId2 = UUID.randomUUID();

        while (iter.hasNext()) {
            User user = iter.next();

            primIdx.addWrite(user.id, new User(user.id, "hacked@hacked.org", user.deptId), txId2);
        }

        Iterator<User> iter2 = primIdx.scan(null, false, null, false, null);

        Timestamp commit2 = Timestamp.nextVersion();

        while (iter2.hasNext()) {
            User user = iter2.next();

            assertEquals("hacked@hacked.org", user.getEmail());

            primIdx.commitWrite(user.id, commit2);
        }

        Iterator<User> iter3 = primIdx.scan(null, false, null, false, null);

        // New transaction removes a committed value.
        UUID txId3 = UUID.randomUUID();

        while (iter3.hasNext()) {
            User user = iter3.next();

            assertEquals("hacked@hacked.org", user.getEmail());

            primIdx.addWrite(user.id, null, txId3);
        }

        Iterator<User> iter4 = primIdx.scan(null, false, null, false, null);

        assertFalse(iter4.hasNext());
    }

    @Test
    public void testSecondaryIndexUpdate() {
        final String email = "test1@badmail.com";

        User user = new User(1, email, 1);

        UUID txId = UUID.randomUUID();

        Timestamp ver0 = Timestamp.nextVersion();

        primIdx.addWrite(user.id, user, txId);
        emailIdx.put(user.email, user.id);

        Timestamp ver1 = Timestamp.nextVersion();
        Timestamp ver2 = Timestamp.nextVersion();

        primIdx.commitWrite(user.id, ver2);

        // Must see self write.
        validateScan(emailIdx, null, false, null, false, null, 1, user);
        validateScan(emailIdx, email, true, null, false, null, 1, user);
        validateScan(emailIdx, null, false, email, true, null, 1, user);
        validateScan(emailIdx, email, true, email, true, null, 1, user);
        validateScan(emailIdx, email, false, null, false, null, 0, null);
        validateScan(emailIdx, null, false, email, false, null, 0, null);
        validateScan(emailIdx, email, false, email, false, null, 0, null);

        // Must not see below write.
        validateScan(emailIdx, null, false, null, false, ver0, 0, null);
        validateScan(emailIdx, email, true, null, false, ver0, 0, null);
        validateScan(emailIdx, null, false, email, true, ver0, 0, null);
        validateScan(emailIdx, email, true, email, true, ver0, 0, null);
        validateScan(emailIdx, email, false, null, false, ver0, 0, null);
        validateScan(emailIdx, null, false, email, false, ver0, 0, null);
        validateScan(emailIdx, email, false, email, false, ver0, 0, null);

        // Must not see uncommitted write.
        validateScan(emailIdx, null, false, null, false, ver1, 0, null);
        validateScan(emailIdx, email, true, null, false, ver1, 0, null);
        validateScan(emailIdx, null, false, email, true, ver1, 0, null);
        validateScan(emailIdx, email, true, email, true, ver1, 0, null);
        validateScan(emailIdx, email, false, null, false, ver1, 0, null);
        validateScan(emailIdx, null, false, email, false, ver1, 0, null);
        validateScan(emailIdx, email, false, email, false, ver1, 0, null);

        // Must see committed write.
        validateScan(emailIdx, null, false, null, false, ver2, 1, user);
        validateScan(emailIdx, email, true, null, false, ver2, 1, user);
        validateScan(emailIdx, null, false, email, true, ver2, 1, user);
        validateScan(emailIdx, email, true, email, true, ver2, 1, user);
        validateScan(emailIdx, email, false, null, false, ver2, 0, null);
        validateScan(emailIdx, null, false, email, false, ver2, 0, null);
        validateScan(emailIdx, email, false, email, false, ver2, 0, null);

        Timestamp ver3 = Timestamp.nextVersion();

        // Reindex new email.
        User user2 = new User(1, "test1@goodmail.com", 1);

        primIdx.addWrite(user.id, user2, txId);
        emailIdx.put(user2.email, user.id);

        validateScan(emailIdx, null, false, null, false, ver2, 1, user); // Must not see uncommitted version.
        validateScan(emailIdx, null, false, null, false, ver3, 1, user); // Must not see uncommitted version.
        validateScan(emailIdx, null, false, null, false, null, 1, user2); // Must see self write.

        Timestamp ver4 = Timestamp.nextVersion();

        primIdx.commitWrite(user.id, ver4);
        validateScan(emailIdx, null, false, null, false, ver4, 1, user2); // Must see committed version.

        // Reindex new email.
        User user3 = new User(1, "test1@newmail.com", 1);

        primIdx.addWrite(user.id, user3, txId);
        emailIdx.put(user3.email, user.id);

        validateScan(emailIdx, null, false, null, false, ver4, 1, user2);
        validateScan(emailIdx, null, false, null, false, null, 1, user3);

        Timestamp ver5 = Timestamp.nextVersion();

        primIdx.commitWrite(user.id, ver5);
        validateScan(emailIdx, null, false, null, false, ver5, 1, user3);
    }

    private <SK extends Comparable<SK>, PK extends Comparable<PK>, T> void validateScan(
            SecondaryIndex<SK, PK, T> idx,
            @Nullable SK lower,
            boolean fromInclusive,
            @Nullable SK upper,
            boolean toInclusive,
            @Nullable Timestamp timestamp,
            int expSize,
            @Nullable T expVal
    ) {
        List<T> users = stream(spliteratorUnknownSize(
                idx.scan(lower, fromInclusive, upper, toInclusive, timestamp), ORDERED), false).collect(toList());

        if (expSize == 0) {
            assertTrue(users.isEmpty(), users.toString());
        }
        else {
            assertEquals(expSize, users.size(), users.toString());
        }

        if (expSize > 0) {
            assertEquals(expVal, users.get(0));
        }
    }

    @Test
    public void testSecondaryIndexScan() {
        UUID txId = UUID.randomUUID();

        // Split users in two depts.
        List<User> users = IntStream.range(0, 10).mapToObj(i -> new User(i, "test" + i + "@badmail.com", i / 5)).collect(toList());
        List<Timestamp> commits = new ArrayList<>();

        for (User user : users) {
            primIdx.addWrite(user.id, user, txId);
            Timestamp timestamp = Timestamp.nextVersion();
            commits.add(timestamp);
            deptIdx.put(user.deptId, user.id);
            primIdx.commitWrite(user.id, timestamp);
        }

        for (int i = 0; i < commits.size(); i++) {
            Timestamp timestamp = commits.get(i);
            Iterator<User> iter = deptIdx.scan(null, false, null, false, timestamp);

            assertEquals(i + 1, stream(spliteratorUnknownSize(iter, ORDERED), false).count());
        }

        validateScan(deptIdx, 0, true, 0, true, commits.get(commits.size() - 1), 5, users.get(0));
        validateScan(deptIdx, 0, true, 1, false, commits.get(commits.size() - 1), 5, users.get(0));
        validateScan(deptIdx, 0, true, 1, true, commits.get(commits.size() - 1), 10, users.get(0));
        validateScan(deptIdx, null, true, 1, false, commits.get(commits.size() - 1), 5, users.get(0));
        validateScan(deptIdx, 0, true, 1, true, commits.get(commits.size() - 2), 9, users.get(0));
        validateScan(deptIdx, 1, true, 1, true, commits.get(commits.size() - 1), 5, users.get(5));
        validateScan(deptIdx, 0, false, 1, true, commits.get(commits.size() - 1), 5, users.get(5));
    }

    private static class PrimaryIndex<PK extends Comparable<PK>, T> {
        /** Sorted map delegate. */
        private TreeMap<PK, VersionedValue<T>> map = new TreeMap<>();

        /**
         * Creates a new uncommitted version of a record identified by a given primary key.
         * <p>Only one uncommitted version can exist at a time.
         * <p>Uncommitted values can be read using {@code null} timestamp.
         *
         * @param key The primary key.
         * @param val The value or null for tombstone.
         * @param txId Txn id.
         * @return Version chain head.
         */
        public VersionedValue addWrite(PK key, @Nullable T val, UUID txId) {
            VersionedValue<T> top = map.get(key);

            if (top == null) {
                top = new VersionedValue<>(null, null, val, null);

                map.put(key, top);
            }
            else {
                assert top.begin != null : "Only one write intent is allowed: " + top;

                // Re-link.
                VersionedValue<T> next = new VersionedValue<>(top.begin, top.end, top.value, top.next);
                top.setId(txId);
                top.setBegin(null);
                top.setEnd(null);
                top.setValue(val);
                top.setNext(next);
            }

            return top;
        }

        public void commitWrite(PK pk, Timestamp timestamp) {
            VersionedValue<T> top = map.get(pk);

            top.setBegin(timestamp);
            top.setId(null);

            if (top.next != null)
                top.next.end = timestamp;
        }

        public void abortWrite(PK pk) {
            VersionedValue<T> top = map.get(pk);

            Objects.requireNonNull(top);

            if (top.next == null)
                map.remove(pk);
            else {
                top.setBegin(top.next.begin);
                top.setEnd(top.next.end);
                top.setValue(top.next.value);
                top.setNext(top.next);
            }
        }

        /**
         * @param pk The primary key.
         * @param timestamp The timestamp.
         * @return Versioned value or null if a tombstone or not found.
         */
        public @Nullable T read(PK pk, @Nullable Timestamp timestamp) {
            VersionedValue<T> top = map.get(pk);

            return read(top, timestamp);
        }

        private @Nullable T read(VersionedValue<T> top, @Nullable Timestamp timestamp) {
            if (top == null)
                return null;

            if (timestamp == null) {
                return top.value;
            }

            VersionedValue<T> cur = top;

            do {
                if (cur.begin != null && timestamp.compareTo(cur.begin) >= 0 && (cur.end == null || timestamp.compareTo(cur.end) < 0)) {
                    return cur.value;
                }

                cur = cur.next;
            } while(cur != null);

            return null;
        }

        public void cleanupCompletedVersions(@Nullable PK lower, boolean fromInclusive, @Nullable PK upper, boolean toInclusive, Timestamp timestamp) {
            // TODO
        }

        public void printVersionChain(PK key) {
            VersionedValue<T> top = map.get(key);

            if (top == null) {
                return;
            }

            System.out.println("key=" + key);
            System.out.println("    begin=" + top.begin + " end=" + top.end + ", value=" + top.value);

            VersionedValue<T> next = top.next;

            while(next != null) {
                System.out.println("    begin=" + next.begin + " end=" + next.end + ", value=" + next.value);

                next = next.next;
            }
        }

        /**
         * Scan the index at the given ts (null for latest).
         * <p>Tombstone values are skipped.
         *
         * TODO support for prefix scans.
         * @param ts The timestamp.
         * @return The iterator.
         */
        public Iterator<T> scan(@Nullable PK lower, boolean fromInclusive, @Nullable PK upper, boolean toInclusive, @Nullable Timestamp ts) {
            Iterator<Entry<PK, VersionedValue<T>>> iter;

            if (lower == null && upper != null) {
                iter = map.headMap(upper, toInclusive).entrySet().iterator();
            } else if (lower != null && upper == null) {
                iter = map.tailMap(lower, fromInclusive).entrySet().iterator();
            } else if (lower != null) {
                iter = map.subMap(lower, fromInclusive, upper, toInclusive).entrySet().iterator();
            } else {
                iter = map.entrySet().iterator();
            }

            return new Iterator<T>() {
                @Nullable T next;

                @Override
                public boolean hasNext() {
                    if (next == null) {
                        while(true) {
                            if (!iter.hasNext())
                                return false;

                            Entry<PK, VersionedValue<T>> entry = iter.next();

                            if ((next = read(entry.getValue(), ts)) != null)
                                return true;
                        }
                    }

                    return false;
                }

                @Override
                public T next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    T val = next;
                    next = null;

                    return val;
                }
            };
        }
    }

    private static class SecondaryIndex<SK extends Comparable<SK>, PK extends Comparable<PK>, T> {
        private TreeMap<SK, Set<PK>> map = new TreeMap<>();

        private PrimaryIndex<PK, T> primIdx;

        private final BiPredicate<SK, T> filter;

        private SecondaryIndex(PrimaryIndex<PK, T> primIdx, BiPredicate<SK, T> filter) {
            this.primIdx = primIdx;
            this.filter = filter;
        }

        public void put(SK secondaryKey, PK primaryKey) {
            map.compute(secondaryKey, (sk, pks) -> {
                if (pks == null)
                    pks = new HashSet<>();

                pks.add(primaryKey);

                return pks;
            });
        }

        public void remove(SK secondaryKey, PK primaryKey) {
            map.compute(secondaryKey, (sk, pks) -> {
                pks.remove(primaryKey);

                return pks.isEmpty() ? null : pks;
            });
        }

        public Iterator<T> scan(
                @Nullable SK lower,
                boolean fromInclusive,
                @Nullable SK upper,
                boolean toInclusive,
                @Nullable Timestamp timestamp
        ) {
            Iterator<Entry<SK, Set<PK>>> iter;

            if (lower == null && upper != null) {
                iter = map.headMap(upper, toInclusive).entrySet().iterator();
            } else if (lower != null && upper == null) {
                iter = map.tailMap(lower, fromInclusive).entrySet().iterator();
            } else if (lower != null) {
                iter = map.subMap(lower, fromInclusive, upper, toInclusive).entrySet().iterator();
            } else {
                iter = map.entrySet().iterator();
            }

            return new Iterator<T>() {
                @Nullable T next;
                SK sk;
                Iterator<PK> subIter;

                @Override
                public boolean hasNext() {
                    if (next == null) {
                        while(true) {
                            if (subIter == null) {
                                if (!iter.hasNext())
                                    return false;

                                Entry<SK, Set<PK>> entry = iter.next();

                                sk = entry.getKey();
                                subIter = entry.getValue().iterator();
                            }

                            if (!subIter.hasNext()) {
                                subIter = null;
                                sk = null;

                                continue;
                            }

                            PK pk = subIter.next();

                            T versionedValue = primIdx.read(pk, timestamp);

                            if (versionedValue != null) {
                                // Filter out false positive matches.
                                if (filter.test(sk, versionedValue)) {
                                    this.next = versionedValue;

                                    return true;
                                }
                            }
                        }
                    }

                    return false;
                }

                @Override
                public T next() {
                    if (next == null)
                        throw new NoSuchElementException();

                    T val = next;
                    next = null;

                    return val;
                }
            };
        }
    }

    private static class VersionedValue<T> {
        private @Nullable Timestamp begin;
        private @Nullable Timestamp end;
        private T value;
        private @Nullable UUID id; // Lock holder for uncommitted version.
        private @Nullable VersionedValue<T> next;

        private VersionedValue(@Nullable Timestamp begin, @Nullable Timestamp end, @Nullable T value, @Nullable VersionedValue<T> next) {
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

        public @Nullable VersionedValue<T> getNext() {
            return next;
        }

        public void setNext(@Nullable VersionedValue<T> next) {
            this.next = next;
        }

        public UUID getId() {
            return id;
        }

        public void setId(@Nullable UUID id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return S.toString(VersionedValue.class, this);
        }
    }

    private static class User {
        private int id;

        private String email;

        private int deptId;

        private User(int id, String email, int deptId) {
            this.id = id;
            this.email = email;
            this.deptId = deptId;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public int getDepartmentId() {
            return deptId;
        }

        public void setDepartmentId(int deptId) {
            this.deptId = deptId;
        }

        @Override
        public String toString() {
            return S.toString(User.class, this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            User user = (User) o;

            if (id != user.id) {
                return false;
            }
            if (deptId != user.deptId) {
                return false;
            }
            if (!email.equals(user.email)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + email.hashCode();
            result = 31 * result + deptId;
            return result;
        }
    }
}
