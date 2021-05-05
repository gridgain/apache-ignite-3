package org.apache.ignite.metastorage.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.MetastoreEvent;
import org.apache.ignite.metastorage.common.MetastoreEventListener;
import org.apache.ignite.metastorage.common.MetastoreEventListenerContext;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.WatchListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Single threaded meta service implementation for test purposes.
 */
public class TestMetaStorageService implements MetaStorageService, MetastoreEventListenerContext {
    public TreeMap<VersionedKey, Entry> data = new TreeMap<>();

    public long revision = 0;

    private Set<MetastoreEventListener> listeners = new HashSet<>();

    private List<MetastoreEvent> events = new ArrayList<>();

    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key) {
        return CompletableFuture.completedFuture(get0(key, Long.MAX_VALUE));
    }

    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key, long revUpperBound) {
        return CompletableFuture.completedFuture(get0(key, revUpperBound));
    }

    private Entry get0(Key key, long rev) {
        VersionedKey verKey = new VersionedKey(key.bytes(), rev);

        NavigableMap<VersionedKey, Entry> map = data.tailMap(verKey, true);

        if (map.isEmpty())
            return null;

        Map.Entry<VersionedKey, Entry> first = map.firstEntry();

        return Arrays.equals(first.getKey().data, key.bytes()) ? first.getValue() : null;
    }

    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys, long revUpperBound) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> put(@NotNull Key key, @NotNull byte[] value) {
        revision++;

        VersionedKey verKey = new VersionedKey(key.bytes(), revision);

        Entry e = new EntryImpl(key, value, revision);

        data.put(verKey, e);

        return CompletableFuture.completedFuture(null);
    }

    @Override public @NotNull CompletableFuture<Entry> getAndPut(@NotNull Key key, @NotNull byte[] value) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<Key, byte[]> vals) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndPutAll(@NotNull Map<Key, byte[]> vals) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> remove(@NotNull Key key) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull Key key) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<Key> keys) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndRemoveAll(@NotNull Collection<Key> keys) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> invoke(@NotNull Key key, @NotNull Condition condition, @NotNull Operation success, @NotNull Operation failure) {
        return invoke(List.of(key), condition, List.of(success), List.of(failure), 0);
    }

    @Override public CompletableFuture<Boolean> invoke(@NotNull List<Key> keys, @NotNull Condition condition, @NotNull List<Operation> success, @NotNull List<Operation> failure, int evtMarker) {
        revision++;

        Entry entry = get0(keys.get(0), Long.MAX_VALUE);

        List<Entry> entries = new ArrayList<>();
        List<Entry> oldEntries = new ArrayList<>();

        boolean res = condition.test(entry);

        List<Operation> ops = res ? success : failure;

        for (int i = 0; i < ops.size(); i++) {
            Operation operation = ops.get(i);
            Object upd = operation.upd;

            if (upd instanceof Operation.PutOp) {
                Operation.PutOp putOp0 = (Operation.PutOp) upd;

                Entry e = new EntryImpl(keys.get(i), putOp0.val, revision);
                entries.add(e);
                data.put(new VersionedKey(e.key().bytes(), revision), e);

                Entry old = get0(keys.get(i), revision - 1);

                oldEntries.add(old);
            }
        }

        for (MetastoreEventListener listener : listeners)
            listener.onUpdate(evtMarker, entries, oldEntries, revision, this);

        return CompletableFuture.completedFuture(res);
    }

    @Override public @NotNull CompletableFuture<Entry> getAndInvoke(@NotNull Key key, @NotNull Condition condition, @NotNull Operation success, @NotNull Operation failure) {
        return null;
    }

    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo, long revUpperBound) {
        return null;
    }

    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return null;
    }

    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@Nullable Key keyFrom, @Nullable Key keyTo, long revision, @NotNull WatchListener lsnr) {
        return null;
    }

    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Key key, long revision, @NotNull WatchListener lsnr) {
        return null;
    }

    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Collection<Key> keys, long revision, @NotNull WatchListener lsnr) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> compact() {
        return null;
    }

    @Override public void addMetastoreListener(MetastoreEventListener lsnt) {
        listeners.add(lsnt);
    }

    @Override public List<MetastoreEvent> fetch(long fromRev) {
        List<MetastoreEvent> copy = new ArrayList<>();

        for (MetastoreEvent event : events) {
            if (event.getRevision() >= fromRev)
                copy.add(event);
        }

        return copy;
    }

    @Override public void discard(long fromRev, long toRev) {
        // TODO
    }

    @Override public void addEvent(MetastoreEvent event) {
        events.add(event);
    }

    class VersionedKey implements Comparable<VersionedKey> {
        byte[] data;

        long version;

        VersionedKey(byte[] data, long version) {
            this.data = data;
            this.version = version;
        }

        @Override public int compareTo(@NotNull TestMetaStorageService.VersionedKey o) {
            int ret = Arrays.compare(data, o.data);

            return ret == 0 ? Long.compare(o.version, version) : ret;
        }

        @Override public String toString() {
            return new String(data) + ":" + version;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionedKey that = (VersionedKey) o;

            if (version != that.version) return false;
            if (!Arrays.equals(data, that.data)) return false;

            return true;
        }

        @Override public int hashCode() {
            int result = Arrays.hashCode(data);
            result = 31 * result + (int) (version ^ (version >>> 32));
            return result;
        }
    }

    private static class EntryImpl implements Entry {
        private Key key;
        private byte[] value;
        private long revision;

        EntryImpl(Key key, byte[] value, long revision) {
            this.key = key;
            this.value = value;
            this.revision = revision;
        }

        @Override public @NotNull Key key() {
            return key;
        }

        @Nullable @Override public byte[] value() {
            return value;
        }

        @Override public long revision() {
            return revision;
        }

        @Override public String toString() {
            return new String(key.bytes()) + ":" + ByteUtils.fromBytes(value) + ":" + revision;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EntryImpl entry = (EntryImpl) o;

            if (revision != entry.revision) return false;
            if (!key.equals(entry.key)) return false;
            if (!Arrays.equals(value, entry.value)) return false;

            return true;
        }

        @Override public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + Arrays.hashCode(value);
            result = 31 * result + (int) (revision ^ (revision >>> 32));
            return result;
        }
    }

}
