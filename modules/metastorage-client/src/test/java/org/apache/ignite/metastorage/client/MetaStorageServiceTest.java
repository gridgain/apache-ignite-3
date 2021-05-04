package org.apache.ignite.metastorage.client;

import java.io.Serializable;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.MetastoreEvent;
import org.apache.ignite.metastorage.common.MetastoreEventListener;
import org.apache.ignite.metastorage.common.MetastoreEventListenerContext;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.Operations;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.metastorage.common.Operations.noop;
import static org.apache.ignite.metastorage.common.Operations.put;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class MetaStorageServiceTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageServiceTest.class);

    /** */
    private static final int TABLE_CREATE_EVT_MARKER = 1;

    /** */
    private static final int CHANGE_AFFINITY_EVT_MARKER = 2;

    /** Configured tables list. */
    private static final String TABLE_CONFIG_PREFIX = "config.tables.";

    /** Table versions. */
    private static final String TABLE_VERSION_PREFIX = "version.tables.";

    /** Tables name to id map. */
    private static final String TABLE_ID_PREFIX = "id.tables.";

    /** Table change versions. */
    private static final String TABLE_CHANGE_PREFIX = "meta.tables.";

    /** Calculated affinity list. */
    private static final String TABLE_AFFINITY_PREFIX = "affinity.tables.";

    /** Table schemas list. */
    private static final String TABLE_SCHEMA_PREFIX = "schema.tables."; // TODO swap

    private TestMetaStorageService metaStorageService;

    @BeforeEach
    public void before() {
        metaStorageService = new TestMetaStorageService();

        // Listen for key changes and translate it to events.
        metaStorageService.addMetastoreListener(new MetastoreEventListener() {
            @Override public boolean onUpdate(int evtMarker, List<Entry> entries, List<Entry> oldEntries, long revision, MetastoreEventListenerContext ctx) {
                if (entries.size() == 1) {
                    Entry newEntry = entries.get(0);
                    Entry oldEntry = oldEntries.get(0);

                    if (newEntry != null && oldEntry == null) { // A table is created.
                        String keyStr = new String(newEntry.key().bytes());

                        if (keyStr.startsWith(TABLE_CONFIG_PREFIX)) {
                            ctx.addEvent(new TableCreatedEvent(revision, (TableConfig) fromBytes(newEntry.value())));
                        }
                    }
                    else if (newEntry != null && oldEntry != null) { // A table is updated.
                        String keyStr = new String(newEntry.key().bytes());

                        if (keyStr.startsWith(TABLE_CONFIG_PREFIX)) {
                            ctx.addEvent(new TableChangedEvent(revision, (TableConfig) fromBytes(oldEntry.value()), (TableConfig) fromBytes(newEntry.value())));
                        }
                    }
                    else if (newEntry == null && oldEntry != null) { // A table is deleted.
                        String keyStr = new String(oldEntry.key().bytes());

                        if (keyStr.startsWith(TABLE_CONFIG_PREFIX)) {
                            // TODO delete.
                        }
                    }
                }
                else if (evtMarker == TABLE_CREATE_EVT_MARKER && entries.size() == 4) {
                    List<List<String>> affinity = (List<List<String>>) fromBytes(entries.get(2).value());

                    String schema = (String) fromBytes(entries.get(3).value());

                    long tableId = (long) fromBytes(entries.get(1).value());

                    String keyStr = new String(entries.get(1).key().bytes());

                    String tableName = keyStr.substring(TABLE_ID_PREFIX.length());

                    assert affinity != null && schema != null : entries;

                    TableStartedEvent evt = new TableStartedEvent(revision, tableId, tableName, affinity, schema);

                    ctx.addEvent(evt);
                }
                else if (evtMarker == CHANGE_AFFINITY_EVT_MARKER && entries.size() == 2) {
                    List<List<String>> oldAffinity = (List<List<String>>) fromBytes(oldEntries.get(1).value());

                    List<List<String>> newAffinity = (List<List<String>>) fromBytes(entries.get(1).value());

                    String keyStr = new String(entries.get(1).key().bytes());

                    long tableId = Long.parseLong(keyStr.substring(TABLE_AFFINITY_PREFIX.length()));

                    AffinityChangedEvent evt = new AffinityChangedEvent(revision, tableId, oldAffinity, newAffinity);

                    ctx.addEvent(evt);
                }

                return false;
            }
        });
    }

    @Test
    public void testOperations() throws Exception {
        Key key = new Key("k");

        assertNull(metaStorageService.get(key).get());

        metaStorageService.put(key, ByteUtils.toBytes(0));
        metaStorageService.put(key, ByteUtils.toBytes(1));
        metaStorageService.put(key, ByteUtils.toBytes(2));

        Key key2 = new Key("kk");

        assertNull(metaStorageService.get(key2).get());

        metaStorageService.put(key2, ByteUtils.toBytes(0));
        metaStorageService.put(key2, ByteUtils.toBytes(1));
        metaStorageService.put(key2, ByteUtils.toBytes(2));

        Entry lastEntry = metaStorageService.get(key).get();
        assertEquals(3, lastEntry.revision());

        Entry lastEntry2 = metaStorageService.get(key2).get();
        assertEquals(6, lastEntry2.revision());

        Entry entry = metaStorageService.get(key, 2).get();
        assertEquals(2, entry.revision());
        Entry entry2 = metaStorageService.get(key2, 2).get();
        assertNull(entry2);

        entry = metaStorageService.get(key, 5).get();
        assertEquals(3, entry.revision());
        entry2 = metaStorageService.get(key2, 5).get();
        assertEquals(5, entry2.revision());

        List<Key> key1 = List.of(key, key2);
        List<Operation> suc = List.of(Operations.put(ByteUtils.toBytes(100)), Operations.put(ByteUtils.toBytes(100)));
        List<Operation> fail = List.of(Operations.noop(), Operations.noop());

        metaStorageService.invoke(key1, Conditions.revision().eq(2), suc, fail, 0);

        // Expecting nothing changed.
        entry = metaStorageService.get(key, 5).get();
        assertEquals(3, entry.revision());
        entry2 = metaStorageService.get(key2, 5).get();
        assertEquals(5, entry2.revision());

        metaStorageService.invoke(key1, Conditions.revision().eq(3), suc, fail, 0);

        // Expecting nothing changed.
        entry = metaStorageService.get(key).get();
        assertEquals(100, fromBytes(entry.value()));
        entry2 = metaStorageService.get(key2).get();
        assertEquals(100, fromBytes(entry2.value()));

        System.out.println(metaStorageService.data);
    }

    @Test
    public void testCreateTable() throws InterruptedException, ExecutionException {
        final String tableName = "testTable";

        createTable(tableName, 2, 3);

        long rev0 = metaStorageService.revision;

        List<MetastoreEvent> events = metaStorageService.fetch(0);

        assertEquals(1, events.size());

        processEvents(events);

        long rev1 = metaStorageService.revision;

        assertTrue(rev1 > rev0);

        List<MetastoreEvent> events2 = metaStorageService.fetch(0);

        assertEquals(2, events2.size());

        TreeMap<TestMetaStorageService.VersionedKey, Entry> data = new TreeMap<>(metaStorageService.data);

        // Test idempotency. Events can be processed again without side effects.
        processEvents(events2);

        // Metastorage state shouldn't change.
        List<MetastoreEvent> events3 = metaStorageService.fetch(0);

        assertEquals(2, events3.size(), "Metastorage state shouldn't change");

        assertEquals(data, metaStorageService.data, "Metastorage state shouldn't change");
    }

    @Test
    public void testCreateStartChange() throws InterruptedException, ExecutionException {
        final String tableName = "testTable";

        createTable(tableName, 2, 3);

        List<MetastoreEvent> events = metaStorageService.fetch(0);
        assertEquals(1, events.size());
        processEvents(events);

        List<MetastoreEvent> events2 = metaStorageService.fetch(0);
        assertEquals(2, events2.size());

        setBackups(tableName, 1);

        List<MetastoreEvent> events3 = metaStorageService.fetch(0);
        assertEquals(3, events3.size());
        processEvents(events3);

        List<MetastoreEvent> events4 = metaStorageService.fetch(0);
        assertEquals(4, events4.size());
    }

    @Test
    public void testCreateChangeStart() throws InterruptedException, ExecutionException {
        final String tableName = "testTable";

        createTable(tableName, 2, 3);
        setBackups(tableName, 1);

        List<MetastoreEvent> events = metaStorageService.fetch(0);

        assertEquals(2, events.size());

        processEvents(events);

        List<MetastoreEvent> events2 = metaStorageService.fetch(0);
        assertEquals(3, events2.size());

        processEvents(events2);

        // Table is started, can now retry.
        setBackups(tableName, 1);

        List<MetastoreEvent> events3 = metaStorageService.fetch(0);
        assertEquals(4, events3.size());

        processEvents(events3);

        List<MetastoreEvent> events4 = metaStorageService.fetch(0);

        assertEquals(5, events4.size());
    }

    private void processEvents(List<MetastoreEvent> events) {
        for (MetastoreEvent event : events) {
            if (event instanceof TableCreatedEvent) { // This is create table event.
                TableCreatedEvent event0 = (TableCreatedEvent) event;

                String tblName = event0.getConfig().getTableName();

                long tblId = event.getRevision(); // Metastore revision is unique.

                // Calculate affinity
                List<List<String>> affinity = List.of(List.of("node1", "node2"), List.of("node2", "node3"), List.of("node1", "node3"));

                // Set schema.
                String schema = event0.getConfig().getSchema();

                try {
                    Key changeVerKey = new Key(TABLE_CHANGE_PREFIX + tblId); // Table change id, used for CAS.
                    Key nameToIdKey = new Key(TABLE_ID_PREFIX + tblName); // For resolving table id by name.
                    Key affKey = new Key(TABLE_AFFINITY_PREFIX + tblId); // Affinity key.
                    Key schemaKey = new Key(TABLE_SCHEMA_PREFIX + tblId); // Schema key.

                    UUID changeId = UUID.randomUUID(); // Value doesn't matter.

                    metaStorageService.invoke(
                        List.of(changeVerKey, nameToIdKey, affKey, schemaKey),
                        Conditions.value().eq(null),
                        List.of(put(toBytes(changeId)), put(toBytes(tblId)), put(toBytes(affinity)), put(toBytes(schema))),
                        List.of(noop(), noop(), noop(), noop()),
                        TABLE_CREATE_EVT_MARKER).get();
                }
                catch (Exception e) {
                    break;
                }
            }
            else if (event instanceof TableChangedEvent) {
                TableChangedEvent event0 = (TableChangedEvent) event;

                if (event0.oldConfig.getBackups() != event0.newConfig.getBackups()) {
                    try {
                        Entry idEntry = metaStorageService.get(new Key(TABLE_ID_PREFIX + event0.newConfig.getTableName()), event0.getRevision()).get();

                        if (idEntry == null) {
                            LOG.warn("Table \"{0}\" is not started yet, retry later", event0.newConfig.getTableName());

                            continue; // Ignore and continue - table not started.
                        }

                        long tblId = (long) fromBytes(idEntry.value());

                        // Recalculate affinity
                        List<List<String>> newAffinity = List.of(List.of("node1"), List.of("node2"), List.of("node3"));

                        Key changeVerKey = new Key(TABLE_CHANGE_PREFIX + tblId); // Table change id, used for CAS.
                        Key affKey = new Key(TABLE_AFFINITY_PREFIX + tblId); // Affinity key.

                        Entry entry = metaStorageService.get(changeVerKey, event0.getRevision()).get();

                        UUID changeId = UUID.randomUUID(); // Value doesn't matter.

                        metaStorageService.invoke(
                            List.of(changeVerKey, affKey),
                            Conditions.revision().eq(entry.revision()),
                            List.of(put(toBytes(changeId)), put(toBytes(newAffinity))),
                            List.of(noop(), noop()),
                            CHANGE_AFFINITY_EVT_MARKER).get();
                    } catch (Exception e) {
                        // TODO handle error.
                    }
                }
            }
            else if (event instanceof TableStartedEvent) {
                // TODO create local raft group for partition.
            }
            else if (event instanceof AffinityChangedEvent) {
                // TODO recreate raft groups according to new affinity.
            }
        }
    }

    private static class TableConfig implements Serializable {
        private String tableName;

        private int backups;

        private int parts;

        private String schema;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public int getBackups() {
            return backups;
        }

        public void setBackups(int backups) {
            this.backups = backups;
        }

        public int getPartitions() {
            return parts;
        }

        public void setPartitions(int parts) {
            this.parts = parts;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        @Override public String toString() {
            return S.toString(TableConfig.class, this);
        }
    }

    private static class TableCreatedEvent extends MetastoreEvent {
        private final TableConfig config;

        TableCreatedEvent(long revision, TableConfig config) {
            super(revision);
            this.config = config;
        }

        public TableConfig getConfig() {
            return config;
        }

        @Override public String toString() {
            return S.toString(TableCreatedEvent.class, this);
        }
    }

    private static class TableChangedEvent extends MetastoreEvent {
        private final TableConfig oldConfig;

        private final TableConfig newConfig;

        TableChangedEvent(long revision, TableConfig oldConfig, TableConfig newConfig) {
            super(revision);
            this.oldConfig = oldConfig;
            this.newConfig = newConfig;
        }

        public TableConfig getOldConfig() {
            return oldConfig;
        }

        public TableConfig getNewConfig() {
            return newConfig;
        }

        @Override public String toString() {
            return S.toString(TableChangedEvent.class, this);
        }
    }

    private static class TableStartedEvent extends MetastoreEvent {
        private long tableId;

        private String tableName;

        private List<List<String>> affinity;

        private String schema;

        TableStartedEvent(long revision, long tableId, String tableName, List<List<String>> affinity, String schema) {
            super(revision);
            this.tableId = tableId;
            this.tableName = tableName;
            this.affinity = affinity;
            this.schema = schema;
        }

        public String getTableName() {
            return tableName;
        }

        public List<List<String>> getAffinity() {
            return affinity;
        }

        public String getSchema() {
            return schema;
        }
    }

    private static class AffinityChangedEvent extends MetastoreEvent {
        private final long tableId;

        private final List<List<String>> oldAffinity;

        private final List<List<String>> newAffinity;

        AffinityChangedEvent(long revision, long tableId, List<List<String>> oldAffinity, List<List<String>> newAffinity) {
            super(revision);
            this.tableId = tableId;
            this.oldAffinity = oldAffinity;
            this.newAffinity = newAffinity;
        }

        public List<List<String>> getOldAffinity() {
            return oldAffinity;
        }

        public List<List<String>> getNewAffinity() {
            return newAffinity;
        }
    }

    /**
     * @param tableName
     * @param backups
     * @param partitions
     * @return
     */
    private CompletableFuture<Boolean> createTable(String tableName, int backups, int partitions) {
        TableConfig cfg = new TableConfig();
        cfg.setTableName(tableName);
        cfg.setBackups(backups);
        cfg.setPartitions(partitions);
        cfg.setSchema("{id INT, name VARCHAR(100)}");

        return metaStorageService.invoke(
            new Key(TABLE_CONFIG_PREFIX + tableName),
            Conditions.value().eq(null),
            put(toBytes(cfg)),
            noop());
    }

    /**
     * @param tableName
     * @param newBackups
     * @return
     */
    private CompletableFuture<Boolean> setBackups(String tableName, int newBackups) {
        CompletableFuture<Entry> getFut = metaStorageService.get(new Key(TABLE_CONFIG_PREFIX + tableName));

        return getFut.thenCompose(new Function<Entry, CompletionStage<Boolean>>() {
            @Override public CompletionStage<Boolean> apply(Entry prevEntry) {
                @Nullable byte[] val = prevEntry.value();

                TableConfig cfg = (TableConfig) fromBytes(val);
                cfg.setBackups(newBackups);

                return metaStorageService.invoke(
                    new Key(TABLE_CONFIG_PREFIX + tableName),
                    Conditions.revision().eq(prevEntry.revision()),
                    put(toBytes(cfg)),
                    noop());
            }
        });
    }

    private void dropTable(String tableName) {

    }
}
