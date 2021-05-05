package org.apache.ignite.metastorage.client;

import java.io.Serializable;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.MetastoreEvent;
import org.apache.ignite.metastorage.common.MetastoreEventListener;
import org.apache.ignite.metastorage.common.MetastoreEventListenerContext;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.metastorage.common.Operations.noop;
import static org.apache.ignite.metastorage.common.Operations.put;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class MetaStorageServiceEventsTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageServiceEventsTest.class);

    /** */
    private static final int TABLE_CREATE_EVT_MARKER = 1;

    /** */
    private static final int CHANGE_AFFINITY_EVT_MARKER = 2;

    /** Table configurations. */
    private static final String TABLE_CONFIG_PREFIX = "config.tables.";

    /** Tables name to id map. */
    private static final String TABLE_ID_PREFIX = "id.tables.";

    /** Table change versions. Guards table metadata updates. */
    private static final String TABLE_CHANGE_PREFIX = "meta.tables.";

    /** Calculated affinity list. */
    private static final String TABLE_AFFINITY_PREFIX = "affinity.tables.";

    /** Table schemas list. */
    private static final String TABLE_SCHEMA_PREFIX = "schema.tables."; // TODO swap

    private TestMetaStorageService metaStorageService;

    @BeforeEach
    public void before() {
        metaStorageService = new TestMetaStorageService();

        // Listen for metastore changes and translate it to events.
        metaStorageService.addMetastoreListener(new MetastoreEventListener() {
            @Override public boolean onUpdate(int evtMarker, List<Entry> entries, List<Entry> oldEntries, long revision, MetastoreEventListenerContext ctx) {
                // TODO event translation could be cleaner.
                if (entries.size() == 1) {
                    Entry newEntry = entries.get(0);
                    Entry oldEntry = oldEntries.get(0);

                    if (newEntry != null && oldEntry == null) { // A table is created.
                        String keyStr = new String(newEntry.key().bytes());

                        if (keyStr.startsWith(TABLE_CONFIG_PREFIX)) {
                            TableConfig tableConfig = (TableConfig) fromBytes(newEntry.value());
                            ctx.addEvent(new TableCreatedEvent(revision, tableConfig.getTableName()));
                        }
                    }
                    else if (newEntry != null && oldEntry != null) { // A table is updated.
                        String keyStr = new String(newEntry.key().bytes());

                        if (keyStr.startsWith(TABLE_CONFIG_PREFIX)) {
                            ctx.addEvent(new TableChangedEvent(revision, ((TableConfig) fromBytes(newEntry.value())).getTableName()));
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

    public long getTableId(String tblName) {
        Key key = new Key(TABLE_ID_PREFIX + tblName);

        Entry entry = metaStorageService.get(key).getNow(null);

        if (entry == null)
            throw new NullPointerException();

        return (long) fromBytes(entry.value());
    }

    public List<List<String>> getTableAffinity(long tblId) {
        Key key = new Key(TABLE_AFFINITY_PREFIX + tblId);

        Entry entry = metaStorageService.get(key).getNow(null);

        if (entry == null)
            throw new NullPointerException();

        return (List<List<String>>) fromBytes(entry.value());
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

        // Test idempotency. Events can be processed again without side effects.
        TreeMap<TestMetaStorageService.VersionedKey, Entry> data = new TreeMap<>(metaStorageService.data);
        processEvents(events2);
        List<MetastoreEvent> events3 = metaStorageService.fetch(0);
        assertEquals(2, events3.size());
        assertEquals(data, metaStorageService.data, "Metastorage state shouldn't change");

        long tableId = getTableId(tableName);
        assertEquals(1, tableId);

        List<List<String>> affinity = getTableAffinity(tableId);
        assertEquals(2, affinity.get(0).size());
    }

    @Test
    public void testCreateStartChangeTable() throws InterruptedException, ExecutionException {
        final String tableName = "testTable";

        createTable(tableName, 2, 3);

        List<MetastoreEvent> events = metaStorageService.fetch(0);
        assertEquals(1, events.size());
        processEvents(events);

        List<MetastoreEvent> events2 = metaStorageService.fetch(0);
        assertEquals(2, events2.size());

        processEvents(events2);

        List<MetastoreEvent> events3 = metaStorageService.fetch(0);
        assertEquals(2, events3.size());

        setBackups(tableName, 1);

        List<MetastoreEvent> events4 = metaStorageService.fetch(0);
        assertEquals(3, events4.size());
        processEvents(events4);

        List<MetastoreEvent> events5 = metaStorageService.fetch(0);
        assertEquals(4, events5.size());

        // Test idempotency.
        TreeMap<TestMetaStorageService.VersionedKey, Entry> data = new TreeMap<>(metaStorageService.data);
        processEvents(events5);
        List<MetastoreEvent> events6 = metaStorageService.fetch(0);
        assertEquals(4, events6.size());
        assertEquals(data, metaStorageService.data, "Metastorage state shouldn't change");

        long tableId = getTableId(tableName);
        assertEquals(1, tableId);

        List<List<String>> affinity = getTableAffinity(tableId);
        assertEquals(1, affinity.get(0).size());
    }

    @Test
    public void testCreateChangeStartTable() throws InterruptedException, ExecutionException {
        final String tableName = "testTable";

        createTable(tableName, 2, 3);
        setBackups(tableName, 1);

        List<MetastoreEvent> events = metaStorageService.fetch(0);

        assertEquals(2, events.size());

        processEvents(events.subList(0, 1)); // Should create table with merged changes.

        List<MetastoreEvent> events2 = metaStorageService.fetch(0);
        assertEquals(3, events2.size());

        // Test idempotency.
        TreeMap<TestMetaStorageService.VersionedKey, Entry> data = new TreeMap<>(metaStorageService.data);
        processEvents(events2);
        List<MetastoreEvent> events3 = metaStorageService.fetch(0);
        assertEquals(3, events3.size());
        assertEquals(data, metaStorageService.data, "Metastorage state shouldn't change");

        long tableId = getTableId(tableName);
        assertEquals(1, tableId);

        List<List<String>> affinity = getTableAffinity(tableId);
        assertEquals(1, affinity.get(0).size());
    }

    private void processEvents(List<MetastoreEvent> events) {
        for (MetastoreEvent event : events) {
            if (event instanceof TableCreatedEvent) { // This is create table event.
                TableCreatedEvent event0 = (TableCreatedEvent) event;

                try {
                    // Read latest config to start (will merge all pending change events)
                    Entry cfgEntry = metaStorageService.get(new Key(TABLE_CONFIG_PREFIX + event0.getTableName())).get();

                    long rev = cfgEntry.revision();

                    TableConfig actualCfg = (TableConfig) fromBytes(cfgEntry.value());

                    String tblName = actualCfg.getTableName();

                    long tblId = event0.getRevision();

                    // Calculate affinity
                    List<List<String>> affinity = actualCfg.getBackups() == 2 ?
                        List.of(List.of("node1", "node2"), List.of("node2", "node3"), List.of("node1", "node3")) :
                        List.of(List.of("node1"), List.of("node2"), List.of("node3"));

                    // Set schema.
                    String schema = actualCfg.getSchema();

                    Key changeVerKey = new Key(TABLE_CHANGE_PREFIX + tblId); // Table change id, used for CAS.
                    Key nameToIdKey = new Key(TABLE_ID_PREFIX + tblName); // For resolving table id by name.
                    Key affKey = new Key(TABLE_AFFINITY_PREFIX + tblId); // Affinity key.
                    Key schemaKey = new Key(TABLE_SCHEMA_PREFIX + tblId); // Schema key.

                    long changeId = rev; // The revision number of configuration change.

                    metaStorageService.invoke(
                        List.of(changeVerKey, nameToIdKey, affKey, schemaKey),
                        Conditions.value().eq(null),
                        List.of(put(toBytes(changeId)), put(toBytes(tblId)), put(toBytes(affinity)), put(toBytes(schema))),
                        List.of(noop(), noop(), noop(), noop()),
                        TABLE_CREATE_EVT_MARKER).get();
                }
                catch (Exception e) {
                    LOG.error("Failed to process event", e);

                    continue;
                }
            }
            else if (event instanceof TableChangedEvent) {
                TableChangedEvent event0 = (TableChangedEvent) event;

                try {
                    Entry idEntry = metaStorageService.get(new Key(TABLE_ID_PREFIX + event0.getTableName()), event0.getRevision()).get();

                    if (idEntry == null) {
                        LOG.warn("Table \"{0}\" is not started yet, event is ignored", event0.getTableName());

                        continue; // Ignore and continue - table not started.
                    }

                    long tblId = (long) fromBytes(idEntry.value());

                    Key changeVerKey = new Key(TABLE_CHANGE_PREFIX + tblId); // Table change id, used for CAS.
                    Key affKey = new Key(TABLE_AFFINITY_PREFIX + tblId); // Affinity key.

                    Entry changeEntry = metaStorageService.get(changeVerKey, event0.getRevision()).get();

                    // Actual configurations are read from metastore.
                    Entry oldCfgEntry = metaStorageService.get(new Key(TABLE_CONFIG_PREFIX + event0.getTableName()), changeEntry.revision()).get();
                    Entry newCfgEntry = metaStorageService.get(new Key(TABLE_CONFIG_PREFIX + event0.getTableName()), event0.getRevision()).get();

                    TableConfig oldCfg = (TableConfig) fromBytes(oldCfgEntry.value());
                    TableConfig newCfg = (TableConfig) fromBytes(newCfgEntry.value());

                    if (oldCfg.getBackups() == newCfg.getBackups()) {
                        LOG.warn("Configuration has not changed, event is ignored");

                        continue;
                    }

                    // Recalculate affinity
                    List<List<String>> newAffinity = newCfg.getBackups() == 2 ?
                        List.of(List.of("node1", "node2"), List.of("node2", "node3"), List.of("node1", "node3")) :
                        List.of(List.of("node1"), List.of("node2"), List.of("node3"));

                    long changeId = event0.getRevision(); // The revision number of configuration change.

                    metaStorageService.invoke(
                        List.of(changeVerKey, affKey),
                        Conditions.revision().eq(changeEntry.revision()),
                        List.of(put(toBytes(changeId)), put(toBytes(newAffinity))),
                        List.of(noop(), noop()),
                        CHANGE_AFFINITY_EVT_MARKER).get();
                } catch (Exception e) {
                    // TODO handle error.
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
        private final String tableName;

        TableCreatedEvent(long revision, String tableName) {
            super(revision);
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override public String toString() {
            return S.toString(TableCreatedEvent.class, this);
        }
    }

    private static class TableChangedEvent extends MetastoreEvent {
        private final String tableName;

        TableChangedEvent(long revision, String tableName) {
            super(revision);
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
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

                if (cfg.getBackups() == newBackups)
                    return CompletableFuture.completedFuture(false);

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
