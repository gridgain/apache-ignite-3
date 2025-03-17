package org.apache.ignite.internal.sql.engine.querydb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class DbRecords {

    private final List<StatementRecord> statements = new ArrayList<>();

    public StatementRecord newStatementRecord() {
        return new StatementRecord();
    }

    public void addStatement(StatementRecord record) {
        statements.add(record);
    }

    public List<StatementRecord> statements() {
        return statements;
    }

    public void clear() {
        statements.clear();
    }

    public static class StatementRecord {
        private final List<Entry<DbInsert, Map<String, Object>>> inserts = new ArrayList<>();

        private StatementRecord() {

        }

        public void add(DbTable table, Map<String, Object> values) {
            inserts.add(Map.entry(new DbInsert(table), values));
        }

        public List<Map.Entry<DbInsert, Map<String, Object>>> inserts() {
            return inserts;
        }
    }
}
