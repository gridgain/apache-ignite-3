package org.apache.ignite.internal.sql.engine.querydb;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class DbTable {

    private final String table;

    private final List<String> columnsNames;

    private final List<Map.Entry<String, String>> columns;

    private final List<String> primaryKey;

    private DbTable(Builder builder) {
        this.table = builder.table;
        this.columnsNames = builder.columns.stream()
                .map(Entry::getKey)
                .collect(Collectors.toList());

        this.columns = List.copyOf(builder.columns);
        this.primaryKey = List.copyOf(builder.primaryKey);
    }

    public List<String> columns() {
        return columnsNames;
    }

    public String makeSqlCreate() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(table);
        sb.append(" (");

        for (var col : columns) {
            sb.append(col.getKey());
            sb.append(" ");
            sb.append(col.getValue());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);

        sb.append(", PRIMARY KEY (");
        sb.append(String.join(", ", primaryKey));
        sb.append("))");
        return sb.toString();
    }

    public String makeSqlDrop() {
        return "DROP TABLE IF EXISTS " + table;
    }

    public static Builder builder(String table) {
        return new Builder(table);
    }

    public String name() {
        return table;
    }

    public static class Builder {

        private final String table;

        private final List<Map.Entry<String, String>> columns = new ArrayList<>();

        private final List<String> primaryKey = new ArrayList<>();

        public Builder(String table) {
            this.table = table;
        }

        public Builder addColumn(String name, String type) {
            this.columns.add(Map.entry(name, type));
            return this;
        }

        public Builder primaryKey(String... columns) {
            this.primaryKey.clear();
            this.primaryKey.addAll(Arrays.asList(columns));
            return this;
        }

        public DbTable build() {
            return new DbTable(this);
        }
    }
}
