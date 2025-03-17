package org.apache.ignite.internal.sql.engine.querydb;

import java.util.Map;

public class DbInsert {

    private final DbTable table;

    public DbInsert(DbTable table) {
        this.table = table;
    }

    public DbTable table() {
        return table;
    }

    public String makeSqlString(Map<String, Object> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.name());
        sb.append(" (");
        sb.append(String.join(", ", table.columns()));
        sb.append(") ");
        sb.append("VALUES(");

        for (var col : table.columns()) {
            if (!values.containsKey(col)) {
                throw new IllegalArgumentException("No value for column: " + col);
            }
            Object val = values.get(col);
            if (val instanceof String) {
                String valStr = (String) val;
                sb.append("'");
                sb.append(DataFile.escaceString(valStr));
                sb.append("'");
            } else {
                sb.append(val);
            }
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");

        return sb.toString();
    }
}
