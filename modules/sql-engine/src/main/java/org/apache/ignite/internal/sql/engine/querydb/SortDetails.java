package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.ignite.internal.sql.engine.rel.IgniteSort;

public class SortDetails {

    private final String collation;

    SortDetails(IgniteSort rel) {
        StringBuilder sb = new StringBuilder();
        for (var field : rel.collation.getFieldCollations()) {
            sb.append(field.getFieldIndex());
            sb.append(" ");
            sb.append(field.getDirection());
        }

        this.collation = sb.toString();
    }

    public String collation() {
        return collation;
    }
}
