package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;

public class FilterDetails {

    private final ExprDetails condition;

    FilterDetails(IgniteFilter rel) {
        this.condition = ExprDetails.tryFrom(rel.getCondition());
    }

    public ExprDetails condition() {
        return condition;
    }
}
