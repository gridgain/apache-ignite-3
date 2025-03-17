package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;

public class JoinDetails {

    private final ExprDetails condition;

    JoinDetails(AbstractIgniteJoin rel) {
        this.condition = ExprDetails.tryFrom(rel.getCondition());
    }

    public ExprDetails condition() {
        return condition;
    }
}
