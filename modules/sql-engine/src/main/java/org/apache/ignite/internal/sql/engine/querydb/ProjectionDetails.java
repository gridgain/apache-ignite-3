package org.apache.ignite.internal.sql.engine.querydb;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;

public class ProjectionDetails {

    private final List<ExprDetails> expressions = new ArrayList<>();

    ProjectionDetails(IgniteProject rel) {
        for (var expr : rel.getProjects()) {
            ExprDetails exprDetails = ExprDetails.tryFrom(expr);
            if (exprDetails != null) {
                expressions.add(exprDetails);
            }
        }
    }

    public List<ExprDetails> expressions() {
        return expressions;
    }
}
