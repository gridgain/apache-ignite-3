package org.apache.ignite.internal.sql.engine.querydb;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.jetbrains.annotations.Nullable;

public class TableScanDetails {

    private final List<ExprDetails> expressions = new ArrayList<>();

    private final String tableName;

    private final ExprDetails condition;

    TableScanDetails(IgniteTableScan rel) {
        RelOptTable table = rel.getTable();
        assert table != null;

        this.tableName = String.join(".", table.getQualifiedName());

        if (rel.projects() != null) {
            for (var expr : rel.projects()) {
                ExprDetails exprDetails = ExprDetails.tryFrom(expr);
                if (exprDetails != null) {
                    expressions.add(exprDetails);
                } else if (expr instanceof RexLocalRef) {
                    RexLocalRef ref = (RexLocalRef) expr;
                    String fieldName = rel.getRowType().getFieldNames().get(ref.getIndex());
                    expressions.add(ExprDetails.column(fieldName, ref.getType()));
                }
            }
        } else {
            for (var field : rel.getRowType().getFieldList()) {
                expressions.add(ExprDetails.column(field.getName(), field.getType()));
            }
        }

        if (rel.condition() != null) {
            condition = ExprDetails.tryFrom(rel.condition());
        } else {
            condition = null;
        }
    }

    public String tableName() {
        return tableName;
    }

    public List<ExprDetails> expressions() {
        return expressions;
    }

    public @Nullable ExprDetails condition() {
        return condition;
    }
}
