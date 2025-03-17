package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;

public class FunctionScanDetails {

    private final ExprDetails expr;

    public FunctionScanDetails(IgniteTableFunctionScan scan) {
        RexNode call = scan.getCall();

        this.expr = ExprDetails.tryFrom(call);
        assert expr != null;
    }

    public ExprDetails function() {
        return expr;
    }
}
