package org.apache.ignite.internal.sql.engine.querydb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.Nullable;

public class ExprDetails {

    private final String kind;

    private final String opName;

    private final int numOperands;

    private final int numNodes;

    private final TypeDetails type;

    private final List<OperandDetails> operands;

    private final String str;

    public static @Nullable ExprDetails tryFrom(RexNode node) {
        if (node instanceof RexCall) {
            return new ExprDetails((RexCall) node);
        } else if (node instanceof RexLiteral) {
            return new ExprDetails((RexLiteral) node);
        } else {
            return null;
        }
    }

    public static ExprDetails column(String name, RelDataType input) {
        return new ExprDetails(name, input);
    }

    private ExprDetails(RexCall call) {
        this.kind = call.getKind().sql;
        this.opName = call.getOperator().getName();
        this.type = TypeDetails.getType(call.getType());
        this.operands = new ArrayList<>();
        this.str = call.toString();
        for (int i = 0; i < call.operandCount(); i++) {
            RexNode operandNode = call.operands.get(i);
            operands.add(new OperandDetails(i, operandNode));
        }
        this.numOperands = operands.size();
        this.numNodes = call.nodeCount();
    }

    private ExprDetails(String name, RelDataType type) {
        this.kind = "COL";
        this.opName = null;
        this.numOperands = 0;
        this.type = TypeDetails.getType(type);
        this.str = name;
        this.operands = Collections.emptyList();
        this.numNodes = 1;
    }

    private ExprDetails(RexLiteral literal) {
        this.kind = literal.getKind().sql;
        this.opName = null;
        this.numOperands = 0;
        this.type = TypeDetails.getType(literal.getType());
        this.str = "literal";
        this.operands = Collections.emptyList();
        this.numNodes = literal.nodeCount();
    }

    public String kind() {
        return kind;
    }

    public String operatorName() {
        return opName;
    }

    public TypeDetails type() {
        return type;
    }

    public String str() {
        return str;
    }

    public int numOperands() {
        return numOperands;
    }

    public int numNodes() {
        return numNodes;
    }

    public List<OperandDetails> operands() {
        return operands;
    }

    public static class OperandDetails {

        private final int index;

        private final String kind;

        private final TypeDetails type;

        OperandDetails(int index, RexNode node) {
            this.index = index;
            this.kind = node.getKind().sql;
            this.type = TypeDetails.getType(node.getType());
        }

        public int index() {
            return index;
        }

        public String kind() {
            return kind;
        }

        public TypeDetails type() {
            return type;
        }
    }
}
