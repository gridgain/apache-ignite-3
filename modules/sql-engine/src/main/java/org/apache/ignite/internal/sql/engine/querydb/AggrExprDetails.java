package org.apache.ignite.internal.sql.engine.querydb;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class AggrExprDetails {

    private final int numOperands;

    private final String kind;

    private final TypeDetails type;

    private final boolean distinct;

    private final List<AggregateOperand> operands = new ArrayList<>();

    private final String str;

    AggrExprDetails(AggregateCall call, RelDataType inputType) {
        this.kind = call.getAggregation().kind.sql;
        this.type = TypeDetails.getType(call.getType());
        this.numOperands = call.getArgList().size();
        this.distinct = call.isDistinct();
        this.str = call.toString();

        int id = 0;

        if (call.distinctKeys != null) {
            for (Ord<Integer> key : Ord.zip(call.distinctKeys)) {
                RelDataTypeField field = inputType.getFieldList().get(key.getValue());
                TypeDetails fieldType = TypeDetails.getType(field.getType());

                operands.add(new AggregateOperand(id, key.i, null, fieldType));
                id ++;
            }
        }

        for (Ord<Integer> index : Ord.zip(call.getArgList())) {
            RelDataTypeField field = inputType.getFieldList().get(index.getValue());
            TypeDetails fieldType = TypeDetails.getType(field.getType());

            operands.add(new AggregateOperand(id, null, index.i, fieldType));
            id ++;
        }
    }

    public String kind() {
        return kind;
    }

    public String str() {
        return str;
    }

    public TypeDetails type() {
        return type;
    }

    public int numOperands() {
        return numOperands;
    }

    public boolean distinct() {
        return distinct;
    }

    public List<AggregateOperand> operands() {
        return operands;
    }

    public static class AggregateOperand {

        private final Integer distinctKey;

        private final int index;

        private final TypeDetails type;

        private final int id;

        public AggregateOperand(int id, Integer distinctKey, Integer index, TypeDetails type) {
            this.id = id;
            this.distinctKey = distinctKey;
            this.index = index;
            this.type = type;
        }

        public int id() {
            return id;
        }

        public Integer distinctKey() {
            return distinctKey;
        }

        public Integer index() {
            return index;
        }

        public TypeDetails type() {
            return type;
        }
    }
}
