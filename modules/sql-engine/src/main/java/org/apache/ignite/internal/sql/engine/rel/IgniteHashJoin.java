package org.apache.ignite.internal.sql.engine.rel;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Relational operator that represent hash join algo.
 */
public class IgniteHashJoin extends AbstractIgniteJoin {
    private static final String REL_TYPE_NAME = "HashJoin";

    public IgniteHashJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
            RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    public IgniteHashJoin(RelInput input) {
        this(input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInputs().get(0),
                input.getInputs().get(1),
                input.getExpression("condition"),
                Set.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
                input.getEnum("joinType", JoinRelType.class));
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double rowCount = 0;
        double leftRowCount = mq.getRowCount(getLeft());
        double rightRowCount = mq.getRowCount(getRight());

        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount;
        } else {
            rowCount += leftRowCount;
        }

        if (Double.isInfinite(rightRowCount)) {
            rowCount = rightRowCount;
        } else {
            // believe in some kind of keys equality
            rowCount += Util.nLogN(rightRowCount);
        }

        double rightSize = rightRowCount * (getRight().getRowType().getFieldCount() * 8) * IgniteCost.AVERAGE_FIELD_SIZE;

        return costFactory.makeCost(rowCount, rowCount * IgniteCost.ROW_PASS_THROUGH_COST, 0, rightSize, 0);
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
            boolean semiJoinDone) {
        return new IgniteHashJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteHashJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
                getVariablesSet(), getJoinType());
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
