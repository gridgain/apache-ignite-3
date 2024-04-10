package org.apache.ignite.internal.sql.engine.rule;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;

public class HashJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    public static final RelOptRule INSTANCE = new HashJoinConverterRule();

    /**
     * Creates a converter.
     */
    public HashJoinConverterRule() {
        super(LogicalJoin.class, "HashJoinConverter");
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin logicalJoin = call.rel(0);

        RexShuttle rexShuttle = new RexShuttle() {
            @Override public RexNode visitCall(RexCall call) {
                if (call.getOperator().getKind() != SqlKind.EQUALS && call.getOperator().getKind() != SqlKind.AND) {
                    throw Util.FoundOne.NULL;
                }
                return super.visitCall(call);
            }
        };

        try {
            rexShuttle.apply(logicalJoin.getCondition());
        } catch (ControlFlowException ex) {
            return false;
        }

        return !nullOrEmpty(logicalJoin.analyzeCondition().pairs())
                && logicalJoin.analyzeCondition().isEqui();
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        return new IgniteHashJoin(cluster, outTraits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }
}
