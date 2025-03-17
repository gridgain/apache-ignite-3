package org.apache.ignite.internal.sql.engine.querydb;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;

public class AggregationDetails {

    private final List<AggrExprDetails> expressions = new ArrayList<>();

    private final List<IntSet> groupingSets = new ArrayList<>();

    AggregationDetails(IgniteAggregate rel) {
        for (var call : rel.getAggCallList()) {
            expressions.add(new AggrExprDetails(call, rel.getInput().getRowType()));
        }
        for (var group : rel.groupSets) {
            IntSet groupSet = new IntOpenHashSet(group.size());
            for (int groupKey : group) {
                groupSet.add(groupKey);
            }
            groupingSets.add(groupSet);
        }
    }

    AggregationDetails(IgniteReduceAggregateBase rel) {
        for (var call : rel.getAggregateCalls()) {
            expressions.add(new AggrExprDetails(call, rel.getInput().getRowType()));
        }
    }

    public List<AggrExprDetails> aggregates() {
        return expressions;
    }

    public List<IntSet> groupingSets() {
        return groupingSets;
    }
}
/*
select
n.stmt_id, n.node_kind,
 'R ' || r.table_name as right_table,
 'R_ID ' || r.node_id::VARCHAR as right_id,

 'L ' || l.table_name as left_table,
 'L_ID ' || l.node_id::VARCHAR as left_id,

e.*, eo.* from nodes n
join exprs e using (node_id)
join expr_operands eo using (expr_id)
join nodes l on l.stmt_id = n.stmt_id
join nodes r on r.stmt_id = n.stmt_id
where expr_output_type = 'BOOLEAN' and expr_index = -1 and
r.node_level = n.node_level + 1 and r.node_pos = 1 and
l.node_level = n.node_level + 1 and l.node_pos = 0;
 */
