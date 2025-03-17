package org.apache.ignite.internal.sql.engine.querydb;

import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.jetbrains.annotations.Nullable;

final class RelNodeDetails {

    private final IgniteRel rel;

    private final String kind;

    private final String outputType;

    private final int level;

    private final int childIndex;

    private final int position;

    private final Integer parentId;

    private TableScanDetails tableScan;

    private FunctionScanDetails functionScan;

    private ProjectionDetails projection;

    private JoinDetails join;

    private FilterDetails filter;

    private AggregationDetails aggregation;

    private SortDetails sort;

    RelNodeDetails(IgniteRel rel, int level, int childIndex, int position, Integer parentId) {
        this.level = level;
        this.childIndex = childIndex;
        this.position = position;
        this.outputType = TypeDetails.getType(rel.getRowType()).name();
        this.kind = rel.getRelTypeName();
        this.parentId = parentId;

        if (rel instanceof IgniteTableScan) {
            tableScan = new TableScanDetails((IgniteTableScan) rel);
        } else if (rel instanceof IgniteProject) {
            projection = new ProjectionDetails((IgniteProject) rel);
        } else if (rel instanceof IgniteFilter) {
            filter = new FilterDetails((IgniteFilter) rel);
        } else if (rel instanceof AbstractIgniteJoin) {
            join = new JoinDetails((AbstractIgniteJoin) rel);
        } else if (rel instanceof IgniteAggregate) {
            aggregation = new AggregationDetails((IgniteAggregate) rel);
        } else if (rel instanceof IgniteReduceAggregateBase) {
            aggregation = new AggregationDetails((IgniteReduceAggregateBase) rel);
        } else if (rel instanceof IgniteSort) {
            sort = new SortDetails((IgniteSort) rel);
        } else if (rel instanceof IgniteTableFunctionScan) {
            functionScan = new FunctionScanDetails((IgniteTableFunctionScan) rel);
        }
        this.rel = rel;
    }

    public String kind() {
        return kind;
    }

    public int level() {
        return level;
    }

    public int position() {
        return position;
    }

    public int childIndex() {
        return childIndex;
    }

    public @Nullable Integer internalParentId() {
        return parentId;
    }

    public String outputType() {
        return outputType;
    }

    @Override
    public String toString() {
        return "RelNodeDetails{" +
                "rel=" + rel.getClass().getSimpleName() +
                ", depth=" + level +
                ", position=" + position +
                '}';
    }

    public IgniteRel rel() {
        return rel;
    }

    public TableScanDetails tableScan() {
        return tableScan;
    }

    public ProjectionDetails projection() {
        return projection;
    }

    public JoinDetails join() {
        return join;
    }

    public FilterDetails filter() {
        return filter;
    }

    public AggregationDetails aggregation() {
        return aggregation;
    }

    public SortDetails sort() {
        return sort;
    }

    public FunctionScanDetails function() {
        return functionScan;
    }
}
