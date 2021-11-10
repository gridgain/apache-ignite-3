package org.apache.ignite.internal.processors.query.calcite.extension.api;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

public interface InternalTable extends TranslatableTable, Wrapper {
    /**
     * @return Table description.
     */
    InternalTableDescriptor descriptor();
    
    
    /** {@inheritDoc} */
    @Override default RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRowType(typeFactory, null);
    }
    
    /**
     * Returns new type according {@code usedClumns} param.
     *
     * @param typeFactory Factory.
     * @param requiredColumns Used columns enumeration.
     */
    RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns);
    
    /** {@inheritDoc} */
    @Override default TableScan toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return toRel(context.getCluster(), relOptTable);
    }
    
    /**
     * Converts table into relational expression.
     *
     * @param cluster Custer.
     * @param relOptTbl Table.
     * @return Table relational expression.
     */
    IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl);
    
    /**
     * @return Table distribution.
     */
    IgniteDistribution distribution();
}
