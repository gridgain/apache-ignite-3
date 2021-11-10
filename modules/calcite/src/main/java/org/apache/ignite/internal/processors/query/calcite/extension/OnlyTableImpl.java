package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.extension.api.InternalTable;
import org.apache.ignite.internal.processors.query.calcite.extension.api.InternalTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

class OnlyTableImpl implements InternalTable {
    private InternalTableDescriptor desc;
    
    OnlyTableImpl(
            InternalTableDescriptor desc) {
        this.desc = desc;
    }
    
    @Override
    public InternalTableDescriptor descriptor() {
        return desc;
    }
    
    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }
    
    @Override public TableScan toRel(RelOptCluster cluster, RelOptTable relOptTable) {
        RelTraitSet traits = cluster.traitSetOf(MyConvention.INSTANCE)
            .replace(IgniteDistributions.broadcast());

        return new MyPhysTableScan(
            cluster, traits, List.of(), relOptTable
        );
    }
    
    @Override
    public IgniteDistribution distribution() {
        return IgniteDistributions.random();
    }
    
    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        if (aClass.isInstance(desc))
            return aClass.cast(desc);
    
        return null;
    }
}
