package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.extension.api.InternalTable;
import org.apache.ignite.internal.processors.query.calcite.extension.api.InternalTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
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
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return desc.rowType((IgniteTypeFactory) typeFactory, null);
    }
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return null;
    }
    
    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return new Statistic() {
            @Override public @Nullable List<RelCollation> getCollations() {
                return List.of();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    /** {@inheritDoc} */
    @Override public boolean isRolledUp(String column) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
        return false;
    }

    @Override public TableScan toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();

        RelTraitSet traits = cluster.traitSetOf(MyConvention.INSTANCE)
            .replace(IgniteDistributions.broadcast());

        return new MyPhysTableScan(
            context.getCluster(), traits, List.of(), relOptTable
        );
    }
    
    @Override
    public IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        return null;
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
