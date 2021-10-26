package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;

class OnlyTableImpl implements Table, TranslatableTable {
    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            List.of(typeFactory.createJavaType(Integer.class), typeFactory.createJavaType(String.class)),
            List.of("C1", "C2")
        );
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

    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();

        RelTraitSet traits = cluster.traitSetOf(MyConvention.INSTANCE)
            .replace(IgniteDistributions.broadcast());

        return new MyPhysTableScan(
            context.getCluster(), traits, List.of(), relOptTable
        );
    }
}
