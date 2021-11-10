package org.apache.ignite.internal.processors.query.calcite.extension.api;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

public interface InternalTableDescriptor extends RelProtoDataType, InitializerExpressionFactory {
    /** {@inheritDoc} */
    @Override default RelDataType apply(RelDataTypeFactory factory) {
        return rowType((IgniteTypeFactory)factory, null);
    }
    
    /**
     * Returns row type excluding effectively virtual fields.
     *
     * @param factory Type factory.
     * @return Row type for INSERT operation.
     */
    default RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, null);
    }
    
    /**
     * Returns row type containing only key fields.
     *
     * @param factory Type factory.
     * @return Row type for DELETE operation.
     */
    default RelDataType deleteRowType(IgniteTypeFactory factory) {
        return rowType(factory, null);
    }
    
    /**
     * Returns row type including effectively virtual fields.
     *
     * @param factory Type factory.
     * @return Row type for SELECT operation.
     */
    default RelDataType selectForUpdateRowType(IgniteTypeFactory factory) {
        return rowType(factory, null);
    }
    
    /**
     * Returns row type.
     *
     * @param factory Type factory.
     * @param usedColumns Participating columns numeration.
     * @return Row type.
     */
    RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns);
    
    /**
     * Checks whether is possible to update a column with a given index.
     *
     * @param tbl Parent table.
     * @param colIdx Column index.
     * @return {@code True} if update operation is allowed for a column with a given index.
     */
    boolean isUpdateAllowed(RelOptTable tbl, int colIdx);
    
    /**
     * Returns column descriptor for given field name.
     *
     * @return Column descriptor
     */
    ColumnDescriptor columnDescriptor(String fieldName);
}
