package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.extension.api.InternalTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

public class MyInternalTableDescriptor extends NullInitializerExpressionFactory
        implements InternalTableDescriptor {
    /** */
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];
    
    /** */
    private final ColumnDescriptor[] descriptors;
    
    /** */
    private final Map<String, ColumnDescriptor> descriptorsMap;
    
    /** */
    private final ImmutableBitSet insertFields;
    
    /** */
    private final ImmutableBitSet keyFields;
    
    /** */
    public MyInternalTableDescriptor(
            List<ColumnDescriptor> columnDescriptors
    ) {
        ImmutableBitSet.Builder keyFieldsBuilder = ImmutableBitSet.builder();
        
        Map<String, ColumnDescriptor> descriptorsMap = new HashMap<>(columnDescriptors.size());
        for (ColumnDescriptor descriptor : columnDescriptors) {
            descriptorsMap.put(descriptor.name(), descriptor);
            
            if (descriptor.key())
                keyFieldsBuilder.set(descriptor.fieldIndex());
        }
        
        this.descriptors = columnDescriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;
        
        insertFields = ImmutableBitSet.range(columnDescriptors.size());
        keyFields = keyFieldsBuilder.build();
    }
    
    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, insertFields);
    }
    
    /** {@inheritDoc} */
    @Override public RelDataType deleteRowType(IgniteTypeFactory factory) {
        return rowType(factory, keyFields);
    }
    
    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return !descriptors[colIdx].key();
    }
    
    /** {@inheritDoc} */
    @Override public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);
        
        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++)
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }
        else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1))
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }
    
        RelDataType build = b.build();
        return factory.createStructType(build.getFieldList());
    }
    
    /** {@inheritDoc} */
    @Override public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }
}
