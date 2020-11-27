package org.apache.ignite.internal.schema;

import org.apache.ignite.schema.SchemaValueColumnBuilder;

public class ValueColumnConfigurationBuilder extends ColumnConfigurationBuilder<SchemaValueColumnBuilder> implements SchemaValueColumnBuilder {
    private final ValueColumnCollectionBuilder parent;

    public ValueColumnConfigurationBuilder(ValueColumnCollectionBuilder parent) {
        this.parent = parent;
    }

    @Override public ValueColumnCollectionBuilder done() {
        parent.addColumn(this);

        return parent;
    }

    @Override protected ValueColumnConfigurationBuilder getThis() {
        return this;
    }
}
