package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.FilterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.extension.api.SqlExtensionPlugin;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;

public class SqlExtensionPluginImpl implements SqlExtensionPlugin {
    public static volatile List<String> allNodes;

    private static final String ONLY_TABLE_NAME = "TEST_TBL";

    /** {@inheritDoc} */
    @Override public void init(SchemaUpdateListener updateListener) {
        SchemaPlus schema = Frameworks.createRootSchema(false);
        schema.add("CUSTOM_SCHEMA", new SchemaImpl());
        updateListener.onSchemaUpdated(schema);
    }

    /** {@inheritDoc} */
    @Override public Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase) {
        if (phase != PlannerPhase.OPTIMIZATION)
            return Set.of();

        return Set.of(FilterConverterRule.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "MY_PLUGIN";
    }

    /** {@inheritDoc} */
    @Override public <Row> NodeImplementor<Row> implementor() {
        return new NodeImplementor<>() {
            @Override public <T extends RelNode> Node<Row> implement(ExecutionContext<Row> ctx, T node) {
                if (node instanceof MyPhysFilter)
                    return implement(ctx, (MyPhysFilter) node);

                if (node instanceof MyPhysTableScan)
                    return implement(ctx, (MyPhysTableScan) node);

                assert false;

                return null;
            }

            private Node<Row> implement(ExecutionContext<Row> ctx, MyPhysTableScan scan) {
                RowHandler.RowFactory<Row> factory = ctx.rowHandler().factory(ctx.getTypeFactory(), scan.getRowType());

                return new ScanNode<>(
                    ctx, scan.getRowType(), List.of(
                        factory.create(1, UUID.randomUUID().toString()),
                        factory.create(2, UUID.randomUUID().toString()),
                        factory.create(3, UUID.randomUUID().toString()),
                        factory.create(4, UUID.randomUUID().toString()),
                        factory.create(5, UUID.randomUUID().toString()),
                        factory.create(6, UUID.randomUUID().toString())
                    )
                );
            }

            private Node<Row> implement(ExecutionContext<Row> ctx, MyPhysFilter filter) {
                Predicate<Row> pred = ctx.expressionFactory().predicate(filter.getCondition(), filter.getRowType());

                FilterNode<Row> node = new FilterNode<>(ctx, filter.getRowType(), pred);

                Node<Row> input = implement(ctx, filter.getInput());

                node.register(input);

                return node;
            }
        };
    }

    @Override public ColocationGroup colocationGroup(RelNode node) {
        return ColocationGroup.forNodes(allNodes);
    }

    static class SchemaImpl extends AbstractSchema {
        @Override protected Map<String, Table> getTableMap() {
            return Map.of(ONLY_TABLE_NAME, new OnlyTableImpl());
        }
    }

}
