package org.apache.ignite.internal.processors.query.calcite.extension.api;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;

public interface SqlExtensionPlugin {
    String name();

    void init(
        SchemaUpdateListener schemaUpdateListener
    );

    Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase);

    <Row> NodeImplementor<Row> implementor();

    ColocationGroup colocationGroup(RelNode node);

    interface NodeImplementor<Row> {
        <T extends RelNode> Node<Row> implement(ExecutionContext<Row> ctx, T node);
    }

    interface SchemaUpdateListener {
        void onSchemaUpdated(Schema schema);
    }
}
