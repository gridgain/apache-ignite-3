package org.apache.ignite.internal.sql.engine.querydb;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.Ord;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.querydb.DbRecords.StatementRecord;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

public class QueryDetailsCollector {

    private static final IgniteLogger LOG = Loggers.forClass(QueryDetailsCollector.class);

    public static QueryDetailsCollector INSTANCE = new QueryDetailsCollector(true);

    private final AtomicInteger stmtId = new AtomicInteger(1);
    private final AtomicInteger nodeId = new AtomicInteger(1);
    private final AtomicInteger exprId = new AtomicInteger(1);
    private final AtomicInteger aggId = new AtomicInteger(1);

    private final DbSchema schema = new DbSchema();
    private final DbRecords records = new DbRecords();

    private final boolean writeStatementsPeriodically;

    public QueryDetailsCollector() {
        this(false);
    }

    private QueryDetailsCollector(boolean writeStatementsPeriodically) {
        this.writeStatementsPeriodically = writeStatementsPeriodically;
    }

    public synchronized void collect(String query, MultiStepPlan plan) {
        RelInfoCollector collector = new RelInfoCollector();
        collector.skipExchange = true;

        plan.root().accept(collector);

        StatementRecord stmt = records.newStatementRecord();

        stmt.add(schema.statements, Map.of(
                "STMT_ID", stmtId.get(),
                "STMT_KIND", plan.type().name(),
                "STMT_STR", query,
                "STMT_NUM_DYNAMIC_PARAMS", plan.parameterMetadata().parameterTypes().size()
        ));

        Int2IntMap assignedNodeIds = new Int2IntOpenHashMap();

        for (var node : collector.treeDetails) {
            String tableName = null;

            if (node.tableScan() != null) {
                tableName = node.tableScan().tableName();
            }

            HashMap<String, Object> nodeAttrs = new HashMap<>(Map.of(
                    "STMT_ID", stmtId.get(),
                    "NODE_ID", nodeId.get(),
                    "NODE_KIND", node.kind(),
                    "NODE_TYPE", node.outputType(),
                    "NODE_LEVEL", node.level(),
                    "NODE_INDEX", node.childIndex(),
                    "NODE_POS", node.position()
            ));
            nodeAttrs.put("TABLE_NAME", tableName);

            Integer internalParentId = node.internalParentId();
            if (internalParentId != null) {
                int parentNodeId = assignedNodeIds.getOrDefault((int) internalParentId, -1);
                if (parentNodeId == -1) {
                    throw new IllegalArgumentException();
                }
                nodeAttrs.put("NODE_PARENT_ID", parentNodeId);
            } else {
                nodeAttrs.put("NODE_PARENT_ID", null);
            }

            stmt.add(schema.nodes, nodeAttrs);

            insertNodeDetails(stmt, node);

            assignedNodeIds.put(node.rel().getId(), nodeId.get());

            nodeId.incrementAndGet();
        }

        for (var ord : Ord.zip(plan.parameterMetadata().parameterTypes())) {
            TypeDetails paramType = TypeDetails.getParamType(ord.e);
            Map<String, Object> paramAttrs = Map.of(
                    "STMT_ID", stmtId.get(),
                    "DYN_PARAM_INDEX", ord.i,
                    "DYN_PARAM_TYPE", paramType.name(),
                    "DYN_PARAM_TYPE_PRECISION", paramType.precision(),
                    "DYN_PARAM_TYPE_SCALE", paramType.scale()
            );
            stmt.add(schema.dynamicParams, paramAttrs);
        }

        stmtId.incrementAndGet();
        records.addStatement(stmt);

        if (writeStatementsPeriodically) {
            writeTo(new DataFile(schema, records));
        }
    }

    public synchronized void writeTo(DataFile dataFile) {
        if (records.statements().size() > 100) {
            int count = records.statements().size();

            dataFile.dumpToSql(StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            records.clear();

            LOG.warn("Written {} statements", count);
        }
    }

    public synchronized void dump() {
        DataFile dataFile = new DataFile(schema, records);
        dataFile.dumpToSql(StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    public synchronized DbSchema schema() {
        return schema;
    }

    public synchronized DbRecords records() {
        return records;
    }

    private void insertNodeDetails(DbRecords.StatementRecord stmt, RelNodeDetails node) {
        // Projection
        if (node.projection() != null) {
            ProjectionDetails projection = node.projection();

            for (var ord : Ord.zip(projection.expressions())) {
                insertExpr(stmt, ord.getValue(),  nodeId.get(), exprId, ord.i);
            }
        }

        // Join
        if (node.join() != null) {
            JoinDetails join = node.join();
            insertExpr(stmt, join.condition(), nodeId.get(), exprId, -1);
        }

        // Aggregation
        if (node.aggregation() != null) {
            AggregationDetails aggregation = node.aggregation();

            for (var ord : Ord.zip(aggregation.groupingSets())) {
                for (int key : ord.getValue()) {
                    Map<String, Object> aggRoupAttrs = Map.of(
                            "NODE_ID", nodeId.get(),
                            "GROUP_INDEX", ord.i,
                            "GROUP_KEY", key
                    );
                    stmt.add(schema.aggGroups, aggRoupAttrs);
                }
            }

            for (var ord : Ord.zip(aggregation.aggregates())) {
                insertAggExpr(stmt, ord.getValue(), nodeId.get(), aggId, ord.i);
            }
        }

        // Filter
        if (node.filter() != null) {
            FilterDetails filter = node.filter();
            insertExpr(stmt, filter.condition(),  nodeId.get(), exprId, -1);
        }

        if (node.sort() != null) {
            SortDetails sort = node.sort();
//            insertExpr(stmt, );
        }

        // Table scan
        if (node.tableScan() != null) {
            TableScanDetails scan = node.tableScan();

            for (var ord : Ord.zip(scan.expressions())) {
                insertExpr(stmt, ord.getValue(),  nodeId.get(), exprId, ord.i);
            }

            ExprDetails condition = scan.condition();
            if (condition != null) {
                insertExpr(stmt, condition, nodeId.get(), exprId, -1);
            }
        }

        // Function scan
        if (node.function() != null) {
            FunctionScanDetails functionScan = node.function();
            insertExpr(stmt, functionScan.function(), nodeId.get(), exprId, -1);
        }
    }

    private void insertExpr(DbRecords.StatementRecord stmt, ExprDetails expr, int nodeId, AtomicInteger exprId, int index) {
        HashMap<String, Object> exprAttrs = new HashMap<>(Map.of(
                "NODE_ID", nodeId,
                "EXPR_ID", exprId.get(),
                "EXPR_INDEX", index,
                "EXPR_KIND", expr.kind(),
                "EXPR_STR", expr.str(),
                "EXPR_TYPE", expr.type().name(),
                "EXPR_TYPE_PRECISION", expr.type().precision(),
                "EXPR_TYPE_SCALE", expr.type().scale(),
                "EXPR_NUM_OPERANDS", expr.numOperands(),
                "EXPR_NUM_NODES", expr.numNodes()
        ));
        exprAttrs.put("EXPR_NAME", expr.operatorName());
        stmt.add(schema.exprs, exprAttrs);

        for (var operand : expr.operands()) {
            Map<String, Object> exprOpAttrs = Map.of(
                    "EXPR_ID", exprId.get(),
                    "OP_INDEX", operand.index(),
                    "OP_KIND", operand.kind(),
                    "OP_TYPE", operand.type().name(),
                    "OP_TYPE_PRECISION", operand.type().precision(),
                    "OP_TYPE_SCALE", operand.type().scale()
            );
            stmt.add(schema.exprOperands, exprOpAttrs);
        }

        exprId.incrementAndGet();
    }

    private void insertAggExpr(DbRecords.StatementRecord stmt, AggrExprDetails expr, int nodeId, AtomicInteger exprId, int index) {
        Map<String, Object> aggExprAttrs = Map.of(
                "NODE_ID", nodeId,
                "AGG_ID", exprId.get(),
                "AGG_INDEX", index,
                "AGG_KIND", expr.kind(),
                "AGG_STR", expr.str(),
                "AGG_TYPE", expr.type().name(),
                "AGG_TYPE_PRECISION", expr.type().precision(),
                "AGG_TYPE_SCALE", expr.type().scale(),
                "AGG_NUM_OPERANDS", expr.numOperands(),
                "AGG_DISTINCT", expr.distinct()
        );
        stmt.add(schema.aggExprs, aggExprAttrs);

        for (var operand : expr.operands()) {
            Map<String, Object> aggExprOpAttrs = new HashMap<>();
            aggExprOpAttrs.put("AGG_ID", exprId.get());
            aggExprOpAttrs.put("AGG_OP_ID", operand.id());
            aggExprOpAttrs.put("AGG_OP_DISTINCT_KEY", operand.distinctKey());
            aggExprOpAttrs.put("AGG_OP_INDEX", operand.index());
            aggExprOpAttrs.put("AGG_OP_TYPE", operand.type().name());
            aggExprOpAttrs.put("AGG_OP_TYPE_PRECISION", operand.type().precision());
            aggExprOpAttrs.put("AGG_OP_TYPE_SCALE", operand.type().scale());
            stmt.add(schema.aggExprOperands, aggExprOpAttrs);
        }

        exprId.incrementAndGet();
    }

    private static class RelInfoCollector extends IgniteRelShuttle {

        private final RelTreeDetails treeDetails = new RelTreeDetails();

        private final ArrayDeque<Integer> stack = new ArrayDeque<>();

        private boolean skipExchange = true;

        private int childIndex = 0;

        private Integer parentId;

        void dump(IgniteRel rel) {
            rel.accept(this);

            System.err.println("Levels:");

            for (int i = 0; i < treeDetails.depth(); i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("level: ").append(i).append(" pos: ");
                int pos = 0;
                for (var node : treeDetails.level(i)) {
                    sb.append(pos)
                            .append(" ")
                            .append(node.rel().getClass().getSimpleName())
                            .append(" index: ").append(node.childIndex())
                            .append(" position: ").append(node.position())
                            .append(" inputs: ")
                            .append(node.rel().getInputs().size())
                            .append(" ");
                    pos += 1;
                }
                System.err.println(sb);
            }
        }

        @Override
        protected IgniteRel processNode(IgniteRel rel) {
            collectData(chooseRel(rel));
            return rel;
        }

        private void collectData(IgniteRel rel) {
            if (stack.isEmpty()) {
                stack.add(0);
            }

            List<IgniteRel> inputs = Commons.cast(rel.getInputs());
            int level = stack.getLast();

            treeDetails.addNode(rel, level, childIndex, parentId);

            for (int i = 0; i < inputs.size(); i++) {
                stack.add(level + 1);
                try {
                    childIndex = i;
                    parentId = rel.getId();
                    IgniteRel child = inputs.get(i);
                    visit(child);
                } finally {
                    childIndex = 0;
                    parentId = null;
                    stack.pop();
                }
            }
        }

        private IgniteRel chooseRel(IgniteRel rel) {
            if (!skipExchange) {
                return rel;
            }
            while (rel instanceof IgniteExchange) {
                // TODO: Treat sort only exchange as Sort.
                rel = (IgniteRel) rel.getInputs().get(0);
            }
            if (rel == null) {
                throw new IllegalStateException("Exchange operator has no non-Exchange input operators");
            }
            return rel;
        }
    }
}
