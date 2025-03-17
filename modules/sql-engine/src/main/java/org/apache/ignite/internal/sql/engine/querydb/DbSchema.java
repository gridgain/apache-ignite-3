package org.apache.ignite.internal.sql.engine.querydb;

import java.util.List;

public final class DbSchema {

    public final DbTable statements = DbTable.builder("STATEMENTS")
            .addColumn("STMT_ID", "INT")
            .addColumn("STMT_STR", "VARCHAR")
            .addColumn("STMT_KIND", "VARCHAR")
            .addColumn("STMT_NUM_DYNAMIC_PARAMS", "INT")
            .primaryKey("STMT_ID")
            .build();

    public final DbTable plans = DbTable.builder("PLANS")
            .addColumn("STMT_ID", "INT")
            .addColumn("PLAN_STR", "VARCHAR")
            .primaryKey("STMT_ID")
            .build();

    public final DbTable nodes = DbTable.builder("NODES")
            .addColumn("STMT_ID", "INT")
            .addColumn("NODE_ID", "INT")
            .addColumn("NODE_KIND", "VARCHAR")
            .addColumn("NODE_TYPE", "VARCHAR")
            .addColumn("NODE_LEVEL", "INT")
            .addColumn("NODE_INDEX", "INT")
            .addColumn("NODE_POS", "INT")
            .addColumn("NODE_PARENT_ID", "INT")
            .addColumn("TABLE_NAME", "VARCHAR")
            .primaryKey("NODE_ID")
            .build();

    public final DbTable exprs = DbTable.builder("EXPRS")
            .addColumn("NODE_ID", "INT")
            .addColumn("EXPR_ID", "INT")
            .addColumn("EXPR_INDEX", "INT")
            .addColumn("EXPR_KIND", "VARCHAR")
            .addColumn("EXPR_STR", "VARCHAR")
            .addColumn("EXPR_NAME", "VARCHAR")
            .addColumn("EXPR_TYPE", "VARCHAR")
            .addColumn("EXPR_TYPE_PRECISION", "INT")
            .addColumn("EXPR_TYPE_SCALE", "INT")
            .addColumn("EXPR_NUM_OPERANDS", "INT")
            .addColumn("EXPR_NUM_NODES", "INT")
            .primaryKey("EXPR_ID")
            .build();

    public final DbTable exprOperands = DbTable.builder("EXPR_OPERANDS")
            .addColumn("EXPR_ID", "INT")
            .addColumn("OP_INDEX", "INT")
            .addColumn("OP_KIND", "VARCHAR")
            .addColumn("OP_TYPE", "VARCHAR")
            .addColumn("OP_TYPE_PRECISION", "INT")
            .addColumn("OP_TYPE_SCALE", "INT")
            .primaryKey("EXPR_ID", "OP_INDEX")
            .build();

    public final DbTable aggGroups = DbTable.builder("AGG_GROUPS")
            .addColumn("NODE_ID", "INT")
            .addColumn("GROUP_INDEX", "INT")
            .addColumn("GROUP_KEY", "INT")
            .primaryKey("NODE_ID", "GROUP_INDEX", "GROUP_KEY")
            .build();

    public final DbTable aggExprs = DbTable.builder("AGG_EXPRS")
            .addColumn("NODE_ID", "INT")
            .addColumn("AGG_ID", "INT")
            .addColumn("AGG_INDEX", "INT")
            .addColumn("AGG_KIND", "VARCHAR")
            .addColumn("AGG_STR", "VARCHAR")
            .addColumn("AGG_TYPE", "VARCHAR")
            .addColumn("AGG_TYPE_PRECISION", "INT")
            .addColumn("AGG_TYPE_SCALE", "INT")
            .addColumn("AGG_NUM_OPERANDS", "INT")
            .addColumn("AGG_DISTINCT", "BOOLEAN")
            .primaryKey("AGG_ID")
            .build();

    public final DbTable aggExprOperands = DbTable.builder("AGG_EXPR_OPERANDS")
            .addColumn("AGG_ID", "INT")
            .addColumn("AGG_OP_ID", "INT")
            .addColumn("AGG_OP_DISTINCT_KEY", "INT")
            .addColumn("AGG_OP_INDEX", "INT")
            .addColumn("AGG_OP_TYPE", "VARCHAR")
            .primaryKey("AGG_ID", "AGG_OP_ID")
            .build();

    public final DbTable dynamicParams = DbTable.builder("DYNAMIC_PARAMS")
            .addColumn("STMT_ID", "INT")
            .addColumn("DYN_PARAM_INDEX", "INT")
            .addColumn("DYN_PARAM_TYPE", "VARCHAR")
            .addColumn("DYN_PARAM_TYPE_PRECISION", "INT")
            .addColumn("DYN_PARAM_TYPE_SCALE", "INT")
            .primaryKey("STMT_ID", "DYN_PARAM_INDEX")
            .build();

    private final List<DbTable> tables = List.of(
            statements,
            plans,
            nodes,
            exprs,
            exprOperands,
            aggGroups,
            aggExprs,
            aggExprOperands,
            dynamicParams
    );

    public String makeSqlString() {
        StringBuilder sb = new StringBuilder();

        for (var table : tables) {
            sb.append(table.makeSqlDrop());
            sb.append(";");
            sb.append(System.lineSeparator());
        }

        for (var table : tables) {
            sb.append(table.makeSqlCreate());
            sb.append(";");
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }
}
