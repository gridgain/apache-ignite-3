/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.calcite.util.Static.RESOURCE;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;
import org.jetbrains.annotations.Nullable;

/** Validator. */
public class IgniteSqlValidator extends SqlValidatorImpl {
    /** Decimal of Integer.MAX_VALUE for fetch/offset bounding. */
    private static final BigDecimal DEC_INT_MAX = BigDecimal.valueOf(Integer.MAX_VALUE);

    private static final int MAX_LENGTH_OF_ALIASES = 256;

    private static final Set<SqlKind> HUMAN_READABLE_ALIASES_FOR;

    static {
        EnumSet<SqlKind> kinds = EnumSet.noneOf(SqlKind.class);

        kinds.addAll(SqlKind.AGGREGATE);
        kinds.addAll(SqlKind.BINARY_ARITHMETIC);
        kinds.addAll(SqlKind.FUNCTION);

        kinds.add(SqlKind.CEIL);
        kinds.add(SqlKind.FLOOR);
        kinds.add(SqlKind.LITERAL);

        kinds.add(SqlKind.PROCEDURE_CALL);

        HUMAN_READABLE_ALIASES_FOR = Collections.unmodifiableSet(kinds);
    }

    /** Dynamic parameters. */
    private final Object[] parameters;

    /**
     * Creates a validator.
     *
     * @param opTab         Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory   Type factory
     * @param config        Config
     * @param parameters    Dynamic parameters
     */
    public IgniteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader,
            IgniteTypeFactory typeFactory, SqlValidator.Config config, Object[] parameters) {
        super(opTab, catalogReader, typeFactory, config);

        this.parameters = parameters;
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode validate(SqlNode topNode) {
        // Calcite fails to validate a query when its top node is EXPLAIN PLAN FOR
        // java.lang.NullPointerException: namespace for <query>
        // at org.apache.calcite.sql.validate.SqlValidatorImpl.getNamespaceOrThrow(SqlValidatorImpl.java:1280)
        if (topNode instanceof SqlExplain) {
            SqlExplain explainNode = (SqlExplain) topNode;
            SqlNode topNodeToValidate = explainNode.getExplicandum();

            SqlNode validatedNode = super.validate(topNodeToValidate);
            explainNode.setOperand(0, validatedNode);
            return explainNode;
        } else {
            return super.validate(topNode);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validateInsert(SqlInsert insert) {
        if (insert.getTargetColumnList() == null) {
            insert.setOperand(3, inferColumnList(insert));
        }

        super.validateInsert(insert);
    }

    /** {@inheritDoc} */
    @Override
    public void validateUpdate(SqlUpdate call) {
        validateUpdateFields(call);

        super.validateUpdate(call);
    }

    /** {@inheritDoc} */
    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() != SqlTypeName.DECIMAL) {
            super.validateLiteral(literal);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier) call.getTargetTable();
        final SqlValidatorTable table = getCatalogReader().getTable(targetTable.names);

        if (table == null) {
            // TODO IGNITE-14865 Calcite exception should be converted/wrapped into a public ignite exception.
            throw newValidationError(call.getTargetTable(), RESOURCE.objectNotFound(targetTable.toString()));
        }

        SqlIdentifier alias = call.getAlias() != null ? call.getAlias() :
                new SqlIdentifier(deriveAlias(targetTable, 0), SqlParserPos.ZERO);

        table.unwrap(IgniteTable.class).descriptor().selectForUpdateRowType((IgniteTypeFactory) typeFactory)
                .getFieldNames().stream()
                .map(name -> alias.plus(name, SqlParserPos.ZERO))
                .forEach(selectList::add);

        int ordinal = 0;
        // Force unique aliases to avoid a duplicate for Y with SET X=Y
        for (SqlNode exp : call.getSourceExpressionList()) {
            selectList.add(SqlValidatorUtil.addAlias(exp, SqlUtil.deriveAliasFromOrdinal(ordinal++)));
        }

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                    SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void addToSelectList(List<SqlNode> list, Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
        if (includeSystemVars || exp.getKind() != SqlKind.IDENTIFIER || !isSystemFieldName(deriveAlias(exp, 0))) {
            super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlIdentifier targetTable = (SqlIdentifier) call.getTargetTable();
        final SqlValidatorTable table = getCatalogReader().getTable(targetTable.names);

        if (table == null) {
            // TODO IGNITE-14865 Calcite exception should be converted/wrapped into a public ignite exception.
            throw newValidationError(targetTable, RESOURCE.objectNotFound(targetTable.toString()));
        }

        table.unwrap(IgniteTable.class).descriptor().deleteRowType((IgniteTypeFactory) typeFactory)
                .getFieldNames().stream()
                .map(name -> new SqlIdentifier(name, SqlParserPos.ZERO))
                .forEach(selectList::add);

        SqlNode sourceTable = call.getTargetTable();

        if (call.getAlias() != null) {
            sourceTable =
                    SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple());
        }

        return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
                call.getCondition(), null, null, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        checkIntegerLimit(select.getFetch(), "fetch / limit");
        checkIntegerLimit(select.getOffset(), "offset");

        super.validateSelect(select, targetRowType);
    }

    /**
     * Check integer limit.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param n        Node to check limit.
     * @param nodeName Node name.
     */
    private void checkIntegerLimit(SqlNode n, String nodeName) {
        if (n instanceof SqlLiteral) {
            BigDecimal offFetchLimit = ((SqlLiteral) n).bigDecimalValue();

            if (offFetchLimit.compareTo(DEC_INT_MAX) > 0 || offFetchLimit.compareTo(BigDecimal.ZERO) < 0) {
                throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
            }
        } else if (n instanceof SqlDynamicParam) {
            SqlDynamicParam dynamicParam = (SqlDynamicParam) n;
            int idx = dynamicParam.getIndex();
            Object param = parameters[idx];
            if (param instanceof Integer) {
                if ((Integer) param < 0) {
                    throw newValidationError(n, IgniteResource.INSTANCE.correctIntegerLimit(nodeName));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public String deriveAlias(SqlNode node, int ordinal) {
        if (node.isA(HUMAN_READABLE_ALIASES_FOR)) {
            String alias = node.toSqlString(c -> c.withDialect(CalciteSqlDialect.DEFAULT)
                    .withQuoteAllIdentifiers(false)
                    .withAlwaysUseParentheses(false)
                    .withClauseStartsLine(false)
            ).getSql();

            return alias.substring(0, Math.min(alias.length(), MAX_LENGTH_OF_ALIASES));
        }

        return super.deriveAlias(node, ordinal);
    }

    /** {@inheritDoc} */
    @Override
    public void validateAggregateParams(SqlCall aggCall,
            @Nullable SqlNode filter, @Nullable SqlNodeList distinctList,
            @Nullable SqlNodeList orderList, SqlValidatorScope scope) {
        validateAggregateFunction(aggCall, (SqlAggFunction) aggCall.getOperator());

        super.validateAggregateParams(aggCall, filter, null, orderList, scope);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        RelDataType dataType = super.deriveType(scope, expr);

        if (!(expr instanceof SqlCall)) {
            return dataType;
        }

        SqlKind sqlKind = expr.getKind();
        // See the comments below.
        // IgniteCustomType: at the moment we allow operations, involving custom data types, that are binary comparison to fail at runtime.
        if (!SqlKind.BINARY_COMPARISON.contains(sqlKind)) {
            return dataType;
        }
        // Comparison and arithmetic operators are SqlCalls.
        SqlCall sqlCall = (SqlCall) expr;
        // IgniteCustomType: We only handle binary operations here.
        var lhs = getValidatedNodeType(sqlCall.operand(0));
        var rhs = getValidatedNodeType(sqlCall.operand(1));

        if (lhs.getSqlTypeName() != SqlTypeName.ANY && rhs.getSqlTypeName() != SqlTypeName.ANY) {
            return dataType;
        }

        // IgniteCustomType:
        //
        // The correct way to implement the validation error bellow would be to move these coercion rules to SqlOperandTypeChecker:
        // 1) Implement the SqlOperandTypeChecker that prohibit arithmetic operations
        // between types that neither support binary/unary operators nor support type coercion.
        // 2) Specify that SqlOperandTypeChecker for every binary/unary operator defined in
        // IgniteSqlOperatorTable
        //
        // This would allow to reject plans that contain type errors
        // at the validation stage.
        //
        // Similar approach can also be used to handle casts between types that can not
        // be converted into one another.
        //
        // And if applied to dynamic parameters with combination of a modified SqlOperandTypeChecker for
        // a cast function this would allow to reject plans with invalid values w/o at validation stage.
        //
        var customTypeCoercionRules = typeFactory().getCustomTypeCoercionRules();
        boolean canConvert;

        // IgniteCustomType: To enable implicit casts to a custom data type.
        if (SqlTypeUtil.equalSansNullability(typeFactory(), lhs, rhs)) {
            // We can always perform binary comparison operations between instances of the same type.
            canConvert = true;
        } else if (lhs instanceof IgniteCustomType) {
            canConvert = customTypeCoercionRules.needToCast(rhs, (IgniteCustomType) lhs);
        } else if (rhs instanceof IgniteCustomType) {
            canConvert = customTypeCoercionRules.needToCast(lhs, (IgniteCustomType) rhs);
        } else {
            // We should not get here because at least one operand type must be a IgniteCustomType
            // and only custom data types must use SqlTypeName::ANY.
            throw new AssertionError("At least one operand must be a custom data type: " + expr);
        }

        if (!canConvert) {
            var ex = RESOURCE.invalidTypesForComparison(
                    lhs.getFullTypeString(), sqlKind.sql, rhs.getFullTypeString());

            throw SqlUtil.newContextException(expr.getParserPosition(), ex);
        } else {
            return dataType;
        }
    }


    /** {@inheritDoc} */
    @Override
    protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
        // Workaround for https://issues.apache.org/jira/browse/CALCITE-4923
        if (node instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) node;

            if (select.getFrom() instanceof SqlJoin) {
                boolean hasStar = false;

                for (SqlNode expr : select.getSelectList()) {
                    if (expr instanceof SqlIdentifier && ((SqlIdentifier) expr).isStar()
                            && ((SqlIdentifier) expr).names.size() == 1) {
                        hasStar = true;
                    }
                }

                performJoinRewrites((SqlJoin) select.getFrom(), hasStar);
            }
        }

        return super.performUnconditionalRewrites(node, underFrom);
    }

    /** Rewrites JOIN clause if required. */
    private void performJoinRewrites(SqlJoin join, boolean hasStar) {
        if (join.getLeft() instanceof SqlJoin) {
            performJoinRewrites((SqlJoin) join.getLeft(), hasStar || join.isNatural());
        }

        if (join.getRight() instanceof SqlJoin) {
            performJoinRewrites((SqlJoin) join.getRight(), hasStar || join.isNatural());
        }

        // Join with USING should be rewriten if SELECT conatins "star" in projects, NATURAL JOIN also has other issues
        // and should be rewritten in any case.
        if (join.isNatural() || (join.getConditionType() == JoinConditionType.USING && hasStar)) {
            // Default Calcite validator can't expand "star" for NATURAL joins and joins with USING if some columns
            // of join sources are filtered out by the addToSelectList method, and the count of columns in the
            // selectList not equals to the count of fields in the corresponding rowType. Since we do filtering in the
            // addToSelectList method (exclude _KEY and _VAL columns), to workaround the expandStar limitation we can
            // wrap each table to a subquery. In this case columns will be filtered out on the subquery level and
            // rowType of the subquery will have the same cardinality as selectList.
            join.setLeft(rewriteTableToQuery(join.getLeft()));
            join.setRight(rewriteTableToQuery(join.getRight()));
        }
    }

    /** Wrap table to subquery "SELECT * FROM table". */
    private SqlNode rewriteTableToQuery(SqlNode from) {
        SqlNode src = from.getKind() == SqlKind.AS ? ((SqlCall) from).getOperandList().get(0) : from;

        if (src.getKind() == SqlKind.IDENTIFIER || src.getKind() == SqlKind.TABLE_REF) {
            String alias = deriveAlias(from, 0);

            SqlSelect expandedQry = new SqlSelect(SqlParserPos.ZERO, null,
                    SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)), src, null, null, null,
                    null, null, null, null, null);

            return SqlValidatorUtil.addAlias(expandedQry, alias);
        } else {
            return from;
        }
    }

    private void validateAggregateFunction(SqlCall call, SqlAggFunction aggFunction) {
        if (!SqlKind.AGGREGATE.contains(aggFunction.kind)) {
            throw newValidationError(call,
                    IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));
        }

        switch (aggFunction.kind) {
            case COUNT:
                if (call.operandCount() > 1) {
                    throw newValidationError(call, RESOURCE.invalidArgCount(aggFunction.getName(), 1));
                }

                return;
            case SUM:
            case AVG:
            case MIN:
            case MAX:
            case ANY_VALUE:

                return;
            default:
                throw newValidationError(call,
                        IgniteResource.INSTANCE.unsupportedAggregationFunction(aggFunction.getName()));
        }
    }

    private SqlNodeList inferColumnList(SqlInsert call) {
        final SqlValidatorTable table = table(validatedNamespace(call, unknownType));

        if (table == null) {
            return null;
        }

        final TableDescriptor desc = table.unwrap(TableDescriptor.class);

        if (desc == null) {
            return null;
        }

        final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);

        for (RelDataTypeField field : desc.insertRowType(typeFactory()).getFieldList()) {
            columnList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
        }

        return columnList;
    }

    private void validateUpdateFields(SqlUpdate call) {
        if (call.getTargetColumnList() == null) {
            return;
        }

        final SqlValidatorNamespace ns = validatedNamespace(call, unknownType);

        final SqlValidatorTable table = table(ns);

        if (table == null) {
            return;
        }

        final TableDescriptor desc = table.unwrap(TableDescriptor.class);

        if (desc == null) {
            return;
        }

        final RelDataType baseType = table.getRowType();
        final RelOptTable relOptTable = relOptTable(ns);

        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier) node;

            RelDataTypeField target = SqlValidatorUtil.getTargetField(
                    baseType, typeFactory(), id, getCatalogReader(), relOptTable);

            if (target == null) {
                throw newValidationError(id,
                        RESOURCE.unknownTargetColumn(id.toString()));
            }

            if (!desc.isUpdateAllowed(relOptTable, target.getIndex())) {
                throw newValidationError(id,
                        IgniteResource.INSTANCE.cannotUpdateField(id.toString()));
            }
        }
    }

    private SqlValidatorTable table(SqlValidatorNamespace ns) {
        RelOptTable relOptTable = relOptTable(ns);

        if (relOptTable != null) {
            return relOptTable.unwrap(SqlValidatorTable.class);
        }

        return ns.getTable();
    }

    private RelOptTable relOptTable(SqlValidatorNamespace ns) {
        return SqlValidatorUtil.getRelOptTable(
                ns, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    }

    private SqlValidatorNamespace validatedNamespace(SqlNode node, RelDataType targetType) {
        SqlValidatorNamespace ns = getNamespace(node);
        validateNamespace(ns, targetType);
        return ns;
    }

    private IgniteTypeFactory typeFactory() {
        return (IgniteTypeFactory) typeFactory;
    }

    private boolean isSystemFieldName(String alias) {
        return Commons.implicitPkEnabled() && Commons.IMPLICIT_PK_COL_NAME.equals(alias);
    }

    /** {@inheritDoc} */
    @Override
    protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlDynamicParam) {
            SqlDynamicParam dynamicParam = (SqlDynamicParam) node;
            RelDataType type = inferDynamicParamType(dynamicParam);

            boolean narrowType = inferredType.equals(unknownType) || SqlTypeUtil.canCastFrom(inferredType, type, true)
                    && SqlTypeUtil.comparePrecision(inferredType.getPrecision(), type.getPrecision()) > 0;

            if (narrowType) {
                setValidatedNodeType(node, type);

                return;
            }
        }

        if (node instanceof SqlCall) {
            SqlValidatorScope newScope = scopes.get(node);

            if (newScope != null) {
                scope = newScope;
            }

            SqlCall call = (SqlCall) node;
            SqlOperandTypeInference operandTypeInference = call.getOperator().getOperandTypeInference();
            SqlOperandTypeChecker operandTypeChecker = call.getOperator().getOperandTypeChecker();
            SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
            List<SqlNode> operands = callBinding.operands();
            RelDataType[] operandTypes = new RelDataType[operands.size()];

            Arrays.fill(operandTypes, unknownType);

            if (operandTypeInference != null && !(call instanceof SqlCase)) {
                operandTypeInference.inferOperandTypes(callBinding, inferredType, operandTypes);
            } else if (operandTypeChecker instanceof FamilyOperandTypeChecker) {
                // Infer operand types from checker for dynamic parameters if it's possible.
                FamilyOperandTypeChecker checker = (FamilyOperandTypeChecker) operandTypeChecker;

                for (int i = 0; i < checker.getOperandCountRange().getMax(); i++) {
                    if (i >= operandTypes.length) {
                        break;
                    }

                    SqlTypeFamily family = checker.getOperandSqlTypeFamily(i);
                    RelDataType type = family.getDefaultConcreteType(typeFactory());

                    if (type != null && operands.get(i) instanceof SqlDynamicParam) {
                        operandTypes[i] = type;
                    }
                }
            }

            for (int i = 0; i < operands.size(); ++i) {
                SqlNode operand = operands.get(i);

                if (operand != null) {
                    inferUnknownTypes(operandTypes[i], scope, operand);
                }
            }
        } else {
            super.inferUnknownTypes(inferredType, scope, node);
        }
    }

    private RelDataType inferDynamicParamType(SqlDynamicParam dynamicParam) {
        RelDataType parameterType;

        Object param = parameters[dynamicParam.getIndex()];
        // IgniteCustomType: first we must check whether dynamic parameter is a custom data type.
        // If so call createCustomType with appropriate arguments.
        if (param instanceof UUID) {
            parameterType =  typeFactory().createCustomType(UuidType.NAME);
        } else if (param != null) {
            parameterType = typeFactory().toSql(typeFactory().createType(param.getClass()));
        } else {
            parameterType = typeFactory().createSqlType(SqlTypeName.NULL);
        }

        // Dynamic parameters are nullable.
        return typeFactory().createTypeWithNullability(parameterType, true);
    }
}
