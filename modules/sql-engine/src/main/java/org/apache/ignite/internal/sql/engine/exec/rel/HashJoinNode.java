package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

public abstract class HashJoinNode<RowT> extends NestedLoopJoinNode<RowT> {
    Map<Object, Map> hashMap = new LinkedHashMap<>(); // hash map will be enough ?

    Collection<Integer> leftJoinPositions = new ArrayList<>();
    Collection<Integer> rightJoinPositions = new ArrayList<>();

    private HashJoinNode(ExecutionContext<RowT> ctx, RexNode cond, RelDataType leftRowType) {
        super(ctx, (p1, p2) -> true); // TODO fix it

        buildJoinPositions(cond, leftRowType, leftJoinPositions, rightJoinPositions);
    }

    void getMoreOrEnd() throws Exception {
        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }

        if (waitingLeft == 0 && leftInBuf.isEmpty()) {
            leftSource().request(waitingLeft = inBufSize);
        }

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    @Override
    protected void rewindInternal() {
        hashMap.clear();

        super.rewindInternal();
    }

    public static <RowT> NestedLoopJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, RexNode cond0) {

        switch (joinType) {
            case INNER:
                return new InnerHashJoin<>(ctx, cond0, leftRowType);

            case LEFT: {
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftHashJoin<>(ctx, rightRowFactory, cond0, leftRowType);
            }

            case RIGHT: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightHashJoin<>(ctx, leftRowFactory, cond0, leftRowType);
            }

            case FULL: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterHashJoin<>(ctx, leftRowFactory, rightRowFactory, cond0, leftRowType);
            }

            case SEMI:
                return new SemiHashJoin<>(ctx, cond0, leftRowType);

            case ANTI:
                return new AntiHashJoin<>(ctx, cond0, leftRowType);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        private InnerHashJoin(ExecutionContext<RowT> ctx, RexNode cond0,
                RelDataType leftRowType) {
            super(ctx, cond0, leftRowType);
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        RowT left = leftInBuf.remove();

                        List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                        if (!rightRows.isEmpty()) {
                            for (RowT right : rightRows) {
                                checkState();

                                requested--;
                                RowT row = handler.concat(left, right);
                                downstream().push(row);
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class LeftHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        // TODO : !!!
        private LeftHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> rightRowFactory,
                RexNode cond0,
                RelDataType leftRowType
        ) {
            super(ctx, cond0, leftRowType);

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        checkState();

                        RowT left = leftInBuf.remove();

                        List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                        if (rightRows.isEmpty()) {
                            requested--;
                            downstream().push(handler.concat(left, rightRowFactory.create()));
                        } else {
                            for (RowT r : rightRows) {
                                if (requested == 0) {
                                    break;
                                }

                                requested--;

                                RowT row = handler.concat(left, r);
                                downstream().push(row);
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class RightHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        // TODO !!!
        private RightHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RexNode cond0,
                RelDataType leftRowType
        ) {
            super(ctx, cond0, leftRowType);

            this.leftRowFactory = leftRowFactory;
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        RowT left = leftInBuf.remove();

                        List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                        for (RowT right : rightRows) {
                            checkState();

                            requested--;

                            RowT joined = handler.concat(left, right);
                            downstream().push(joined);
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && requested > 0) {
                List<RowT> res = getUntouched(hashMap, null);

                for (RowT right : res) {
                    RowT row = handler.concat(leftRowFactory.create(), right);
                    requested--;
                    downstream().push(row);

                    if (requested == 0) {
                        break;
                    }
                }
            }

            getMoreOrEnd();
        }
    }

    private static class FullOuterHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        private FullOuterHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RowHandler.RowFactory<RowT> rightRowFactory,
                RexNode cond0,
                RelDataType leftRowType
        ) {
            super(ctx, cond0, leftRowType);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        checkState();

                        RowT left = leftInBuf.remove();

                        List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                        if (rightRows.isEmpty()) {
                            requested--;
                            downstream().push(handler.concat(left, rightRowFactory.create()));
                        } else {
                            for (RowT right : rightRows) {
                                requested--;

                                RowT joined = handler.concat(left, right);
                                downstream().push(joined);
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && requested > 0) {
                List<RowT> res = getUntouched(hashMap, null);

                for (RowT right : res) {
                    RowT row = handler.concat(leftRowFactory.create(), right);
                    requested--;
                    downstream().push(row);

                    if (requested == 0) {
                        break;
                    }
                }
            }

            getMoreOrEnd();
        }
    }

    private static class SemiHashJoin<RowT> extends HashJoinNode<RowT> {
        private SemiHashJoin(
                ExecutionContext<RowT> ctx,
                RexNode cond0,
                RelDataType leftRowType
        ) {
            super(ctx, cond0, leftRowType);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && !leftInBuf.isEmpty()) {
                    checkState();

                    RowT left = leftInBuf.remove();

                    List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                    if (!rightRows.isEmpty()) {
                        requested--;
                        downstream().push(left);
                    }
                }
            }

            getMoreOrEnd();
        }
    }

    private static class AntiHashJoin<RowT> extends HashJoinNode<RowT> {
        //TODO:!!
        private AntiHashJoin(
                ExecutionContext<RowT> ctx,
                RexNode cond0,
                RelDataType leftRowType) {
            super(ctx, cond0, leftRowType);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && !leftInBuf.isEmpty()) {
                    checkState();

                    RowT left = leftInBuf.remove();

                    List<RowT> rightRows = lookup(left, handler, hashMap, leftJoinPositions);

                    if (rightRows.isEmpty()) {
                        requested--;
                        downstream().push(left);
                    }
                }
            }

            getMoreOrEnd();
        }
    }

    private static void buildJoinPositions(RexNode cond, RelDataType leftRowType, Collection<Integer> leftJoinPositions,
            Collection<Integer> rightJoinPositions) {
        RexShuttle rexShuttle = new RexShuttle() {
            @Override public RexNode visitCall(RexCall call) {
                if (call.getOperator().getKind() == SqlKind.EQUALS) {
                    List<RexNode> node = call.getOperands();
                    RexInputRef n1 = (RexInputRef) node.get(0);
                    RexInputRef n2 = (RexInputRef) node.get(1);
                    leftJoinPositions.add(n1.getIndex());
                    rightJoinPositions.add(n2.getIndex() - leftRowType.getFieldCount());
                }
                return super.visitCall(call);
            }
        };

        rexShuttle.apply(cond);
    }

    private static <RowT> List<RowT> lookup(RowT row, RowHandler<RowT> handler, Map<Object, Map> hashMap, Collection<Integer> leftJoinPositions) {
        Map<Object, Map> next = hashMap;
        for (Integer entry : leftJoinPositions) {
            Object ent = handler.get(entry, row);
            next = next.get(ent);
            if (next == null) {
                return Collections.emptyList();
            }
        }

        for (Map.Entry<Object, Map> ent : next.entrySet()) {
            next.put(ent.getKey(), Collections.emptyMap()); // touched
        }

        return next.keySet().stream().map(k -> (RowT) k).collect(Collectors.toList());
    }

    private static <RowT> List<RowT> getUntouched(Map<Object, Map> entries, @Nullable List<RowT> out) {
        if (out == null) {
            out = new ArrayList<>();
        }

        for (Map.Entry<Object, Map> ent : entries.entrySet()) {
            if (ent.getValue() == null) {
                out.add((RowT) ent.getKey());
            } else {
                getUntouched(ent.getValue(), out);
            }
        }
        return out;
    }

    @Override
    protected void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        Map<Object, Map> next = hashMap;
        int processed = 0;
        for (Integer entry : rightJoinPositions) {
            processed++;
            Object ent = handler.get(entry, row);
            if (processed == rightJoinPositions.size()) {
                Map raw = next.computeIfAbsent(ent, k -> new LinkedHashMap<>());
                raw.put(row, null);
            } else {
                next = next.computeIfAbsent(ent, k -> new LinkedHashMap<>());
            }
        }

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }
}
