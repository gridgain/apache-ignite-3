package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

public abstract class HashJoinNode<RowT> extends AbstractRightMaterializedJoinNode<RowT> {
    Map<Object, Object> hashStore = new Object2ObjectOpenHashMap<>();
    protected final RowHandler<RowT> handler;

    final Collection<Integer> leftJoinPositions;
    private final Collection<Integer> rightJoinPositions;

    boolean touchResults;

    Iterator<RowT> rightIt = Collections.emptyIterator();

    private HashJoinNode(ExecutionContext<RowT> ctx, JoinInfo joinInfo, boolean touch) {
        super(ctx);

        handler = ctx.rowHandler();
        touchResults = touch;

        leftJoinPositions = joinInfo.leftKeys.toIntegerList();
        rightJoinPositions = joinInfo.rightKeys.toIntegerList();
    }

    void getMoreOrEnd() throws Exception {
        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }

        if (waitingLeft == 0 && leftInBuf.isEmpty()) {
            leftSource().request(waitingLeft = inBufSize);
        }

        //System.err.println("requested: " + requested + " " + waitingLeft + " " + waitingRight + " " + leftInBuf.isEmpty() + " " + left);

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty() && left == null) {
            requested = 0;
            downstream().end();
            //System.err.println("end join");
        }
    }

    @Override
    protected void rewindInternal() {
        rightIt = Collections.emptyIterator();

        hashStore.clear();

        super.rewindInternal();
    }

    public static <RowT> HashJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, JoinInfo joinInfo) {

        switch (joinType) {
            case INNER:
                return new InnerHashJoin<>(ctx, joinInfo);

            case LEFT: {
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftHashJoin<>(ctx, rightRowFactory, joinInfo);
            }

            case RIGHT: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightHashJoin<>(ctx, leftRowFactory, joinInfo);
            }

            case FULL: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterHashJoin<>(ctx, leftRowFactory, rightRowFactory, joinInfo);
            }

            case SEMI:
                return new SemiHashJoin<>(ctx, joinInfo);

            case ANTI:
                return new AntiHashJoin<>(ctx, joinInfo);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        private InnerHashJoin(ExecutionContext<RowT> ctx, JoinInfo joinInfo) {
            super(ctx, joinInfo, false);
            System.err.println("!!!call: InnerHashJoin");
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
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
                JoinInfo joinInfo
        ) {
            super(ctx, joinInfo, false);

            this.rightRowFactory = rightRowFactory;
            //System.err.println("!!!call: LeftHashJoin");
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
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
                JoinInfo joinInfo
        ) {
            super(ctx, joinInfo, true);

            this.leftRowFactory = leftRowFactory;
            System.err.println("!!!call: RightHashJoin");
        }

        @Override
        protected void rewindInternal() {
            HashJoinNode.resetTouched(hashStore);

            super.rewindInternal();
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (left == null && !rightIt.hasNext() && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING
                    && waitingRight == NOT_WAITING && requested > 0) {
                List<RowT> res = getUntouched(hashStore, null);

                for (RowT right : res) {
                    RowT row = handler.concat(leftRowFactory.create(), right);
                    --requested;

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
                JoinInfo joinInfo
        ) {
            super(ctx, joinInfo, true);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;

            System.err.println("!!!call: FullOuterHashJoin");
        }

        @Override
        protected void rewindInternal() {
            HashJoinNode.resetTouched(hashStore);

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (left == null && !rightIt.hasNext() && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING
                    && waitingRight == NOT_WAITING && requested > 0) {
                List<RowT> res = getUntouched(hashStore, null);

                for (RowT right : res) {
                    RowT row = handler.concat(leftRowFactory.create(), right);

                    --requested;

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
        private SemiHashJoin(ExecutionContext<RowT> ctx, JoinInfo joinInfo) {
            super(ctx, joinInfo, false);

            System.err.println("!!!call: SemiHashJoin");
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    checkState();

                    left = leftInBuf.remove();

                    Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                    if (!rightRows.isEmpty()) {
                        requested--;

                        downstream().push(left);

                        if (requested == 0) {
                            break;
                        }
                    }

                    left = null;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class AntiHashJoin<RowT> extends HashJoinNode<RowT> {
        //TODO:!!
        private AntiHashJoin(ExecutionContext<RowT> ctx, JoinInfo joinInfo) {
            super(ctx, joinInfo, false);

            System.err.println("!!!call: AntiHashJoin");
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    checkState();

                    left = leftInBuf.remove();

                    Collection<RowT> rightRows = lookup(left, handler, hashStore, leftJoinPositions, touchResults);

                    if (rightRows.isEmpty()) {
                        requested--;

                        downstream().push(left);

                        if (requested == 0) {
                            break;
                        }
                    }

                    left = null;
                }
            }

            getMoreOrEnd();
        }
    }

    private static <RowT> Collection<RowT> lookup(
            RowT row,
            RowHandler<RowT> handler,
            Map<Object, Object> hashMap,
            Collection<Integer> leftJoinPositions,
            boolean processTouched
    ) {
        Map<Object, Object> next = hashMap;
        int processed = 0;
        Collection<RowT> coll = Collections.emptyList();

        for (Integer entry : leftJoinPositions) {
            Object ent = handler.get(entry, row);

            if (ent == null) {
                return Collections.emptyList();
            }

            Object next0 = next.get(ent);

            if (next0 == null) {
                return Collections.emptyList();
            }

            processed++;
            if (processed == leftJoinPositions.size()) {
                coll = (Collection<RowT>) next.get(ent);

                if (processTouched) {
                    ((TouchedList<RowT>) coll).touched = true;
                }
            } else {
                next = (Map<Object, Object>) next0;
            }
        }

        return coll;
    }

    private static <RowT> List<RowT> getUntouched(Map<Object, Object> entries, @Nullable List<RowT> out) {
        if (out == null) {
            out = new ArrayList<>();
        }

        for (Map.Entry<Object, Object> ent : entries.entrySet()) {
            if (ent.getValue() instanceof Collection) {
                TouchedList<RowT> coll = (TouchedList<RowT>) ent.getValue();
                if (!coll.touched) {
                    out.addAll(coll);
                }
            } else {
                getUntouched((Map<Object, Object>) ent.getValue(), out);
            }
        }
        return out;
    }

    private static <RowT> void resetTouched(Map<Object, Object> entries) {
        for (Map.Entry<Object, Object> ent : entries.entrySet()) {
            if (ent.getValue() instanceof Collection) {
                TouchedList<RowT> coll = (TouchedList<RowT>) ent.getValue();
                if (coll.touched) {
                    coll.touched = false;
                }
            } else {
                resetTouched((Map<Object, Object>) ent.getValue());
            }
        }
    }

    @Override
    protected void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        Map<Object, Object> next = hashStore;
        int processed = 0;
        for (Integer entry : rightJoinPositions) {
            processed++;
            Object ent = handler.get(entry, row);
            if (processed == rightJoinPositions.size()) {
                Collection<RowT> raw = touchResults
                        ? (Collection<RowT>) next.computeIfAbsent(ent, k -> new TouchedList<>())
                        : (Collection<RowT>) next.computeIfAbsent(ent, k -> new ArrayList<>());

                raw.add(row);
            } else {
                next = (Object2ObjectOpenHashMap<Object, Object>) next.computeIfAbsent(ent, k -> new Object2ObjectOpenHashMap<>());
            }
        }

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    private static class TouchedList<E> extends ArrayList<E> {
        boolean touched = false;
    }
}
