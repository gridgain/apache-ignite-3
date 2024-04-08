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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.util.RexUtils.replaceInputRefs;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.util.RexUtils.InputRefReplacer;
import org.jetbrains.annotations.Nullable;

/**
 * NestedLoopJoinNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class NestedLoopJoinNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    protected final BiPredicate<RowT, RowT> cond;

    protected final RowHandler<RowT> handler;

    protected int requested;

    protected int waitingLeft;

    protected int waitingRight;

    protected final List<RowT> rightMaterialized = new ArrayList<>(inBufSize);

    protected final Deque<RowT> leftInBuf = new ArrayDeque<>(inBufSize);

    protected boolean inLoop;

    Map<Object, Map> hashMap = new HashMap<>();

    Map<Integer, Integer> joinCondPos = new LinkedHashMap<>();

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx  Execution context.
     * @param cond Join expression.
     */
    private NestedLoopJoinNode(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
        super(ctx);

        this.cond = cond;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::doJoin, this::onError);
        }
    }

    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        rightMaterialized.clear();
        leftInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx == 0) {
            return new Downstream<>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };
        } else if (idx == 1) {
            return new Downstream<>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    private void pushLeft(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft--;

        leftInBuf.add(row);

        join();
    }

    private void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        rightMaterialized.add(row);

        Map<Object, Map> next = hashMap;
        int filled = 0;
        for (Map.Entry <Integer, Integer> entry : joinCondPos.entrySet()) {
            filled++;
            Object ent = handler.get(entry.getValue(), row);
            if (filled == joinCondPos.size()) {
                Map raw = next.computeIfAbsent(ent, k -> new HashMap<>());
                raw.put(row, null);
            } else {
                next = next.computeIfAbsent(ent, k -> new HashMap<>());
            }
        }

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = NOT_WAITING;

        join();
    }

    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = NOT_WAITING;

        join();
    }

    Node<RowT> leftSource() {
        return sources().get(0);
    }

    Node<RowT> rightSource() {
        return sources().get(1);
    }

    protected abstract void join() throws Exception;

    /**
     * Create.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> NestedLoopJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, BiPredicate<RowT, RowT> cond, RexNode cond0) {

        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, cond);

            case LEFT: {
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftJoin<>(ctx, cond, rightRowFactory, cond0, leftRowType);
            }

            case RIGHT: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightJoin<>(ctx, cond, leftRowFactory);
            }

            case FULL: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterJoin<>(ctx, cond, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                return new SemiJoin<>(ctx, cond);

            case ANTI:
                return new AntiJoin<>(ctx, cond);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private InnerJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
            super(ctx, cond);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            RowT row = handler.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    private static void buildJoinPositions(RexNode cond, Map<Integer, Integer> joinCondPos, RelDataType leftRowType) {
        RexShuttle rexShuttle = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef inputRef) {
                return inputRef;
            }

            @Override public RexNode visitCall(RexCall call) {
                if (call.getOperator().getKind() == SqlKind.EQUALS) {
                    List<RexNode> node = call.getOperands();
                    RexInputRef n1 = (RexInputRef) node.get(0);
                    RexInputRef n2 = (RexInputRef) node.get(1);
                    joinCondPos.put(n1.getIndex(), n2.getIndex() - leftRowType.getFieldCount());
                }
                return super.visitCall(call);
            }
        };

        rexShuttle.apply(cond);
    }

    private static class LeftJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         * @param rightRowFactory Right row factory.
         */
        private LeftJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowHandler.RowFactory<RowT> rightRowFactory,
                RexNode cond0,
                RelDataType leftRowType
        ) {
            super(ctx, cond);

            this.rightRowFactory = rightRowFactory;

            buildJoinPositions(cond0, joinCondPos, leftRowType);
        }

        @Override
        protected void rewindInternal() {
            matched = false;
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
/*        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            matched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            for (RowT right : rightMaterialized) {
                                BinaryTuple btl = handler.toBinaryTuple(left);
                                BinaryTuple btr = handler.toBinaryTuple(right);
                                @Nullable Object ho = handler.get(0, right);
                                int comp = btl.byteBuffer().compareTo(btr.byteBuffer());
                                System.err.println(comp);
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            matched = true;

                            RowT row = handler.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!matched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            if (matched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }*/

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {

                        System.err.println("!!!!join");
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        List<RowT> rightRows = lookup(left, handler);

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

                        left = null; // !!!!
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            System.err.println(requested + " " + waitingLeft + " " + waitingRight + " " + left + " " + leftInBuf.isEmpty());

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }

        private <RowT> List<RowT> lookup(RowT row, RowHandler<RowT> handler) {
            Map<Object, Map> next = hashMap;
            for (Map.Entry <Integer, Integer> entry : joinCondPos.entrySet()) {
                Object ent = handler.get(entry.getKey(), row);
                System.err.println("lookup: " + ent);
                next = next.get(ent);
                if (next == null) {
                    return Collections.emptyList();
                }
            }

            return next.keySet().stream().map(k -> (RowT) k).collect(Collectors.toList());
        }
    }

    private static class RightJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        private @Nullable BitSet rightNotMatchedIndexes;

        private int lastPushedInd;

        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         * @param leftRowFactory Left row factory.
         */
        private RightJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowHandler.RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, cond);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightNotMatchedIndexes = null;
            lastPushedInd = 0;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = handler.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    private static class FullOuterJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean leftMatched;

        private @Nullable BitSet rightNotMatchedIndexes;

        private int lastPushedInd;

        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RowHandler.RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, cond);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            leftMatched = false;
            rightNotMatchedIndexes = null;
            lastPushedInd = 0;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = handler.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!leftMatched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    private static class SemiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private SemiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
            super(ctx, cond);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    boolean matched = false;

                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        checkState();

                        if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                            continue;
                        }

                        requested--;
                        downstream().push(left);

                        matched = true;
                    }

                    if (matched || rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty()) {
                downstream().end();
                requested = 0;
            }
        }
    }

    private static class AntiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private RowT left;

        private int rightIdx;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private AntiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
            super(ctx, cond);
        }

        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (!matched && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (cond.test(left, rightMaterialized.get(rightIdx++))) {
                                matched = true;
                            }
                        }

                        if (!matched) {
                            requested--;
                            downstream().push(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }
}
