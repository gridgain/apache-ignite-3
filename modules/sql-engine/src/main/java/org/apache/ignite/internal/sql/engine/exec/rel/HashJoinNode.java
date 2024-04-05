package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.BiPredicate;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;

public class HashJoinNode<RowT> extends AbstractNode<RowT> {
    private final BiPredicate<RowT, RowT> cond;
    private final RowHandler<RowT> handler;
    private int requested;
    private final Deque<RowT> leftInBuf = new ArrayDeque<>(inBufSize); // deque ??
    private final List<RowT> rightInBuf = new ArrayList<>(inBufSize);
    private boolean leftReady;
    private int waitingLeft;
    private int waitingRight;
    private static final int NOT_WAITING = -1;
    RelDataType rightRowType; /// only for left fix it

    public HashJoinNode(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, BiPredicate<RowT, RowT> cond) {
        super(ctx);

        this.cond = cond;
        this.rightRowType = rightRowType;
        handler = ctx.rowHandler();
    }

    @Override
    protected void rewindInternal() {

    }

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
                    //HashJoinNode.onError(e);
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
                    //HashJoinNode.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    private void pushLeft(RowT row) throws Exception {
        assert downstream() != null;
        checkState();

        leftInBuf.add(row);

        System.err.println("!!! pushLeft");

        waitingLeft--;

        if (waitingLeft == 0) {
            if (!leftReady) {
                leftSource().request(inBufSize);

                waitingLeft = inBufSize;
            }
        }
    }

    private void pushRight(RowT row) throws Exception {
        assert downstream() != null;

        checkState();

        rightInBuf.add(row);

        System.err.println("!!! pushRight");

        waitingRight--;

        if (leftReady && waitingRight == 0) {
            //context().execute(this::doJoin, this::onError);
        }
    }

    private void doJoin() throws Exception {
        checkState();

        join();
    }

    private void endLeft() throws Exception {
        assert downstream() != null;

        checkState();

        System.err.println("!!! endLeft");

        leftReady = true;

        context().execute(this::doJoin, this::onError);
    }

    private void endRight() throws Exception {
        assert downstream() != null;

        System.err.println("!!! endRight");

        waitingRight = NOT_WAITING;

        if (leftReady) {
            //context().execute(this::doJoin, this::onError);
        }
    }

    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (waitingLeft == 0) {
            leftSource().request(inBufSize);
            System.err.println("!!!! req left");
            waitingLeft = inBufSize;
        }

        if (waitingRight == 0) {
            rightSource().request(inBufSize);
            System.err.println("!!!! req left");
            waitingRight = inBufSize;
        }
    }

    private Node<RowT> leftSource() {
        return sources().get(0);
    }

    private Node<RowT> rightSource() {
        return sources().get(1);
    }

    protected void join() throws Exception {
        System.err.println("start join");
        for (RowT left : leftInBuf) {

            boolean matched = false;

            for (RowT right : rightInBuf) {
                if (!cond.test(left, right)) {
                    continue;
                }

                matched = true;

                RowT row = handler.concat(left, right);
                downstream().push(row);

                requested--;
            }

            if (!matched) {
                requested--;

                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> rightRowFactory = context().rowHandler().factory(rightRowSchema);

                downstream().push(handler.concat(left, rightRowFactory.create()));
            }

            if (requested == 0) {
                break;
            }

            if (waitingRight != NOT_WAITING) {
                rightSource().request(inBufSize);
                waitingRight = inBufSize;
            }
        }

            //BinaryTuple btl = handler.toBinaryTuple(left);
            //int comp = btl.byteBuffer().compareTo(btr.byteBuffer());
            //System.err.println(comp);

        if (waitingRight == NOT_WAITING || requested == 0) {
            downstream().end();
        }
    }
}
