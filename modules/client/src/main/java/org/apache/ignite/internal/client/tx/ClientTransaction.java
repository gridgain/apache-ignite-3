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

package org.apache.ignite.internal.client.tx;

import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Client transaction.
 */
public class ClientTransaction implements Transaction {
    /** Open state. */
    private static final int STATE_OPEN = 0;

    /** Committed state. */
    private static final int STATE_COMMITTED = 1;

    /** Rolled back state. */
    private static final int STATE_ROLLED_BACK = 2;

    /** Channel that the transaction belongs to. */
    private final ClientChannel ch;

    /** Transaction id. */
    private final long id;

    /** The future used on repeated commit/rollback. */
    private final AtomicReference<CompletableFuture<Void>> finishFut = new AtomicReference<>();

    /** State. */
    private final AtomicInteger state = new AtomicInteger(STATE_OPEN);

    /** Read-only flag. */
    private final boolean isReadOnly;

    /** External commit callback. */
    private final @Nullable Function<ClientTransaction, CompletableFuture<Void>> externalCommit;

    private final Map<Object, Object> dirtyCache = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     */
    public ClientTransaction(ClientChannel ch, long id, boolean isReadOnly) {
        this(ch, id, isReadOnly, null);
    }

    /**
     * Constructor.
     *
     * @param ch Channel that the transaction belongs to.
     * @param id Transaction id.
     * @param isReadOnly Read only.
     * @param externalCommit Not null for external tx.
     */
    public ClientTransaction(
            ClientChannel ch,
            long id,
            boolean isReadOnly,
            @Nullable Function<ClientTransaction, CompletableFuture<Void>> externalCommit
    ) {
        this.ch = ch;
        this.id = id;
        this.isReadOnly = isReadOnly;
        this.externalCommit = externalCommit;
    }

    /**
     * Gets the id.
     *
     * @return Id.
     */
    public long id() {
        return id;
    }

    /**
     * Gets the associated channel.
     *
     * @return Channel.
     */
    public ClientChannel channel() {
        return ch;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        sync(commitAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
            return finishFut.get();
        }

        setState(STATE_COMMITTED);

        if (externalCommit != null) {
            CompletableFuture<Void> fut = externalCommit.apply(this);

            fut.handle((unused, err) -> {
                ch.serviceAsync(err != null ? ClientOp.TX_ROLLBACK : ClientOp.TX_COMMIT, w -> w.out().packLong(id), r -> null)
                        .handle((res, e) -> finishFut.get().complete(null));

                return null;
            });
        } else {
            ch.serviceAsync(ClientOp.TX_COMMIT, w -> w.out().packLong(id), r -> null).handle((res, e) -> finishFut.get().complete(null));
        }

        return finishFut.get();
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        sync(rollbackAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        if (!finishFut.compareAndSet(null, new CompletableFuture<>())) {
            return finishFut.get();
        }

        setState(STATE_ROLLED_BACK);

        CompletableFuture<Void> mainFinishFut = ch.serviceAsync(ClientOp.TX_ROLLBACK, w -> w.out().packLong(id), r -> null);

        mainFinishFut.handle((res, e) -> finishFut.get().complete(null));

        return mainFinishFut;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Gets the internal transaction from the given public transaction. Throws an exception if the given transaction is not an instance of
     * {@link ClientTransaction}.
     *
     * @param tx Public transaction.
     * @return Internal transaction.
     */
    public static ClientTransaction get(Transaction tx) {
        if (!(tx instanceof ClientTransaction)) {
            throw new IgniteException(INTERNAL_ERR, "Unsupported transaction implementation: '"
                    + tx.getClass()
                    + "'. Use IgniteClient.transactions() to start transactions.");
        }

        ClientTransaction clientTx = (ClientTransaction) tx;

        int state = clientTx.state.get();

        if (state == STATE_OPEN) {
            return clientTx;
        }

        // Match org.apache.ignite.internal.tx.TxState enum:
        String stateStr = state == STATE_COMMITTED ? "COMMITTED" : "ABORTED";

        throw new TransactionException(
                TX_FAILED_READ_WRITE_OPERATION_ERR,
                format("Transaction is already finished [id={}, state={}].", clientTx.id, stateStr));
    }

    private void setState(int state) {
        this.state.compareAndExchange(STATE_OPEN, state);
    }

    public Map<Object, Object> dirtyCache() {
        return dirtyCache;
    }

    public boolean external() {
        return externalCommit != null;
    }
}
