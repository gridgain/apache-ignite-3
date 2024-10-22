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

package org.apache.ignite.internal.sql.engine.tx;

import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Context that allows to get explicit transaction provided by user or start implicit one.
 */
public interface QueryTransactionContext {
    /** Returns explicit transaction or start implicit one. */
    QueryTransactionWrapper getOrStartImplicit(boolean readOnly);

    /** Updates tracker of latest time observed by client. */
    void updateObservableTime(HybridTimestamp time);

    /** Returns explicit transaction if one was provided by user. */
    @Nullable QueryTransactionWrapper explicitTx();

    /** Register a callback to be invoked when a new implicit transaction is started. */
    void setImplicitTxStartCallback(Consumer<InternalTransaction> callback);
}
