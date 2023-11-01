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

package org.apache.ignite.client.handler.requests.tx;

import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.tracing.NoopSpan;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Client transaction commit request.
 */
public class ClientTransactionCommitRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param resources Resources.
     * @param metrics   Metrics.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics)
            throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();

        var tx = resources.remove(resourceId).get(InternalTransaction.class);
        var parent = tx == null ? NoopSpan.INSTANCE : tx.traceSpan();

        return asyncSpan("ClientTransactionCommitRequest.process", parent, (span) -> {
            return tx.commitAsync().whenComplete((res, err) -> metrics.transactionsActiveDecrement());
        });
    }
}
