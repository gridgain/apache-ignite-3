/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.query;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.query.IgniteQueryErrorCode;
import org.apache.ignite.client.proto.query.event.JdbcClientMessage;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQuerySingleResult;
import org.apache.ignite.client.proto.query.event.JdbcResponse;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;

/**
 *
 */
public class ClientQueryProcessor implements QueryProcessor {
    /** Default page size. */
    private static final int DEFAULT_PAGE_SIZE = 10;

    /** Default rows count. */
    private static final int DEFAULT_ROWS_CNT = 10;

    /** Remote channel. */
    private final ReliableChannel ch;

    /** Page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** Max rows. */
    private int maxRows = DEFAULT_ROWS_CNT;

    /**
     * @param ch Channel.
     */
    public ClientQueryProcessor(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        var req = new JdbcQueryExecuteRequest(schemaName,
            pageSize,
            maxRows,
            false,
            false,
            qry,
            params
        );

        var res = new JdbcQueryExecuteResult();

        sendRequest(ClientOp.SQL_EXEC, req, res);

        if (res.status() != JdbcResponse.STATUS_SUCCESS)
            throw IgniteQueryErrorCode.createJdbcSqlRuntimeException(res.err(), res.status());

        for (JdbcQuerySingleResult result : res.results()) {
            if (result.status() != JdbcResponse.STATUS_SUCCESS)
                throw IgniteQueryErrorCode.createJdbcSqlRuntimeException(result.err(), result.status());
        }

        return res.results().stream()
            .map(e -> new ClientSqlCursor(ch, e.cursorId(), SqlQueryType.getQryType(e.type()), 10))
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void start() {

    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {

    }

    /**
     * Send JdbcClientMessage request to server size and reads JdbcClientMessage result.
     *
     * @param opCode Operation code.
     * @param req JdbcClientMessage request.
     * @param res JdbcClientMessage result.
     */
    private void sendRequest(int opCode, JdbcClientMessage req, JdbcClientMessage res) {
        ch.serviceAsync(opCode, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        }).join();
    }

    /**
     * @param pageSize New page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @param maxRows New max rows.
     */
    public void maxRows(int maxRows) {
        this.maxRows = maxRows;
    }
}
