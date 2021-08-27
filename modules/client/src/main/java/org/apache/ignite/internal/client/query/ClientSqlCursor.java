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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.query.IgniteQueryErrorCode;
import org.apache.ignite.client.proto.query.event.JdbcClientMessage;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;
import org.apache.ignite.client.proto.query.event.JdbcResponse;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryType;

/**
 *
 */
public class ClientSqlCursor implements SqlCursor<List<?>> {
    /** Remote channel. */
    private final ReliableChannel ch;

    /** Query real type. */
    private final SqlQueryType type;

    /** Fetch size. */
    private final int fetchSize;

    /** Rows. */
    private List<List<Object>> rows;

    /** Cursor id. */
    private final long cursorId;

    /** Initialized cursor. */
    private boolean init;

    /** Current row. */
    private int curRow;

    /** Last. */
    private boolean last;

    /**
     * @param ch Channel.
     * @param cursorId CursorId.
     */
    public ClientSqlCursor(ReliableChannel ch, long cursorId, SqlQueryType type, int fetchSize) {
        this.ch = ch;
        this.cursorId = cursorId;
        this.type = type;
        this.fetchSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override public SqlQueryType getQueryType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        var req = new JdbcQueryCloseRequest(cursorId);
        var res = new JdbcQueryCloseResult();

        this.sendRequest(ClientOp.SQL_CURSOR_CLOSE, req, res);

        if (res.status() != JdbcResponse.STATUS_SUCCESS)
            throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
    }

    /** {@inheritDoc} */
    @Override public Iterator<List<?>> iterator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        initIfNeeded();

        return !(rows.size() == curRow && last);
    }

    /** {@inheritDoc} */
    @Override public List<?> next() {
        initIfNeeded();

        if (curRow == rows.size())
            fetchBatch();

        if (!hasNext())
            throw new NoSuchElementException();

        List<Object> row = rows.get(curRow);

        curRow++;

        return row;
    }

    /**
     *
     */
    private void initIfNeeded() {
        if (init)
            return;

        fetchBatch();

        init = true;
    }

    /**
     *
     */
    private void fetchBatch() {
        if (last)
            return;

        var req = new JdbcQueryFetchRequest(cursorId, fetchSize);
        var res = new JdbcQueryFetchResult();

        sendRequest(ClientOp.SQL_NEXT, req, res);

        if (res.status() != JdbcResponse.STATUS_SUCCESS)
            throw IgniteQueryErrorCode.createJdbcSqlRuntimeException(res.err(), res.status());

        this.rows = res.items();
        this.last = res.last();
        this.curRow = 0;
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
}
