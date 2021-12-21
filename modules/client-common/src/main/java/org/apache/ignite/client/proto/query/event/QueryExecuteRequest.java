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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute request.
 */
public class QueryExecuteRequest implements ClientMessage {
    /** Schema name. */
    private String schemaName;

    /** Fetch size. */
    private int pageSize;

    /** Max rows. */
    private int maxRows;

    /** Sql query. */
    private String sqlQry;

    /** Sql query arguments. */
    private Object[] args;

    /**
     * Default constructor. For deserialization purposes.
     */
    public QueryExecuteRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Cache name.
     * @param pageSize   Fetch size.
     * @param maxRows    Max rows.
     * @param sqlQry     SQL query.
     * @param args       Arguments list.
     */
    public QueryExecuteRequest(String schemaName, int pageSize, int maxRows, String sqlQry, Object[] args) {

        this.schemaName = schemaName == null || schemaName.isEmpty() ? null : schemaName;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.sqlQry = sqlQry;
        this.args = args;
    }

    /**
     * Get the page size.
     *
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Get the max rows.
     *
     * @return Max rows.
     */
    public int maxRows() {
        return maxRows;
    }

    /**
     * Get the sql query.
     *
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /**
     * Get the arguments.
     *
     * @return Sql query arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * Get the schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packString(schemaName);
        packer.packInt(pageSize);
        packer.packInt(maxRows);
        packer.packString(sqlQry);

        packer.packObjectArray(args);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = unpacker.unpackString();
        pageSize = unpacker.unpackInt();
        maxRows = unpacker.unpackInt();
        sqlQry = unpacker.unpackString();

        args = unpacker.unpackObjectArray();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(QueryExecuteRequest.class, this);
    }
}
