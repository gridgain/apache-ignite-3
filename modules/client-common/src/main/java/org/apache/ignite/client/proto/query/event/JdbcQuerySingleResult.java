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

import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends JdbcResponse {
    /** Cursor ID. */
    private long cursorId;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private byte qryTypeId;

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcQuerySingleResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err Error message.
     */
    public JdbcQuerySingleResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param cursorId Cursor ID.
     * @param qryTypeId Query type id.
     */
    public JdbcQuerySingleResult(long cursorId, byte qryTypeId) {
        super();

        this.cursorId = cursorId;
        this.qryTypeId = qryTypeId;
    }

    /**
     * Get the cursor id.
     *
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * Get the isQuery flag.
     *
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public byte type() {
        return qryTypeId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        packer.packLong(cursorId);
        packer.packByte(qryTypeId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        cursorId = unpacker.unpackLong();
        qryTypeId = unpacker.unpackByte();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQuerySingleResult.class, this);
    }
}
