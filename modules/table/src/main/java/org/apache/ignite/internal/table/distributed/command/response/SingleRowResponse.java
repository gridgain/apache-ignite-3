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

package org.apache.ignite.internal.table.distributed.command.response;

import java.io.Serializable;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.table.distributed.command.CommandUtils;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;

/**
 * It is a response object to return a row from the single operation.
 * @see GetCommand
 * @see GetAndDeleteCommand
 * @see GetAndUpsertCommand
 * @see GetAndReplaceCommand
 */
public class SingleRowResponse implements Serializable {
    /** Row. */
    private transient BinaryRow row;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowBytes;

    /**
     * @param row Row.
     */
    public SingleRowResponse(BinaryRow row) {
        this.row = row;

        CommandUtils.rowToBytes(row, bytes -> rowBytes = bytes);
    }

    /**
     * @return Data row.
     */
    public BinaryRow getValue() {
        if (row == null && rowBytes != null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }
}
