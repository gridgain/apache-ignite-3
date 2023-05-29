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

package org.apache.ignite.internal.table.event;

import org.apache.ignite.internal.manager.EventParameters;

/**
 * Table event parameters. There are properties which associate with a concrete table.
 */
public class TableEventParameters extends EventParameters {
    /** Table identifier. */
    private final int tableId;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param tableId   Table identifier.
     */
    public TableEventParameters(long causalityToken, int tableId) {
        super(causalityToken);
        this.tableId = tableId;
    }

    /**
     * Get the table identifier.
     *
     * @return Table id.
     */
    public int tableId() {
        return tableId;
    }
}
