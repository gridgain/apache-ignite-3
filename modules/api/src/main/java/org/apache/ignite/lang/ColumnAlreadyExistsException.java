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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroups.Table.COLUMN_ALREADY_EXISTS_ERR;
import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_ERR_GROUP;

/**
 * This exception is thrown when a new column failed to be created, because another column with the same name already exists.
 */
public class ColumnAlreadyExistsException extends IgniteException {
    /**
     * Create a new exception with given column name.
     *
     * @param name Column name.
     */
    public ColumnAlreadyExistsException(String name) {
        super(TABLE_ERR_GROUP.name(), COLUMN_ALREADY_EXISTS_ERR, "Column already exists [name=" + name + ']');
    }
}
