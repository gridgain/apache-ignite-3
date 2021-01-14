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

package org.apache.ignite.schema;

import java.util.Collection;

/**
 * Sorted index descriptor.
 */
public interface SortedIndex extends TableIndex {
    /**
     * @return Index inline size.
     */
    int inlineSize();

    /**
     * Return user defined indexed columns.
     *
     * @return Declared columns.
     */
    Collection<IndexColumn> columns();

    /**
     * Returns all index columns: user defined + implicitly attched.
     *
     * @return Indexed columns.
     */
    Collection<IndexColumn> indexedColumns();

    /**
     * Unique index flag.
     *
     * IMPORTANT: Index MUST have all affinity columns declared explicitely.
     *
     * @return Uniq flag.
     */
    default boolean unique() {
        return false;
    }
}
