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

/**
 * Table column descriptor.
 */
public interface Column {
    /**
     * @return Columnt type.
     */
    ColumnType type();

    /**
     * @return Column name.
     */
    String name();

    /**
     * Nullable flag.
     *
     * @return {@code True} if null-values is allowed, {@code false} otherwise.
     */
    boolean nullable();

    /**
     * @return Default column value.
     */
    Object defaultValue();

    /**
     * @return {@code true} if column belong to key, {@code false} otherwise.
     */
    boolean isKeyColumn();

    /**
     * @return {@code true} if column is affinity column, {@code false} otherwise.
     */
    boolean isAffinityColumn();
}
