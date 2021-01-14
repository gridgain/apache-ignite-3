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

package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.internal.schema.HashIndexImpl;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.builder.HashIndexBuilder;

public class HashIndexBuilderImpl extends AbstractIndexBuilder implements HashIndexBuilder {
    protected String[] columns;

    public HashIndexBuilderImpl(String indexName) {
        super(indexName);
    }

    @Override public HashIndexBuilder withColumns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    @Override public HashIndex build() {
        assert columns != null;
        assert columns.length > 0;

        return new HashIndexImpl(name());
    }

    String[] columns() {
        return columns;
    }
}
