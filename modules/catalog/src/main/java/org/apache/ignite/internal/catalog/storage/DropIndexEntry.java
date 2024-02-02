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

package org.apache.ignite.internal.catalog.storage;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes drop of an index (it's not the final removal of an index from the Catalog, but it's just a switch to
 * the {@link CatalogIndexStatus#STOPPING} state.
 */
public class DropIndexEntry extends AbstractChangeIndexStatusEntry implements Fireable {
    private static final long serialVersionUID = -604729846502020728L;

    private final int tableId;

    /**
     * Constructs the object.
     *
     * @param indexId An id of an index to drop.
     * @param tableId Table ID for which the index was removed.
     */
    public DropIndexEntry(int indexId, int tableId) {
        super(indexId, CatalogIndexStatus.STOPPING);

        this.tableId = tableId;
    }

    /** Returns an id of an index to drop. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID for which the index was removed. */
    public int tableId() {
        return tableId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_STOPPING;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new StoppingIndexEventParameters(causalityToken, catalogVersion, indexId, tableId);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
