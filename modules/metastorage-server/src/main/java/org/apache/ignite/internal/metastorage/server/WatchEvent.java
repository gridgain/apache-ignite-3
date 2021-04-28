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

package org.apache.ignite.internal.metastorage.server;

import java.util.Collection;
import java.util.List;

public class WatchEvent {
    private final List<EntryEvent> entryEvts;

    private final boolean batch;

    /**
     * Constructs an watch event with given entry events collection.
     *
     * @param entryEvts Events for entries corresponding to an update under one revision.
     */
    public WatchEvent(List<EntryEvent> entryEvts) {
        assert entryEvts != null && !entryEvts.isEmpty();

        this.batch = entryEvts.size() > 1;
        this.entryEvts = entryEvts;
    }

    public boolean batch() {
        return batch;
    }

    public Collection<EntryEvent> entryEvents() {
        return entryEvts;
    }

    public EntryEvent entryEvent() {
        if (batch)
            throw new IllegalStateException("Watch event represents a batch of events.");

        return entryEvts.get(0);
    }
}
