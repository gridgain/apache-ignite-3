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

package org.apache.ignite.internal.vault.common;

import java.util.Comparator;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;

public class Watch {
    private static final Comparator<ByteArray> CMP = ByteArray::compare;

    @Nullable
    private ByteArray startKey;

    @Nullable
    private ByteArray endKey;

    private ValueListener listener;

    /**
     * @param listener Listener.
     */
    public Watch(ValueListener listener) {
        this.listener = listener;
    }

    public void startKey(ByteArray startKey) {
        this.startKey = startKey;
    }

    public void endKey(ByteArray endKey) {
        this.endKey = endKey;
    }

    public void notify(VaultEntry val) {
        if (startKey != null && CMP.compare(val.key(), startKey) < 0)
            return;

        if (endKey != null && CMP.compare(val.key(), endKey) > 0)
            return;

        listener.onValueChanged(val);
    }
}
