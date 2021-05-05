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

import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines operation which will be applied to an entry identified by the key.
 * <p>
 * Invariants:
 * <ul>
 *     <li>Any operation identifies a target entry by not null {@code key} except of {@link Type#NO_OP}.</li>
 *     <li>Only {@link Type#PUT} operation contains value which will be written to meta storage.</li>
 * </ul>
 */
final class Operation {
    /**
     * Key identifies an entry which operation will be applied to. Key is {@code null} for {@link Type#NO_OP} operation.
     */
    @Nullable
    private final byte[] key;

    /**
     * Value which will be associated with the {@link #key}. Value is not {@code null} only for {@link Type#PUT}
     * operation.
     */
    @Nullable
    private final byte[] val;

    /**
     * Operation type.
     * @see Type
     */
    @NotNull
    private final Type type;

    /**
     * Constructs operation which will be applied to an entry identified by the given key.
     *
     * @param type Operation type. Can't be {@code null}.
     * @param key Key identifies an entry which operation will be applied to.
     * @param val Value will be associated with an entry identified by the {@code key}.
     */
    Operation(@NotNull Type type, @Nullable byte[] key, @Nullable byte[] val) {
        assert (type == Type.NO_OP && key == null && val == null)
                || (type == Type.PUT && key != null && val != null)
                || (type == Type.REMOVE && key != null && val == null)
                : "Invalid operation parameters: [type=" + type + ", key=" + Objects.toString(key,"null") +
                ", val=" + Objects.toString(key,"null") + ']';

        this.key = key;
        this.val = val;
        this.type = type;
    }

    /**
     * Returns a key which identifies an entry which operation will be applied to.
     *
     * @return A key which identifies an entry which operation will be applied to.
     */
    @Nullable byte[] key() {
        return key;
    }

    /**
     * Returns a value which will be associated with an entry identified by the {@code key}.
     *
     * @return A value which will be associated with an entry identified by the {@code key}.
     */
    @Nullable byte[] value() {
        return val;
    }

    /**
     * Returns an operation type.
     *
     * @return An operation type.
     */
    @NotNull Type type() {
        return type;
    }

    /** Defines operation types. */
    enum Type {
        /** Put operation. */
        PUT,

        /** Remove operation. */
        REMOVE,

        /** No-op operation. */
        NO_OP
    }
}
