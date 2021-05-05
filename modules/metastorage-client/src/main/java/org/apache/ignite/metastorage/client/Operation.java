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

package org.apache.ignite.metastorage.client;

import org.jetbrains.annotations.Nullable;

/**
 * Defines operation for meta storage conditional update (invoke).
 */
public final class Operation {
    /** Actual operation implementation. */
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final InnerOp upd;

    /**
     * Constructs an operation which wraps the actual operation implementation.
     *
     * @param upd The actual operation implementation.
     */
    Operation(InnerOp upd) {
        this.upd = upd;
    }

    /**
     * Represents operation of type <i>remove</i>.
     */
    public static final class RemoveOp extends AbstractOp {
        /**
         * Default no-op constructor.
         *
         * @param key Identifies an entry which operation will be applied to.
         */
        RemoveOp(byte[] key) {
            super(key);
        }
    }

    /**
     * Represents operation of type <i>put</i>.
     */
    public static final class PutOp extends AbstractOp {
        /** Value. */
        private final byte[] val;

        /**
         * Constructs operation of type <i>put</i>.
         *
         * @param key Identifies an entry which operation will be applied to.
         * @param val The value to which the entry should be updated.
         */
        PutOp(byte[] key, byte[] val) {
            super(key);

            this.val = val;
        }
    }

    /**
     * Represents operation of type <i>no-op</i>.
     */
    public static final class NoOp extends AbstractOp {
        /**
         * Default no-op constructor.
         */
        NoOp() {
            super(null);
        }
    }

    /**
     * Defines operation interface.
     */
    private interface InnerOp {
        @Nullable byte[] key();
    }

    private static class AbstractOp implements InnerOp {
        @Nullable private final byte[] key;

        public AbstractOp(@Nullable byte[] key) {
            this.key = key;
        }

        @Nullable
        @Override public byte[] key() {
            return key;
        }
    }
}
