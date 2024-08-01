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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: enum.proto

package org.apache.ignite.raft.jraft.entity;

import org.apache.ignite.internal.network.annotations.TransferableEnum;
import org.jetbrains.annotations.Nullable;

public final class EnumOutter {
    private EnumOutter() {
    }

    /**
     * Protobuf enum {@code jraft.EntryType}
     */
    public enum EntryType implements TransferableEnum {
        /**
         * <code>ENTRY_TYPE_UNKNOWN = 0;</code>
         */
        ENTRY_TYPE_UNKNOWN(0, 0),
        /**
         * <code>ENTRY_TYPE_NO_OP = 1;</code>
         */
        ENTRY_TYPE_NO_OP(1, 1),
        /**
         * <code>ENTRY_TYPE_DATA = 2;</code>
         */
        ENTRY_TYPE_DATA(2, 2),
        /**
         * <code>ENTRY_TYPE_CONFIGURATION = 3;</code>
         */
        ENTRY_TYPE_CONFIGURATION(3, 3);

        public final int getNumber() {
            return value;
        }

        /**
         * @deprecated Use {@link #forNumber(int)} instead.
         */
        @Deprecated
        public static EntryType valueOf(int value) {
            return forNumber(value);
        }

        public static @Nullable EntryType forNumber(int value) {
            switch (value) {
                case 0:
                    return ENTRY_TYPE_UNKNOWN;
                case 1:
                    return ENTRY_TYPE_NO_OP;
                case 2:
                    return ENTRY_TYPE_DATA;
                case 3:
                    return ENTRY_TYPE_CONFIGURATION;
                default:
                    return null;
            }
        }

        private final int value;

        private final int transferableId;

        EntryType(int value, int transferableId) {
            this.value = value;
            this.transferableId = transferableId;
        }
        @Override
        public int transferableId() {
            return transferableId;
        }
    }

    /**
     * Protobuf enum {@code jraft.ErrorType}
     */
    public enum ErrorType {
        /**
         * <code>ERROR_TYPE_NONE = 0;</code>
         */
        ERROR_TYPE_NONE(0),
        /**
         * <code>ERROR_TYPE_LOG = 1;</code>
         */
        ERROR_TYPE_LOG(1),
        /**
         * <code>ERROR_TYPE_STABLE = 2;</code>
         */
        ERROR_TYPE_STABLE(2),
        /**
         * <code>ERROR_TYPE_SNAPSHOT = 3;</code>
         */
        ERROR_TYPE_SNAPSHOT(3),
        /**
         * <code>ERROR_TYPE_STATE_MACHINE = 4;</code>
         */
        ERROR_TYPE_STATE_MACHINE(4),
        /**
         * <code>ERROR_TYPE_META = 5;</code>
         */
        ERROR_TYPE_META(5);

        public final int getNumber() {
            return value;
        }

        /**
         * @deprecated Use {@link #forNumber(int)} instead.
         */
        @Deprecated
        public static ErrorType valueOf(int value) {
            return forNumber(value);
        }

        public static @Nullable ErrorType forNumber(int value) {
            switch (value) {
                case 0:
                    return ERROR_TYPE_NONE;
                case 1:
                    return ERROR_TYPE_LOG;
                case 2:
                    return ERROR_TYPE_STABLE;
                case 3:
                    return ERROR_TYPE_SNAPSHOT;
                case 4:
                    return ERROR_TYPE_STATE_MACHINE;
                case 5:
                    return ERROR_TYPE_META;
                default:
                    return null;
            }
        }

        private final int value;

        private ErrorType(int value) {
            this.value = value;
        }
    }
}
