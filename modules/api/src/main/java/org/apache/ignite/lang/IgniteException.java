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

import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;
import static org.apache.ignite.lang.ErrorGroup.extractGroupCode;
import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR_GROUP;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * General Ignite exception. This exception is used to indicate any error condition within the node.
 */
public class IgniteException extends RuntimeException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Name of the error group. */
    private final String groupName;

    /**
     * Error code which contains information about error group and code, where code is unique within the group.
     * The structure of a code is shown in the following diagram:
     * +------------+--------------+
     * |  16 bits   |    16 bits   |
     * +------------+--------------+
     * | Group Code |  Error Code  |
     * +------------+--------------+
     */
    private final int code;

    /** Unique identifier of this exception that should help locating the error message in a log file. */
    private final UUID traceId;

    /**
     * Creates an empty exception.
     */
    @Deprecated
    public IgniteException() {
        this(UNKNOWN_ERR_GROUP.name(), UNKNOWN_ERR);
    }

    /**
     * Creates a new exception with the given error message.
     *
     * @param msg Error message.
     */
    @Deprecated
    public IgniteException(String msg) {
        this(UNKNOWN_ERR_GROUP.name(), UNKNOWN_ERR, msg);
    }

    /**
     * Creates a new grid exception with the given throwable as a cause and source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    @Deprecated
    public IgniteException(Throwable cause) {
        this(UNKNOWN_ERR_GROUP.name(), UNKNOWN_ERR, cause);
    }

    /**
     * Creates a new exception with the given error message and optional nested exception.
     *
     * @param msg   Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    @Deprecated
    public IgniteException(String msg, @Nullable Throwable cause) {
        this(UNKNOWN_ERR_GROUP.name(), UNKNOWN_ERR, msg, cause);
    }

    /**
     * Creates a new exception with the given group and error code.
     *
     * @param groupName Group name.
     * @param code Full error code.
     */
    public IgniteException(String groupName, int code) {
        this(UUID.randomUUID(), groupName, code);
    }

    /**
     * Creates a new exception with the given group and error code.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     */
    public IgniteException(UUID traceId, String groupName, int code) {
        super(errorMessage(traceId, groupName, code, null));

        this.traceId = traceId;
        this.groupName = groupName;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code and detail message.
     *
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Detail message.
     */
    public IgniteException(String groupName, int code, String message) {
        this(UUID.randomUUID(), groupName, code, message);
    }

    /**
     * Creates a new exception with the given group, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Detail message.
     */
    public IgniteException(UUID traceId, String groupName, int code, String message) {
        super(errorMessage(traceId, groupName, code, message));

        this.traceId = traceId;
        this.groupName = groupName;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code and cause.
     *
     * @param groupName Group name.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(String groupName, int code, Throwable cause) {
        this(UUID.randomUUID(), groupName, code, cause);
    }

    /**
     * Creates a new exception with the given group, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(UUID traceId, String groupName, int code, Throwable cause) {
        super(errorMessageFromCause(traceId, groupName, code, cause), cause);

        this.traceId = traceId;
        this.groupName = groupName;
        this.code = code;
    }

    /**
     * Creates a new exception with the given group, error code, detail message and cause.
     *
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(String groupName, int code, String message, Throwable cause) {
        this(UUID.randomUUID(), groupName, code, message, cause);
    }

    /**
     * Creates a new exception with the given group, error code, detail message and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteException(UUID traceId, String groupName, int code, String message, Throwable cause) {
        super(errorMessage(traceId, groupName, code, message), cause);

        this.traceId = traceId;
        this.groupName = groupName;
        this.code = code;
    }

    /**
     * Returns a group name of this error.
     *
     * @see #groupCode()
     * @see #code()
     * @return Group name.
     */
    public String groupName() {
        return groupName;
    }

    /**
     * Returns a full error code which includes a group of the error and code which is uniquely identifies a problem within the group.
     * This is a combination of two most-significant bytes that represent the error group and
     * two least-significant bytes for the error code.
     *
     * @return Full error code.
     */
    public int code() {
        return code;
    }

    /**
     * Returns error group.
     *
     * @see #code()
     * @return Error group.
     */
    public int groupCode() {
        return extractGroupCode(code);
    }

    /**
     * Returns error code that uniquely identifies a problem within a group.
     *
     * @see #code()
     * @see #groupCode()
     * @return Error code.
     */
    public int errorCode() {
        return extractErrorCode(code);
    }

    /**
     * Returns an unique identifier of this exception.
     *
     * @return Unique identifier of this exception.
     */
    public UUID traceId() {
        return traceId;
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Original message.
     * @return New error message with predefined prefix.
     */
    private static String errorMessage(UUID traceId, String groupName, int code, String message) {
        return "IGN-" + groupName + '-' + extractErrorCode(code) + " Trace ID:" + traceId + ((message != null) ? ' ' + message : "");
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param cause Cause.
     * @return New error message with predefined prefix.
     */
    private static String errorMessageFromCause(UUID traceId, String groupName, int code, Throwable cause) {
        String c = (cause != null && cause.getMessage() != null) ? cause.getMessage() : null;

        return (c != null && c.startsWith("IGN-")) ? c :  errorMessage(traceId, groupName, code, c);
    }
}
