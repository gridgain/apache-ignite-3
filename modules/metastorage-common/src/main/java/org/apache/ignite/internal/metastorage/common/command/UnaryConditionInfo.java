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

package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.ConditionType;

/**
 * Defines condition for InvokeCommand.
 */
public class UnaryConditionInfo implements ConditionInfo {
    /** Key. */
    private final byte[] key;

    /** Type. */
    private final ConditionType type;

    /** Value. */
    private final byte[] val;

    /** Revision. */
    private final long rev;
    
    /**
     * Construct condition with given parameters.
     *
     * @param key  Key.
     * @param type Condition type.
     * @param val  Value.
     * @param rev  Revision.
     */
    public UnaryConditionInfo(byte[] key, ConditionType type, byte[] val, long rev) {
        this.key = key;
        this.type = type;
        this.val = val;
        this.rev = rev;
    }

    /**
     * Returns key.
     *
     * @return Key.
     */
    public byte[] key() {
        return key;
    }

    /**
     * Returns condition type.
     *
     * @return Condition type.
     */
    public ConditionType type() {
        return type;
    }

    /**
     * Returns value.
     *
     * @return Value.
     */
    public byte[] value() {
        return val;
    }

    /**
     * Returns revision.
     *
     * @return Revision.
     */
    public long revision() {
        return rev;
    }
}
