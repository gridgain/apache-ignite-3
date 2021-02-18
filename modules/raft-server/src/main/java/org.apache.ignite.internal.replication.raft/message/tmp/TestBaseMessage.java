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

package org.apache.ignite.internal.replication.raft.message.tmp;

import org.apache.ignite.internal.replication.raft.message.Message;
import org.apache.ignite.internal.replication.raft.message.MessageType;

import java.util.UUID;

/**
 *
 */
public abstract class TestBaseMessage implements Message {
    /** */
    protected final MessageType type;

    /** */
    protected final UUID from;

    /** */
    protected final UUID to;

    /** */
    protected final long term;

    TestBaseMessage(MessageType type, UUID from, UUID to, long term) {
        this.type = type;
        this.from = from;
        this.to = to;
        this.term = term;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public UUID from() {
        return from;
    }

    /** {@inheritDoc} */
    @Override public UUID to() {
        return to;
    }

    /** {@inheritDoc} */
    @Override public long term() {
        return term;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof TestBaseMessage))
            return false;

        TestBaseMessage that = (TestBaseMessage)o;

        if (term != that.term)
            return false;

        if (type != that.type)
            return false;

        if (!from.equals(that.from))
            return false;

        if (!to.equals(that.to))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = type.hashCode();

        result = 31 * result + from.hashCode();
        result = 31 * result + to.hashCode();
        result = 31 * result + (int)(term ^ (term >>> 32));

        return result;
    }
}
