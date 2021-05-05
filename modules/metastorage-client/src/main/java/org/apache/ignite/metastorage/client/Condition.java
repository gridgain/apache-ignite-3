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

import java.util.Collection;

/**
 * Represents a condition for meta storage conditional update.
 *
 * @see MetaStorageService#invoke(Condition, Operation, Operation)
 * @see MetaStorageService#invoke(Condition, Collection, Collection)
 */
public final class Condition {
    /** Actual condition implementation. */
    private final InnerCondition cond;

    /**
     * Constructs a condition which wraps the actual condition implementation.
     *
     * @param cond The actual condition implementation.
     */
    Condition(InnerCondition cond) {
        this.cond = cond;
    }

    /**
     * Represents condition on entry revision. Only one type of condition could be applied to
     * the one instance of condition. Subsequent invocations of any method which produces condition will throw
     * {@link IllegalStateException}.
     */
    public static final class RevisionCondition extends AbstractCondition {
        /**
         * The type of condition.
         *
         * @see Type
         */
        private Type type;

        /** The revision as the condition argument. */
        private long rev;

        /**
         * Constructs a condition by a revision for an entry identified by the given key.
         *
         * @param key Identifies an entry which condition will be applied to.
         */
        RevisionCondition(byte[] key) {
            super(key);
        }

        /**
         * Produces the condition of type {@link Type#EQUAL}. This condition tests the given revision on equality with
         * target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition eq(long rev) {
            validate(type);

            this.type = Type.EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#NOT_EQUAL}. This condition tests the given revision on inequality
         * with target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#NOT_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ne(long rev) {
            validate(type);

            this.type = Type.NOT_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#GREATER}. This condition tests that the target entry revision
         * is greater than given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#GREATER}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition gt(long rev) {
            validate(type);

            this.type = Type.GREATER;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#GREATER_OR_EQUAL}. This condition tests that the target entry
         * revision is greater than or equal to given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#GREATER_OR_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ge(long rev) {
            validate(type);

            this.type = Type.GREATER_OR_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#LESS}. This condition tests that target entry revision
         * is less than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#LESS}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition lt(long rev) {
            validate(type);

            this.type = Type.LESS;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#LESS_OR_EQUAL}. This condition tests that target entry revision
         * is less than or equal to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#LESS_OR_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition le(long rev) {
            validate(type);

            this.type = Type.LESS_OR_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Defines possible condition types which can be applied to the revision.
         */
        enum Type {
            /** Equality condition type. */
            EQUAL,

            /** Inequality condition type. */
            NOT_EQUAL,

            /** Greater than condition type. */
            GREATER,

            /** Less than condition type. */
            LESS,

            /** Less than or equal to condition type. */
            LESS_OR_EQUAL,

            /** Greater than or equal to condition type. */
            GREATER_OR_EQUAL
        }
    }

    /**
     * Represents condition on entry value. Only one type of condition could be applied to
     * the one instance of condition. Subsequent invocations of any method which produces condition will throw
     * {@link IllegalStateException}.
     */
    public static final class ValueCondition extends AbstractCondition {
        /**
         * The type of condition.
         *
         * @see Type
         */
        private Type type;

        /** The value as the condition argument. */
        private byte[] val;

        /**
         * Constructs a condition by a value for an entry identified by the given key.
         *
         * @param key Identifies an entry which condition will be applied to.
         */
        ValueCondition(byte[] key) {
            super(key);
        }

        /**
         * Produces the condition of type {@link Type#EQUAL}. This condition tests the given value on equality with
         * target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link Type#EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition eq(byte[] val) {
            validate(type);

            this.type = Type.EQUAL;
            this.val = val;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#NOT_EQUAL}. This condition tests the given value on inequality
         * with target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link Type#NOT_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ne(byte[] val) {
            validate(type);

            this.type = Type.NOT_EQUAL;
            this.val = val;

            return new Condition(this);
        }

        /**
         * Defines possible condition types which can be applied to the value.
         */
        enum Type {
            /** Equality condition type. */
            EQUAL,

            /** Inequality condition type. */
            NOT_EQUAL
        }
    }

    /**
     * Represents condition on an entry existence. Only one type of condition could be applied to
     * the one instance of condition. Subsequent invocations of any method which produces condition will throw
     * {@link IllegalStateException}.
     */
    public static final class ExistenceCondition extends AbstractCondition {
        /**
         * The type of condition.
         *
         * @see Type
         */
        private Type type;

        /**
         * Constructs a condition on existence an entry identified by the given key.
         *
         * @param key Identifies an entry which condition will be applied to.
         */
        ExistenceCondition(byte[] key) {
            super(key);
        }

        /**
         * Produces the condition of type {@link Type#EXISTS}. This condition tests the existence of an entry
         * identified by the given key.
         *
         * @return The condition of type {@link Type#EXISTS}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition exists() {
            validate(type);

            this.type = Type.EXISTS;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#NOT_EXISTS}. This condition tests the non-existence of an entry
         * identified by the given key.
         *
         * @return The condition of type {@link Type#NOT_EXISTS}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition notExists() {
            validate(type);

            this.type = Type.NOT_EXISTS;

            return new Condition(this);
        }

        /**
         * Defines possible condition types which can be applied to the value.
         */
        enum Type {
            /** Existence condition type. */
            EXISTS,

            /** Non-existence condition type. */
            NOT_EXISTS
        }
    }

    /**
     * Checks that condition is not defined yet. If the condition is already defined then exception will be thrown.
     *
     * @throws IllegalStateException In case when the condition is already defined.
     */
    private static void validate(Enum<?> type) {
        if (type != null)
            throw new IllegalStateException("Condition type " + type.name() + " is already defined.");
    }

    /**
     * Defines condition interface.
     */
    private interface InnerCondition {
        /**
         * Returns key which identifies an entry which condition will be applied to.
         *
         * @return Key which identifies an entry which condition will be applied to.
         */
        byte[] key();
    }

    /**
     * Defines an abstract condition with the key which identifies an entry which condition will be applied to.
     */
    private static abstract class AbstractCondition implements InnerCondition {
        /** Entry key. */
        private final byte[] key;

        /**
         * Constructs a condition with the given entry key.
         *
         * @param key Key which identifies an entry which condition will be applied to.
         */
        public AbstractCondition(byte[] key) {
            this.key = key;
        }

        /**
         * Returns the key which identifies an entry which condition will be applied to.
         *
         * @return Key which identifies an entry which condition will be applied to.
         */
        @Override public byte[] key() {
            return key;
        }
    }
}
