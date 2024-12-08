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

package org.apache.ignite.internal.util;

import java.lang.reflect.Field;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import jdk.internal.misc.Unsafe;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;

/**
 * Lock state structure is as follows.
 * <pre>
 *     +----------------+---------+----------+
 *     | WRITE WAIT CNT |   TAG   | LOCK CNT |
 *     +----------------+---------+----------+
 *     |     2 bytes    | 2 bytes |  4 bytes |
 *     +----------------+---------+----------+
 * </pre>
 */
public class OffheapReadWriteLock {
    private static final Unsafe UNSAFE = jdkUnsafe();

    private static Unsafe jdkUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");

            f.setAccessible(true);

            return (Unsafe) f.get(null);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public OffheapReadWriteLock(int unused) {
    }

    /**
     * Default empirical value for the spin count.
     *
     * @see #IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT
     */
    private static final int DFLT_OFFHEAP_RWLOCK_SPIN_COUNT = 512;

    /** A number of spin-lock iterations to take before falling back to the blocking approach. */
    private static final String IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT = "IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT";

    /** Count of spins before the fallback to {@link ReentrantLock} and {@link Condition}. */
    private static final int SPIN_CNT = IgniteSystemProperties.getInteger(IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT, DFLT_OFFHEAP_RWLOCK_SPIN_COUNT);

    /** Always lock tag. */
    public static final int TAG_LOCK_ALWAYS = -1;

    /** Lock size. */
    public static final int LOCK_SIZE = 8;

    /**
     * Initializes the lock.
     *
     * @param lock Lock pointer to initialize.
     */
    public void init(long lock, int tag) {
        tag &= 0xFFFF;

        assert tag != 0;

        UNSAFE.putLong(lock, (long) tag << 32);
    }

    /**
     * Acquires a read lock.
     *
     * @param lock Lock address.
     */
    public boolean readLock(long lock, int tag) {
        outer : while (true) {
            for (int i = 0; i < SPIN_CNT; i++) {
                long state = UNSAFE.getLongVolatile(null, lock);

                assert state != 0L;

                if ((state & 0xFFFF000000000000L) != 0L) {
                    // Write lock counter is not 0. Write lock is or will soon be acquired.
                    Thread.yield();

                    continue outer;
                }

                if (!UNSAFE.weakCompareAndSetLong(null, lock, state, state + 1)) {
                    continue;
                }

                // Late check reduces contention.
                if (!checkTag(state, tag)) {
                    // Decrement it back.
                    UNSAFE.getAndAddLong(null, lock, -1L);

                    return false;
                }

                return true;
            }

            // Spin count exhausted. Release the thread.
            Thread.yield();
        }
    }

    /**
     * Releases read lock.
     *
     * @param lock Lock address.
     */
    public void readUnlock(long lock) {
        long state = UNSAFE.getLongVolatile(null, lock);

        if (lockCount(state) <= 0) {
            throw new IllegalMonitorStateException("Attempted to release a read lock while not holding it "
                    + "[lock=" + StringUtils.hexLong(lock) + ", state=" + StringUtils.hexLong(state) + ']');
        }

        UNSAFE.getAndAddLong(null, lock, -1L);
    }

    /**
     * Tries to acquire a write lock.
     *
     * @param lock Lock address.
     */
    public boolean tryWriteLock(long lock, int tag) {
        long state = UNSAFE.getLongVolatile(null, lock);

        return ((int) state == 0) && checkTag(state, tag)
                && UNSAFE.compareAndSetLong(null, lock, state, state + 0x00010000FFFFFFFFL);
    }

    /**
     * Acquires a write lock.
     *
     * @param lock Lock address.
     */
    public boolean writeLock(long lock, int tag) {
        assert tag != 0;

        boolean waitCounterIncreased = false;
        long delta = 0x00010000FFFFFFFFL;

        while (true) {
            for (int i = 0; i < SPIN_CNT; i++) {
                long state = UNSAFE.getLongVolatile(null, lock);

                assert state != 0L;

                if ((int) state != 0) {
                    // Lock counter is not 0.
                    continue;
                }

                if (!UNSAFE.weakCompareAndSetLong(null, lock, state, state + delta)) {
                    continue;
                }

                // Late check reduces contention.
                if (!checkTag(state, tag)) {
                    // Decrement it back.
                    UNSAFE.getAndAddLong(null, lock, -0x00010000FFFFFFFFL);

                    return false;
                }

                return true;
            }

            if (!waitCounterIncreased) {
                UNSAFE.getAndAddLong(null, lock, 0x0001000000000000L);

                delta -= 0x0001000000000000L;

                waitCounterIncreased = true;
            }

            // Spin count exhausted. Release the thread.
            Thread.yield();
        }
    }

    /**
     * Checks whether write lock is acquired.
     *
     * @param lock Lock to check.
     * @return {@code True} if write lock is held by any thread for the given offheap RW lock.
     */
    public boolean isWriteLocked(long lock) {
        return lockCount(UNSAFE.getLongVolatile(null, lock)) < 0;
    }

    /**
     * Checks whether read lock is acquired.
     *
     * @param lock Lock to check.
     * @return {@code True} if at least one read lock is held by any thread for the given offheap RW lock.
     */
    public boolean isReadLocked(long lock) {
        return lockCount(UNSAFE.getLongVolatile(null, lock)) > 0;
    }

    /**
     * Releases write lock.
     *
     * @param lock Lock address.
     */
    public void writeUnlock(long lock, int tag) {
        assert tag != 0;

        long state = GridUnsafe.getLongVolatile(null, lock);

        if (!isWriteLocked(lock)) {
            throw new IllegalMonitorStateException("Attempted to release write lock while not holding it "
                    + "[lock=" + StringUtils.hexLong(lock) + ", state=" + StringUtils.hexLong(state) + ']');
        }

        if (checkTag(state, tag)) {
            UNSAFE.getAndAddLong(null, lock, -0x00010000FFFFFFFFL);
        } else {
            long oldTag = state & 0x0000FFFF00000000L;
            long newTag = (long) (tag & 0xFFFF) << 32;

            UNSAFE.getAndAddLong(null, lock, -0x00010000FFFFFFFFL + (newTag - oldTag));
        }
    }

    /**
     * Upgrades a read lock to a write lock. If this thread is the only read-owner of the read lock,
     * this method will atomically upgrade the read lock to the write lock. In this case {@code true}
     * will be returned. If not, the read lock will be released and write lock will be acquired, leaving
     * a potential gap for other threads to modify a protected resource. In this case this method will return
     * {@code false}.
     *
     * <p>After this method has been called, there is no need to call to {@link #readUnlock(long)} because
     * read lock will be released in any case.
     *
     * @param lock Lock to upgrade.
     * @return {@code null} if tag validation failed, {@code true} if successfully traded the read lock to
     *      the write lock without leaving a gap. Returns {@code false} otherwise, in this case the resource
     *      state must be re-validated.
     */
    // TODO This is stupid, we only need "tryUpgradeToWriteLock".
    //  Current method makes no sense, it's unused, and it's broken in theoriginal implementation.
    public @Nullable Boolean upgradeToWriteLock(long lock, int tag) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = UNSAFE.getLongVolatile(null, lock);

            if (!checkTag(state, tag)) {
                return null;
            }

            if (lockCount(state) == 1) {
                if (UNSAFE.weakCompareAndSetLong(null, lock, state, state + 0x00010000FFFFFFFEL)) {
                    return true;
                } else {
                    // Retry CAS, do not count as spin cycle.
                    i--;
                }
            }
        }

        readUnlock(lock);

        if (writeLock(lock, tag)) {
            return null;
        }

        return false;
    }

    /**
     * Checks that tag in the state matches the expected value.
     *
     * @param state State.
     * @param tag Tag.
     */
    private boolean checkTag(long state, int tag) {
        // If passed in tag is negative, lock regardless of the state.
        return tag < 0 || tag(state) == tag;
    }

    /**
     * Extracts lock count from the state.
     *
     * @param state State.
     * @return Lock count.
     */
    private static int lockCount(long state) {
        return (int) state;
    }

    /**
     * Extracts tag value from the state.
     *
     * @param state Lock state.
     * @return Lock tag.
     */
    private int tag(long state) {
        return (int) ((state >>> 32) & 0xFFFF);
    }
}
