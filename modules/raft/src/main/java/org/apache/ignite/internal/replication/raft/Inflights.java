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

package org.apache.ignite.internal.replication.raft;

/**
 * Inflights limits the number of MsgApp (represented by the largest index
 * contained within) sent to followers but not yet acknowledged by them. Callers
 * use full() to check whether more messages can be sent, call add() whenever
 * they are sending a new append, and release "quota" via freeLE() whenever an
 * ack is received.
 */
public class Inflights {
    // the starting index in the buffer
    private int start;
    // number of inflights in the buffer
    private int count;

    // the size of the buffer
    private final int size;

    // buffer contains the index of the last entry
    // inside one message.
    private long[] buffer;

    public Inflights(int size) {
        buffer = new long[Math.min(size, 4)];

        this.size = size;
    }

    public boolean full() {
        return count == size;
    }

    public int count() {
        return count;
    }

    // Frees the inflights smaller or equal to the given index.
    public void freeLE(long to) {
        if (count == 0 || to < buffer[start]) {
            // out of the left side of the window
            return;
        }

        int idx = start;

        int i;

        for (i = 0; i < count; i++) {
            if (to < buffer[idx])  // found the first large inflight
                break;

            // increase index and maybe rotate
            idx++;

            if (idx >= size)
                idx -= size;
        }

        // free i inflights and set new start index
        count -= i;
        start = idx;

        if (count == 0) {
            // inflights is empty, reset the start index so that we don't grow the
            // buffer unnecessarily.
            start = 0;
        }
    }

    public void freeFirstOne() {
        freeLE(buffer[start]);
    }

    // add() notifies the inflights that a new message with the given index is being
    // dispatched. full() must be called prior to add() to verify that there is room
    // for one more message, and consecutive calls to add add() must provide a
    // monotonic sequence of indexes.
    public void add(long inflight) {
        if (full())
            throw new UnrecoverableException("cannot add into a full inflights");

        int next = start + count;
        int size = this.size;

        if (next >= size)
            next -= size;

        if (next >= buffer.length)
            grow();

        buffer[next] = inflight;
        count++;
    }

    public void reset() {
        count = 0;
        start = 0;
    }

    private void grow() {
        int newSize = buffer.length * 2;

        if (newSize == 0)
            newSize = 1;
        else if (newSize > size)
            newSize = size;

        long[] newBuffer = new long[newSize];

        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);

        buffer = newBuffer;
    }
}
