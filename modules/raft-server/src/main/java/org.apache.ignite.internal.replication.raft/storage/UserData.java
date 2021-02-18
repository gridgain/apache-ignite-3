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

package org.apache.ignite.internal.replication.raft.storage;

/**
 *
 */
@SuppressWarnings("rawtypes")
public class UserData<T> implements LogData {
    private static final UserData EMPTY = new UserData(null);

    private final T data;

    public static <T> UserData<T> empty() {
        return EMPTY;
    }

    public UserData(T data) {
        this.data = data;
    }

    public T data() {
        return data;
    }

    public String toString() {
        return "UserData[" + data + "]";
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        UserData<?> data1 = (UserData<?>)o;

        return data != null ? data.equals(data1.data) : data1.data == null;
    }
}
