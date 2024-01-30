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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Helper class for working with {@link CompletableFuture}. */
public class CompletableFutures {
    private static final CompletableFuture<Void> NULL_COMPLETED_FUTURE = completedFuture(null);

    private static final CompletableFuture<Boolean> TRUE_COMPLETED_FUTURE = completedFuture(true);

    private static final CompletableFuture<Boolean> FALSE_COMPLETED_FUTURE = completedFuture(false);

    private static final CompletableFuture<List<?>> EMPTY_LIST_COMPLETED_FUTURE = completedFuture(List.of());

    private static final CompletableFuture<Set<?>> EMPTY_SET_COMPLETED_FUTURE = completedFuture(Set.of());

    private static final CompletableFuture<Map<?, ?>> EMPTY_MAP_COMPLETED_FUTURE = completedFuture(Map.of());

    /** Returns a future completed with a {@code null} value. */
    public static <T> CompletableFuture<T> nullCompletedFuture() {
        return (CompletableFuture<T>) NULL_COMPLETED_FUTURE;
    }

    /** Returns a future completed with a {@code true} value. */
    public static CompletableFuture<Boolean> trueCompletedFuture() {
        return TRUE_COMPLETED_FUTURE;
    }

    /** Returns a future completed with a {@code false} value. */
    public static CompletableFuture<Boolean> falseCompletedFuture() {
        return FALSE_COMPLETED_FUTURE;
    }

    /**
     * Returns a future completed with the given boolean value.
     *
     * @param b Boolean value.
     */
    public static CompletableFuture<Boolean> booleanCompletedFuture(boolean b) {
        return b ? TRUE_COMPLETED_FUTURE : FALSE_COMPLETED_FUTURE;
    }

    /** Returns a future completed with an empty immutable {@link Collection}. */
    public static <T> CompletableFuture<Collection<T>> emptyCollectionCompletedFuture() {
        return (CompletableFuture<Collection<T>>) (CompletableFuture<?>) EMPTY_LIST_COMPLETED_FUTURE;
    }

    /** Returns a future completed with an empty immutable {@link List}. */
    public static <T> CompletableFuture<List<T>> emptyListCompletedFuture() {
        return (CompletableFuture<List<T>>) (CompletableFuture<?>) EMPTY_LIST_COMPLETED_FUTURE;
    }

    /** Returns a future completed with an empty immutable {@link Set}. */
    public static <T> CompletableFuture<Set<T>> emptySetCompletedFuture() {
        return (CompletableFuture<Set<T>>) (CompletableFuture<?>) EMPTY_SET_COMPLETED_FUTURE;
    }

    /** Returns a future completed with an empty immutable {@link Map}. */
    public static <K, V> CompletableFuture<Map<K, V>> emptyMapCompletedFuture() {
        return (CompletableFuture<Map<K, V>>) (CompletableFuture<?>) EMPTY_MAP_COMPLETED_FUTURE;
    }

    /**
     * Returns a future that is completed when all provided futures complete (the behavior is identical to {@link CompletableFuture#allOf}).
     *
     * <p>If the future completes successfully, it aggregates the results of the source futures into a {@code List}.
     *
     * @param cfs Source futures.
     * @param <T> Type of the result of the source futures.
     * @return Future that completes with a list of results from the source futures.
     */
    @SafeVarargs
    public static <T> CompletableFuture<List<T>> allOf(CompletableFuture<T>... cfs) {
        return CompletableFuture.allOf(cfs)
                .thenApply(v -> {
                    var result = new ArrayList<T>(cfs.length);

                    for (CompletableFuture<T> future : cfs) {
                        result.add(future.join());
                    }

                    return result;
                });
    }
}
