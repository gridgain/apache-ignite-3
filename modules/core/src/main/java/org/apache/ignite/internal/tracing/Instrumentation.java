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

package org.apache.ignite.internal.tracing;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import jdk.jfr.Event;
import jdk.jfr.Label;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.NotNull;

/**
 * Instrumentation class.
 */
public class Instrumentation {
    private static final IgniteLogger LOG = Loggers.forClass(Instrumentation.class);

    private static ThreadLocal<Boolean> firstRun = ThreadLocal.withInitial(() -> true);

    private volatile boolean jfr = false;

    //private final Path file = Path.of("/opt/pubagent/poc/log/stats/dstat/trace.txt");
    private Path file;

    private volatile ConcurrentLinkedQueue<Measurement> measurements = new ConcurrentLinkedQueue<>();

    private final long startTime = System.nanoTime();

    private static final int rate = 1000;

    private static final AtomicInteger opCnt = new AtomicInteger();

    private static final StaticProvider holder = new StaticProvider();

    private static final boolean ENABLED = true;

    public static boolean isEnabled() {
        return ENABLED;
    }

    interface Provider<T> {
        void set(T inst);

        T get();

        void remove();
    }

    public static class StaticProvider implements Provider<Instrumentation> {
        private volatile Instrumentation ins;

        @Override
        public void set(org.apache.ignite.internal.tracing.Instrumentation ins) {
            this.ins = ins;
        }

        @Override
        public Instrumentation get() {
            return ins;
        }

        @Override
        public void remove() {
            ins = null;
        }
    }

    /**
     * Starts instrumentation.
     *
     * @param jfr True to write in jfr, false in file.
     */
    public static void start(boolean jfr) {
        if (isStarted() || !isEnabled()) {
            return;
        }

        if (opCnt.incrementAndGet() % rate == 0) {
            return;
        }

        Instrumentation ins = new Instrumentation();
        ins.jfr = jfr;

        Path file = Path.of("/run/ignite/trace-" + Thread.currentThread().getName() + ".txt");
        //Path file = Path.of("c://work/logs/trace-" + Thread.currentThread().getName() + ".txt");
        ins.file = file;

        // TODO FIXME remove
        if (!firstRun.get() && !jfr) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            firstRun.set(true);

            LOG.info("Instrumentation was started: output=" + file);
        }

        holder.set(ins);
    }

    public static boolean isStarted() {
        return holder.get() != null;
    }

    /**
     * Ends instrumentation.
     */
    public static void end() {
        if (!isStarted()) {
            return;
        }

        Instrumentation ins = holder.get();

        if (ins.jfr) {
            endJfr(ins);
        } else {
            endPrintln(ins);
        }

        holder.remove();
    }

    private static void endPrintln(Instrumentation ins) {
        var endTime = System.nanoTime();
        List<Measurement> m = new ArrayList<>(ins.measurements);
        ins.measurements.clear();
        m.sort(Measurement::compareTo);
        AtomicLong found = new AtomicLong(0L);
        var sb = new StringBuilder();
        AtomicLong previousEnd = new AtomicLong(0L);
        m.forEach(measurement -> {
            found.addAndGet(measurement.end - measurement.start);
            if (previousEnd.get() != 0) {
                var f = measurement.start - previousEnd.get();
                if (f >= 0) {
                    sb.append("    Here is hidden " + f / 1000.0 + " us\n");
                } else {
                    sb.append("    Here is hidden n/a \n");
                }
            }
            previousEnd.set(measurement.end);
            sb.append(measurement.message + " " + (measurement.end - measurement.start) / 1000.0 + " " + measurement.start % 10000000 + " "
                    + measurement.end % 10000000 + "\n");
        });
        double wholeTime = (endTime - ins.startTime);
        sb.append("Whole time: " + (endTime - ins.startTime) / 1000.0 + " us" + "\n");
        sb.append(
                "Found time: " + found + " Not found time: " + (wholeTime - found.get()) + " Percentage of found: "
                        + 100 * found.get() / wholeTime + "\n\n");
        try {
            Files.write(ins.file, sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void endJfr(Instrumentation ins) {
        List<Measurement> m = new ArrayList<>(ins.measurements);
        m.forEach(Event::commit);
        ins.measurements.clear();
    }

    public static void add(Measurement measurement) {
        Instrumentation instrumentation = holder.get();
        if (instrumentation == null) {
            return;
        }

        instrumentation.add(measurement);
    }

    public static void mark(String message) {
        Instrumentation instrumentation = holder.get();
        if (instrumentation == null) {
            return;
        }

        var measure = new Measurement(message);
        measure.start();
        measure.stop();
        instrumentation.measurements.add(measure);
    }

    public static <T> T measure(Action<T> block, String message) {
        Instrumentation instrumentation = holder.get();
        if (instrumentation == null) {
            try {
                return block.action();
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }

        var measure = new Measurement(message);
        measure.start();
        T results = null;
        try {
            results = block.action();
        } catch (Exception e) {
            sneakyThrow(e);
        } finally {
            measure.stop();
        }

        instrumentation.measurements.add(measure);

        return results;
    }

    public static void measure(VoidAction block, String message) {
        Instrumentation instrumentation = holder.get();
        if (instrumentation == null) {
            try {
                block.action();

                return;
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }

        var measure = new Measurement(message);
        measure.start();
        try {
            block.action();
        } catch (Exception e) {
            sneakyThrow(e);
        } finally {
            measure.stop();
        }

        instrumentation.measurements.add(measure);
    }

    public static <T> CompletableFuture<T> measure(Supplier<CompletableFuture<T>> future, String message) {
        Instrumentation inst = holder.get();

        if (inst == null) {
            return future.get();
        }

        var measure = new Measurement(message);
        measure.start();

        CompletableFuture<T> fut = future.get();

        if (fut.isDone()) {
            measure.stop();
            inst.measurements.add(measure);
            return fut;
        }

        return fut.thenApply(v -> {
            measure.stop();
            if (isStarted()) {
                inst.measurements.add(measure);
            }
            return v;
        });
    }

    public interface Action<T> {
        T action() throws Exception;
    }

    public interface VoidAction {
        void action() throws Exception;
    }

    public static class Measurement extends Event implements Comparable<Measurement> {
        private long start;
        private long end;

        @Label("Message")
        private String message;

        public Measurement(String message) {
            this.message = message + ":" + Thread.currentThread().getName();
        }

        public void start() {
            begin();

            start = System.nanoTime();
        }

        public void stop() {
            end();

            end = System.nanoTime();
        }

        @Override
        public int compareTo(@NotNull Instrumentation.Measurement o) {
            return Long.compare(start, o.start);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Measurement that = (Measurement) o;
            return start == that.start && end == that.end && message.equals(that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end, message);
        }
    }
}
