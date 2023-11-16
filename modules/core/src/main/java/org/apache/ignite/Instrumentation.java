package org.apache.ignite;

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import jdk.jfr.AnnotationElement;
import jdk.jfr.Event;
import jdk.jfr.EventFactory;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.ValueDescriptor;
import org.jetbrains.annotations.NotNull;

public class Instrumentation {

    private static volatile boolean firstRun = true;

    private static volatile boolean inInstrumentation = false;

    private static volatile boolean jfr = false;

//    private static final Path file = Path.of("/Users/kgusakov/Projects/ignite-3/results.txt");
    private static final Path file = Path.of("/opt/pubagent/poc/log/stats/dstat/trace.txt");

    private static volatile BufferedWriter writer;

    private static volatile ConcurrentLinkedQueue<Measurement> measurements = new ConcurrentLinkedQueue<>();

    private static volatile long startTime = System.nanoTime();

    public static void start(boolean jfr) {
        Instrumentation.jfr = jfr;
        inInstrumentation = true;
        if (firstRun && !jfr) {
            try {
                Files.deleteIfExists(file);
                file.toFile().createNewFile();
                writer = new BufferedWriter(new FileWriter(file.toFile(), true), 512 * 1024);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            firstRun = false;
        }

        startTime = System.nanoTime();
    }

    public static void end() {
        inInstrumentation = false;
        if (jfr) {
            endJfr();
        } else {
            endPrintln();
        }
    }

    private static void endPrintln() {
        var endTime = System.nanoTime();
        List<Measurement> m = new ArrayList<>(measurements);
        measurements.clear();
        m.sort(Measurement::compareTo);
        AtomicLong found = new AtomicLong(0l);
        var sb = new StringBuilder();
        AtomicLong previousEnd = new AtomicLong(0l);
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
            sb.append(measurement.message + " " + (measurement.end - measurement.start) / 1000.0 + " " + measurement.start % 10000000 + " " + measurement.end % 10000000 + "\n");
        });
        double wholeTime = (endTime - startTime);
        sb.append("Whole time: " + (endTime - startTime) / 1000.0 + " us" + "\n");
        sb.append(
                "Found time: " + found + " Not found time: " + (wholeTime - found.get()) + " Percentage of found: " + 100 * found.get() / wholeTime + "\n\n");
        try {
            writer.write(sb.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void endJfr() {
        List<Measurement> m = new ArrayList<>(measurements);
        m.forEach(Event::commit);
        measurements.clear();
    }

    public static void add(Measurement measurement) {
        measurements.add(measurement);
    }

    public static void mark(String message) {
        var measure = new Measurement(message);
        measure.start();
        measure.stop();
        measurements.add(measure);
    }


    public static <T> T measure(Action<T> block, String message) {
        var measure = new Measurement(message);
        measure.start();
        T results = null;
        try {
            results = block.action();
            measure.stop();
        } catch (Exception e) {
            sneakyThrow(e);
            measure.stop();
        }

        measurements.add(measure);

        return results;
    }

    public static void measure(VoidAction block, String message) {
        var measure = new Measurement(message);
        measure.start();
        try {
            block.action();
            measure.stop();
        } catch (Exception e) {
            sneakyThrow(e);
            measure.stop();
        }

        measurements.add(measure);
    }

    public static  void dumpStack() {
//        if (inInstrumentation)
//            Thread.dumpStack();
    }

    public static <T> CompletableFuture<T> measure(Supplier<CompletableFuture<T>> future, String message) {
        var measure = new Measurement(message);
        measure.start();
        return Optional.ofNullable(future.get()).orElse(CompletableFuture.completedFuture(null)).thenApply(v -> {
            measure.stop();
            measurements.add(measure);
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
            this.message = message;
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