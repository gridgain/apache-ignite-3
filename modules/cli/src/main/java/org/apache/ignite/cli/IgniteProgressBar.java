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

package org.apache.ignite.cli;

import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine.Help.Ansi;

/**
 * Basic implementation of a progress bar.
 */
public class IgniteProgressBar implements AutoCloseable {
    private final PrintWriter out;

    private int current;

    private int max;

    private ScheduledExecutorService exec;

    /**
     * Creates a new progress bar.
     *
     * @param initialMax Initial maximum number of steps.
     */
    public IgniteProgressBar(PrintWriter out, int initialMax) {
        this.out = out;

        assert initialMax > 0;

        max = initialMax;
    }

    /**
     * Performs a single step.
     */
    public void step() {
        if (current < max)
            current++;

        out.print('\r' + render());
        out.flush();
    }

    /**
     * Performs a single step every N milliseconds.
     *
     * @param interval Interval in milliseconds.
     */
    public void stepPeriodically(long interval) {
        if (exec == null) {
            exec = Executors.newSingleThreadScheduledExecutor();

            exec.scheduleAtFixedRate(this::step, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Updates maximum number of steps.
     *
     * @param newMax New maximum.
     */
    public void setMax(int newMax) {
        assert newMax > 0;

        max = newMax;
    }

    @Override public void close() {
        while (current < max) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }

            step();
        }

        out.println();
    }

    private String render() {
        assert current <= max;

        int completed = (int)((double)current / (double)max * 100);

        StringBuilder sb = new StringBuilder("|");

        sb.append("=".repeat(completed));

        String percentage;
        int percentageLen;

        if (completed < 100) {
            sb.append('>').append(" ".repeat(99 - completed));

            percentage = completed + "%";
            percentageLen = percentage.length();
        }
        else {
            percentage = "@|green,bold Done!|@";
            percentageLen = 5;
        }

        sb.append("|").append(" ".repeat(6 - percentageLen)).append(percentage);

        return Ansi.AUTO.string(sb.toString());
    }
}
