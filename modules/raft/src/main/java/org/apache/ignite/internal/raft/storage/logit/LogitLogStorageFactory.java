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

package org.apache.ignite.internal.raft.storage.logit;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.FeatureChecker;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.LogitLogStorage;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.jetbrains.annotations.TestOnly;
import sun.nio.ch.DirectBuffer;

/**
 * Log storage factory for {@link LogitLogStorage} instances.
 */
public class LogitLogStorageFactory implements LogStorageFactory {
    private static final IgniteLogger LOG = Loggers.forClass(LogitLogStorageFactory.class);

    private static final String LOG_DIR_PREFIX = "log-";

    /** Executor for shared storages. */
    private final ScheduledExecutorService checkpointExecutor;

    private final NodeOptions options;

    private final StoreOptions storeOptions;

    /** Base location of all log storages, created by this factory. */
    private Path logPath;

    /**
     * Constructor.
     *
     * @param options Node options with base location of all log storages, created by this factory.
     * @param storeOptions Logit log storage options.
     */
    public LogitLogStorageFactory(String nodeName, NodeOptions options, StoreOptions storeOptions) {
        this.options = options;
        this.storeOptions = storeOptions;
        checkpointExecutor = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "logit-checkpoint-executor", LOG)
        );

        checkVmOptions();
    }

    private static void checkVmOptions() {
        try {
            Class.forName(DirectBuffer.class.getName());
        } catch (Throwable e) {
            throw new IgniteInternalException("sun.nio.ch.DirectBuffer is unavailable." + FeatureChecker.JAVA_VER_SPECIFIC_WARN, e);
        }
    }

    @Override
    public void start() {
        this.logPath = options.getLogPath();
    }

    @Override
    public LogStorage createLogStorage(String groupId, RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(groupId), "Blank log storage uri.");

        Path storagePath = resolveLogStoragePath(groupId);

        return new LogitLogStorage(storagePath, storeOptions, raftOptions, checkpointExecutor);
    }

    @Override
    public void close() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(checkpointExecutor);
    }

    @Override
    public void sync() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21955
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** Returns base location of all log storages, created by this factory. */
    @TestOnly
    public Path logPath() {
        return logPath;
    }

    private Path resolveLogStoragePath(String groupId) {
        return logPath.resolve(LOG_DIR_PREFIX + groupId);
    }
}
