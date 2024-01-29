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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileView;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileStorageEngineConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Storage engine implementation based on {@link PageMemory} for in-memory case.
 */
public class VolatilePageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "aimem";

    private static final IgniteLogger LOG = Loggers.forClass(VolatilePageMemoryStorageEngine.class);

    private final String igniteInstanceName;

    private final StoragesConfiguration storagesConfiguration;

    private final VolatilePageMemoryProfileStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final PageEvictionTracker pageEvictionTracker;

    private final Map<String, VolatilePageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    private volatile GradualTaskExecutor destructionExecutor;

    /**
     * Constructor.
     *
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     * @param pageEvictionTracker Eviction tracker to use.
     */
    public VolatilePageMemoryStorageEngine(
            String igniteInstanceName,
            VolatilePageMemoryProfileStorageEngineConfiguration engineConfig,
            StoragesConfiguration storagesConfiguration,
            PageIoRegistry ioRegistry,
            PageEvictionTracker pageEvictionTracker) {
        this.igniteInstanceName = igniteInstanceName;
        this.engineConfig = engineConfig;
        this.storagesConfiguration = storagesConfiguration;
        this.ioRegistry = ioRegistry;
        this.pageEvictionTracker = pageEvictionTracker;
    }

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void start() throws StorageException {
        storagesConfiguration.profiles().value().stream().forEach(p -> {
            if (p instanceof VolatilePageMemoryProfileView) {
                addDataRegion(p.name());
            }
        });

        ThreadPoolExecutor destructionThreadPool = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors(),
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(igniteInstanceName, "volatile-mv-partition-destruction", LOG)
        );
        destructionExecutor = new GradualTaskExecutor(destructionThreadPool);
    }

    @Override
    public void stop() throws StorageException {
        destructionExecutor.close();

        try {
            closeAll(regions.values().stream().map(region -> region::stop));
        } catch (Exception e) {
            throw new StorageException("Error when stopping components", e);
        }
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public VolatilePageMemoryTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        VolatilePageMemoryDataRegion dataRegion = regions.get(tableDescriptor.getDataRegion());

        assert dataRegion != null : "tableId=" + tableDescriptor.getId() + ", dataRegion=" + tableDescriptor.getDataRegion();

        return new VolatilePageMemoryTableStorage(tableDescriptor, indexDescriptorSupplier, dataRegion, destructionExecutor);
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param name Data region name.
     */
    private void addDataRegion(String name) {
        VolatilePageMemoryProfileConfiguration storageProfileConfiguration =
                (VolatilePageMemoryProfileConfiguration) storagesConfiguration.profiles().get(name);

        int pageSize = engineConfig.pageSize().value();

        VolatilePageMemoryDataRegion dataRegion = new VolatilePageMemoryDataRegion(
                storageProfileConfiguration,
                ioRegistry,
                pageSize,
                pageEvictionTracker
        );

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
