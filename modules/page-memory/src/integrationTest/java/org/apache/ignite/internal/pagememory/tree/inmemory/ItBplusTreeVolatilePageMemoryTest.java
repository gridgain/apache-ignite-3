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

package org.apache.ignite.internal.pagememory.tree.inmemory;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.AbstractBplusTreePageMemoryTest;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class to test the {@link BplusTree} with {@link VolatilePageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreeVolatilePageMemoryTest extends AbstractBplusTreePageMemoryTest {
    @InjectConfiguration(polymorphicExtensions = { VolatilePageMemoryProfileConfigurationSchema.class }, value = "mock.engine = aimem")
    private StorageProfileConfiguration dataRegionCfg;

    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() throws Exception {
        dataRegionCfg
                .change(c -> ((VolatilePageMemoryProfileChange) c)
                        .changeInitSize(MAX_MEMORY_SIZE)
                        .changeMaxSize(MAX_MEMORY_SIZE))
                .get(1, TimeUnit.SECONDS);

        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                (VolatilePageMemoryProfileConfiguration) ((DynamicConfiguration) dataRegionCfg).specificConfigTree(),
                ioRegistry,
                PAGE_SIZE
        );
    }

    /** {@inheritDoc} */
    @Override
    protected long acquiredPages() {
        return ((VolatilePageMemory) pageMem).acquiredPages();
    }

    /** {@inheritDoc} */
    @Override
    protected @Nullable ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) {
        return null;
    }
}
