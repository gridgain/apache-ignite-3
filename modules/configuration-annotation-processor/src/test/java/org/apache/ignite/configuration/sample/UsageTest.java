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

package org.apache.ignite.configuration.sample;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.SystemConfiguration;
import org.apache.ignite.configuration.internal.NamedList;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.ignite.configuration.PublicConfigurator;

/**
 * Simple usage test of generated configuration schema.
 */
public class UsageTest {
    private final ConfigurationStorage storage = new ConfigurationStorage() {
        @Override public <T extends Serializable> void save(String propertyName, T object) throws StorageException {

        }

        @Override public <T extends Serializable> T get(String propertyName) throws StorageException {
            return null;
        }

        @Override
        public <T extends Serializable> void listen(String key, Consumer<T> listener) throws StorageException {

        }
    };

    /**
     * Test creation of configuration and calling configuration API methods.
     */
    @Test
    public void test() {
        InitLocal initLocal = new InitLocal().withBaseline(
            new InitBaseline()
                .withNodes(
                    new NamedList<>(
                        Collections.singletonMap("node1", new InitNode().withConsistentId("test").withPort(1000))
                    )
                )
                .withAutoAdjust(new InitAutoAdjust().withEnabled(true).withTimeout(100000L))
        );

        final Configurator<LocalConfigurationImpl> configurator = Configurator.create(
            storage,
            LocalConfigurationImpl::new,
            initLocal
        );

        final LocalConfiguration root = configurator.getRoot();
        root.baseline().autoAdjust().enabled().value();

        try {
            configurator.set(Selectors.LOCAL_BASELINE_AUTO_ADJUST_ENABLED, false);
            Assertions.fail();
        } catch (ConfigurationValidationException e) {}
        configurator.set(Selectors.LOCAL_BASELINE_AUTO_ADJUST, new ChangeAutoAdjust().withEnabled(false).withTimeout(0L));
        configurator.getRoot().baseline().nodes().get("node1").autoAdjustEnabled(false);
        configurator.getRoot().baseline().autoAdjust().enabled(true);
        configurator.getRoot().baseline().nodes().get("node1").autoAdjustEnabled(true);

        try{
            configurator.getRoot().baseline().autoAdjust().enabled(false);
            Assertions.fail();
        } catch (ConfigurationValidationException e) {}

        PublicConfigurator<LocalConfiguration> con = new PublicConfigurator<>(configurator);
    }

    /**
     * Test to show an API to work with multiroot configurations.
     */
    @Test
    public void multiRootConfigurationTest() {
        SystemConfiguration sysConf = new SystemConfiguration();

        InitNetwork initNetwork = new InitNetwork().withDiscovery(
            new InitDiscovery()
                .withFailureDetectionTimeout(30_000)
                .withJoinTimeout(10_000)
        );

        InitLocal initLocal = new InitLocal().withBaseline(
            new InitBaseline().withAutoAdjust(
                new InitAutoAdjust().withEnabled(true)
                    .withTimeout(30_000L))
        );

        Configurator<LocalConfigurationImpl> localConf = Configurator.create(storage,
            LocalConfigurationImpl::new, initLocal);

        sysConf.registerConfigurator(localConf);

        Configurator<NetworkConfigurationImpl> networkConf = Configurator.create(storage,
            NetworkConfigurationImpl::new, initNetwork);

        sysConf.registerConfigurator(networkConf);

        sysConf.getConfiguration(NetworkConfigurationImpl.KEY).discovery().failureDetectionTimeout();
    }
}
