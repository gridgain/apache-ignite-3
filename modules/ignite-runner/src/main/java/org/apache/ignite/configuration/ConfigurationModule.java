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

package org.apache.ignite.configuration;

import java.io.Reader;

import org.apache.ignite.configuration.extended.ChangeLocal;
import org.apache.ignite.configuration.extended.InitLocal;
import org.apache.ignite.configuration.extended.LocalConfigurationImpl;
import org.apache.ignite.configuration.extended.Selectors;
import org.apache.ignite.configuration.presentation.FormatConverter;
import org.apache.ignite.configuration.presentation.json.JsonConverter;
import org.apache.ignite.configuration.storage.ConfigurationStorage;

/**
 * Module is responsible for preparing configuration when module is started.
 *
 * Preparing configuration includes reading it from configuration file, parsing it and initializing
 * {@link Configurator} object.
 */
public class ConfigurationModule {
    static {
        try {
            Selectors.LOCAL_BASELINE.select(null);
        }
        catch (Throwable ignored) {
            // No-op.
        }
    }

    /** */
    private Configurator<LocalConfigurationImpl> localConfigurator;

    /** */
    private final SystemConfiguration sysConf = new SystemConfiguration();

    /** */
    public void bootstrap(Reader confReader, ConfigurationStorage storage) {
        FormatConverter converter = new JsonConverter();

        Configurator<LocalConfigurationImpl> configurator =
            Configurator.create(storage, LocalConfigurationImpl::new, converter.convertFrom(confReader, "local", InitLocal.class));

        localConfigurator = configurator;

        sysConf.registerConfigurator(
            configurator,
            s -> configurator.getPublic(Selectors.find(s)),
            s -> {
                ChangeLocal chLoc = converter.convertFrom(s, "local", ChangeLocal.class);

                configurator.set(Selectors.LOCAL, chLoc);

                return null;
            }
        );
    }

    /** */
    public Configurator<LocalConfigurationImpl> localConfigurator() {
        return localConfigurator;
    }

    /** */
    public SystemConfiguration systemConfiguration() {
        return sysConf;
    }
}
