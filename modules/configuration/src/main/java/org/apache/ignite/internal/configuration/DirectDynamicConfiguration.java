/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.configuration;

import java.util.List;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.DirectAccess;

/**
 * {@link DynamicConfiguration} extension that implements {@link DirectConfigurationProperty}.
 *
 * @see DirectAccess
 */
public abstract class DirectDynamicConfiguration<VIEWT, CHANGET>
        extends DynamicConfiguration<VIEWT, CHANGET>
        implements DirectConfigurationProperty<VIEWT> {
    /**
     * Constructor.
     *
     * @param prefix     Configuration prefix.
     * @param key        Configuration key.
     * @param rootKey    Root key.
     * @param changer    Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     */
    public DirectDynamicConfiguration(
            List<String> prefix,
            String key,
            RootKey<?, ?> rootKey,
            DynamicConfigurationChanger changer,
            boolean listenOnly
    ) {
        super(prefix, key, rootKey, changer, listenOnly);
    }

    /** {@inheritDoc} */
    @Override
    public VIEWT directValue() {
        if (listenOnly) {
            throw listenOnlyException();
        }

        return changer.getLatest(keys);
    }
}
