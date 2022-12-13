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

package org.apache.ignite.internal.schema.configuration;

import com.google.auto.service.AutoService;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.HashIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.IndexValidator;
import org.apache.ignite.internal.schema.configuration.index.IndexValidatorImpl;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.storage.KnownDataStorage;
import org.apache.ignite.internal.schema.configuration.storage.KnownDataStorageValidator;
import org.apache.ignite.internal.schema.configuration.storage.UnknownDataStorageConfigurationSchema;

/**
 * {@link ConfigurationModule} for cluster-wide configuration provided by ignite-schema.
 */
@AutoService(ConfigurationModule.class)
public class SchemaDistributedConfigurationModule implements ConfigurationModule {
    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return List.of(TablesConfiguration.KEY);
    }

    /** {@inheritDoc} */
    @Override
    public Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators() {
        return Map.of(
                KnownDataStorage.class, Set.of(new KnownDataStorageValidator()),
                TableValidator.class, Set.of(TableValidatorImpl.INSTANCE),
                ColumnTypeValidator.class, Set.of(ColumnTypeValidatorImpl.INSTANCE),
                IndexValidator.class, Set.of(IndexValidatorImpl.INSTANCE)
        );
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                UnknownDataStorageConfigurationSchema.class,
                ConstantValueDefaultConfigurationSchema.class,
                FunctionCallDefaultConfigurationSchema.class,
                NullValueDefaultConfigurationSchema.class,
                HashIndexConfigurationSchema.class,
                SortedIndexConfigurationSchema.class
        );
    }
}
