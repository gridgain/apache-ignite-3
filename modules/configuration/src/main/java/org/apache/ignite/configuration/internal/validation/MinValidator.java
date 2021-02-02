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

package org.apache.ignite.configuration.internal.validation;

import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.FieldValidator;

/**
 * Validate that field value is not less than some minimal value.
 *
 * @param <C> Root configuration type.
 */
public class MinValidator<C extends DynamicConfiguration<?, ?, ?>> extends FieldValidator<Number, C> {
    /** Minimal value. */
    private final long minValue;

    /** Constructor. */
    public MinValidator(long minValue, String message) {
        super(message);
        this.minValue = minValue;
    }

    /** {@inheritDoc} */
    @Override public void validate(Number value, C newRoot, C oldRoot) throws ConfigurationValidationException {
        if (value.longValue() < minValue)
            throw new ConfigurationValidationException(message);
    }
}
