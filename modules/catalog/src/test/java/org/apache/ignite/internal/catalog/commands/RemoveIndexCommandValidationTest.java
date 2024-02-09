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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;

import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Tests to verify validation of {@link RemoveIndexCommand}. */
public class RemoveIndexCommandValidationTest extends AbstractChangeIndexStatusCommandValidationTest {
    @Override
    CatalogCommand createCommand(int indexId) {
        return RemoveIndexCommand.builder().indexId(indexId).build();
    }

    @Override
    boolean isInvalidPreviousIndexStatus(CatalogIndexStatus indexStatus) {
        return indexStatus != STOPPING;
    }

    @Override
    Class<? extends Exception> expectedExceptionClassForWrongStatus() {
        return CatalogValidationException.class;
    }

    @Override
    String expectedExceptionMessageSubstringForWrongStatus() {
        return "Cannot remove index";
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20121")
    @Test
    @Override
    void exceptionIsThrownIfIndexWithGivenIdNotFound() {
        super.exceptionIsThrownIfIndexWithGivenIdNotFound();
    }
}
