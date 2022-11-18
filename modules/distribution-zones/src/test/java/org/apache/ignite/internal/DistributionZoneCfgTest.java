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

package org.apache.ignite.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DistributionZoneCfg}.
 */
class DistributionZoneCfgTest {
    private static final String ZONE_NAME = "zone1";

    @Test
    public void testDefaultValues() {
        DistributionZoneCfg zoneCfg = new DistributionZoneCfg.Builder().name(ZONE_NAME).build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjust());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjust() {
        DistributionZoneCfg zoneCfg = new DistributionZoneCfg.Builder()
                .name(ZONE_NAME)
                .dataNodesAutoAdjust(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(100, zoneCfg.dataNodesAutoAdjust());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjustScaleUp() {
        DistributionZoneCfg zoneCfg = new DistributionZoneCfg.Builder()
                .name(ZONE_NAME)
                .dataNodesAutoAdjustScaleUp(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjust());
        assertEquals(100, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testAutoAdjustScaleDown() {
        DistributionZoneCfg zoneCfg = new DistributionZoneCfg.Builder()
                .name(ZONE_NAME)
                .dataNodesAutoAdjustScaleDown(100)
                .build();

        assertEquals(ZONE_NAME, zoneCfg.name());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjust());
        assertEquals(Integer.MAX_VALUE, zoneCfg.dataNodesAutoAdjustScaleUp());
        assertEquals(100, zoneCfg.dataNodesAutoAdjustScaleDown());
    }

    @Test
    public void testNullZoneName() {
        assertThrows(NullPointerException.class,
                () -> new DistributionZoneCfg.Builder().build(),
                "name is null");
    }

    @Test
    public void testIncompatibleValues1() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneCfg.Builder()
                        .name(ZONE_NAME)
                        .dataNodesAutoAdjust(1)
                        .dataNodesAutoAdjustScaleUp(1)
                        .build());
    }

    @Test
    public void testIncompatibleValues2() {
        assertThrows(IllegalArgumentException.class,
                () -> new DistributionZoneCfg.Builder()
                        .name(ZONE_NAME)
                        .dataNodesAutoAdjust(1)
                        .dataNodesAutoAdjustScaleDown(1)
                        .build());
    }
}
