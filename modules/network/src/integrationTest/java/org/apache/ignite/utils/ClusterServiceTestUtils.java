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

package org.apache.ignite.utils;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

public class ClusterServiceTestUtils {
    /**
     * Creates a cluster service.
     */
    public static ClusterService clusterService(String nodeName, int port, NodeFinder nodeFinder, MessageSerializationRegistry msgSerializationRegistry, ClusterServiceFactory clusterServiceFactory) {
        var ctx = new ClusterLocalConfiguration(nodeName, msgSerializationRegistry);

        ConfigurationManager nodeConfigurationMgr = new ConfigurationManager(
            Collections.singleton(NetworkConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(ConfigurationType.LOCAL)
        );

        var clusterSvc = clusterServiceFactory.createClusterService(
            ctx,
            nodeConfigurationMgr,
            () -> nodeFinder
        );

        var clusterSvcWrapper = new ClusterService() {
            @Override public TopologyService topologyService() {
                return clusterSvc.topologyService();
            }

            @Override public MessagingService messagingService() {
                return clusterSvc.messagingService();
            }

            @Override public ClusterLocalConfiguration localConfiguration() {
                return clusterSvc.localConfiguration();
            }

            @Override public boolean isStopped() {
                return clusterSvc.isStopped();
            }

            @Override public void start() {
                nodeConfigurationMgr.start();

                nodeConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).
                    change(netCfg -> netCfg.changePort(port)).join();

                clusterSvc.start();
            }

            @Override public void stop() {
                clusterSvc.stop();
                nodeConfigurationMgr.stop();
            }
        };

        clusterSvcWrapper.start();

        return clusterSvcWrapper;
    }
}
