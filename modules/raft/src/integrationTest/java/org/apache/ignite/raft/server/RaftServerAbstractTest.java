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

package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessageSerializationFactory;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessageSerializationFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessage;
import org.apache.ignite.network.scalecube.message.ScaleCubeMessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;

/**
 * Abstract test for raft server.
 */
abstract class RaftServerAbstractTest {
    /**
     * The logger.
     */
    protected static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerAbstractTest.class);

    /**
     * The message factory.
     */
    protected static final RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /**
     * Server port offset.
     */
    protected static final int PORT = 20010;

    /**
     * The registry.
     */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry()
        .registerFactory(ScaleCubeMessage.TYPE, new ScaleCubeMessageSerializationFactory())
        .registerFactory(HandshakeStartMessage.TYPE, new HandshakeStartMessageSerializationFactory())
        .registerFactory(HandshakeStartResponseMessage.TYPE, new HandshakeStartResponseMessageSerializationFactory());

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<String> servers, boolean start) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        if (start)
            network.start();

        return network;
    }
}
