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

package org.apache.ignite.internal.app;

import io.netty.util.internal.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.Ignition;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.impl.VaultServiceImpl;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.client.Condition;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.metastorage.client.Entry;
import org.apache.ignite.metastorage.client.Operation;
import org.apache.ignite.metastorage.client.WatchListener;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.utils.IgniteProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
public class IgnitionImpl implements Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgnitionImpl.class);

    /** */
    private static final String[] BANNER = new String[] {
        "",
        "           #              ___                         __",
        "         ###             /   |   ____   ____ _ _____ / /_   ___",
        "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
        "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  #######  ######            /_/",
        "    ########  ####        ____               _  __           _____",
        "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
        "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
        "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "       ##                  /____/\n"
    };

    /** */
    private static final String VER_KEY = "version";

    /** {@inheritDoc} */
    @Override public synchronized Ignite start(String jsonStrBootstrapCfg) {
        ackBanner();

        // Vault Component startup.
        VaultManager vaultMgr = new VaultManager(new VaultServiceImpl());

        boolean cfgBootstrappedFromPds = vaultMgr.bootstrapped();

        List<RootKey<?, ?>> rootKeys = Arrays.asList(
            NetworkConfiguration.KEY,
            NodeConfiguration.KEY,
            ClusterConfiguration.KEY,
            TablesConfiguration.KEY
        );

        List<ConfigurationStorage> configurationStorages =
            new ArrayList<>(Collections.singletonList(new LocalConfigurationStorage(vaultMgr)));

        // Bootstrap local configuration manager.
        ConfigurationManager locConfigurationMgr = new ConfigurationManager(rootKeys, configurationStorages);

        if (!cfgBootstrappedFromPds && jsonStrBootstrapCfg != null)
            try {
                locConfigurationMgr.bootstrap(jsonStrBootstrapCfg);
            }
            catch (Exception e) {
                LOG.warn("Unable to parse user specific configuration, default configuration will be used", e);
            }
        else if (jsonStrBootstrapCfg != null)
            LOG.warn("User specific configuration will be ignored, cause vault was bootstrapped with pds configuration");
        else
            locConfigurationMgr.configurationRegistry().startStorageConfigurations(ConfigurationType.LOCAL);

        NetworkView netConfigurationView =
            locConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).value();

        var serializationRegistry = new MessageSerializationRegistry();

        String localNodeName = locConfigurationMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .name().value();

        assert !StringUtil.isNullOrEmpty(localNodeName) : "Node local name is empty";

        // Network startup.
        ClusterService clusterNetSvc = new ScaleCubeClusterServiceFactory().createClusterService(
            new ClusterLocalConfiguration(
                localNodeName,
                netConfigurationView.port(),
                Arrays.asList(netConfigurationView.netClusterNodes()),
                serializationRegistry
            )
        );

        clusterNetSvc.start();

        // Raft Component startup.
        Loza raftMgr = new Loza(clusterNetSvc);

        // MetaStorage Component startup.
        MetaStorageManager metaStorageMgr = new MetaStorageManager(
            vaultMgr,
            clusterNetSvc,
            raftMgr,
            metaStorageServiceMock()
        );

        // TODO IGNITE-14578 Bootstrap configuration manager with distributed configuration.
        configurationStorages.add(new DistributedConfigurationStorage(metaStorageMgr));

        // Start configuration manager.
        ConfigurationManager configurationMgr = new ConfigurationManager(rootKeys, configurationStorages);

        // Baseline manager startup.
        BaselineManager baselineMgr = new BaselineManager(configurationMgr, metaStorageMgr, clusterNetSvc);

        // Affinity manager startup.
        new AffinityManager(configurationMgr, metaStorageMgr, baselineMgr, vaultMgr);

        SchemaManager schemaMgr = new SchemaManager(configurationMgr);

        // Distributed table manager startup.
        IgniteTables distributedTblMgr = new TableManager(
            configurationMgr,
            metaStorageMgr,
            schemaMgr,
            raftMgr,
            vaultMgr
        );

        // TODO IGNITE-14579 Start rest manager.

        // Deploy all resisted watches cause all components are ready and have registered their listeners.
        metaStorageMgr.deployWatches();

        ackSuccessStart();

        return new IgniteImpl(distributedTblMgr);
    }

    /** */
    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    /** */
    private static void ackBanner() {
        String ver = IgniteProperties.get(VER_KEY);

        String banner = String.join("\n", BANNER);

        LOG.info(banner + '\n' + " ".repeat(22) + "Apache Ignite ver. " + ver + '\n');
    }

    // TODO: remove when metastorage service will be ready.
    private static MetaStorageService metaStorageServiceMock() {
        return new MetaStorageService() {
            @Override public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Collection<ByteArray> keys) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Collection<ByteArray> keys, long revUpperBound) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] value) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, @NotNull byte[] value) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<ByteArray> keys) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override
            public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Collection<ByteArray> keys) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Boolean> invoke(@NotNull Condition condition,
                @NotNull Operation success, @NotNull Operation failure) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Boolean> invoke(@NotNull Condition condition,
                                                                        @NotNull Collection<Operation> success,
                                                                        @NotNull Collection<Operation> failure
            ) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<IgniteUuid> watch(@Nullable ByteArray keyFrom, @Nullable ByteArray keyTo,
                long revision, @NotNull WatchListener lsnr) {
                return CompletableFuture.completedFuture(new IgniteUuid(UUID.randomUUID(), 0L));
            }

            @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull ByteArray key, long revision,
                @NotNull WatchListener lsnr) {
                return CompletableFuture.completedFuture(new IgniteUuid(UUID.randomUUID(), 0L));
            }

            @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Collection<ByteArray> keys, long revision,
                @NotNull WatchListener lsnr) {
                return CompletableFuture.completedFuture(new IgniteUuid(UUID.randomUUID(), 0L));
            }

            @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }

            @Override public @NotNull CompletableFuture<Void> compact() {
                throw new UnsupportedOperationException("Meta storage service is not implemented yet");
            }
        };
    }
}
