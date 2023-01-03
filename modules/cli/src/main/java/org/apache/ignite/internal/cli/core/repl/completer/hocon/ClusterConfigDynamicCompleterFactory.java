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

package org.apache.ignite.internal.cli.core.repl.completer.hocon;

import com.typesafe.config.Config;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.repl.completer.DummyCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.registry.ClusterConfigRegistry;

/** Factory for cluster config show/update command. */
@Singleton
public class ClusterConfigDynamicCompleterFactory implements DynamicCompleterFactory {

    private final ClusterConfigRegistry clusterConfigRegistry;

    public ClusterConfigDynamicCompleterFactory(ClusterConfigRegistry clusterConfigRegistry) {
        this.clusterConfigRegistry = clusterConfigRegistry;
    }

    @Override
    public DynamicCompleter getDynamicCompleter(String[] words) {
        Config config = clusterConfigRegistry.config();
        if (config != null) {
            return new HoconDynamicCompleter(config);
        } else {
            return new DummyCompleter();
        }
    }
}
