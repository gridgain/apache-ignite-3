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

package org.apache.ignite.cli.commands.topology;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cluster.topology.LogicalTopologyCall;
import org.apache.ignite.cli.call.cluster.topology.TopologyCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.TopologyDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that show logical cluster topology.
 */
@Command(name = "logical")
public class LogicalTopologySubCommand extends BaseCommand implements Callable<Integer> {
    /**
     * Node url option.
     */
    @Option(names = {"--cluster-url"}, description = "Url to ignite node.", descriptionKey = "ignite.cluster-url")
    private String clusterUrl;

    @Inject
    private LogicalTopologyCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> TopologyCallInput.builder().clusterUrl(clusterUrl).build())
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new TopologyDecorator())
                .build()
                .runPipeline();
    }
}
