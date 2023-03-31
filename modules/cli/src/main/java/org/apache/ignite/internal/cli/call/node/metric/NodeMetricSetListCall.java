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

package org.apache.ignite.internal.cli.call.node.metric;

import jakarta.inject.Singleton;
import java.util.List;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.NodeMetricApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.MetricSet;

/** Lists node metric sets. */
@Singleton
public class NodeMetricSetListCall implements Call<UrlCallInput, List<MetricSet>> {
    private final ApiClientFactory clientFactory;

    public NodeMetricSetListCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    /** {@inheritDoc} */
    @Override
    public CallOutput<List<MetricSet>> execute(UrlCallInput input) {

        try {
            return DefaultCallOutput.success(listNodeMetricSets(input));
        } catch (ApiException | IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getUrl()));
        }
    }

    private List<MetricSet> listNodeMetricSets(UrlCallInput input) throws ApiException {
        return new NodeMetricApi(clientFactory.getClient(input.getUrl())).listNodeMetricSets();
    }
}
