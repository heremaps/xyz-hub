/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.xyz.hub.util.metrics.net;

import static com.here.xyz.hub.util.metrics.base.Metric.MetricUnit.PERCENT;
import static com.here.xyz.hub.util.metrics.net.ConnectionMetrics.TARGET;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.metrics.base.AttributedMetricCollection.Attribute;
import com.here.xyz.hub.util.metrics.base.AttributedMetricCollector;
import com.here.xyz.hub.util.metrics.net.ConnectionMetrics.HubHttpClientMetrics;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

class HttpPoolUtilization extends AttributedMetricCollector<Collection<Double>> {

  HttpPoolUtilization() {
    super(HttpPoolUtilization.class.getSimpleName(), PERCENT);
  }

  @Override
  protected Map<Collection<Attribute>, Collection<Double>> gatherValues() {
    return HubHttpClientMetrics.endpointsConnected.entrySet().stream()
        .collect(Collectors.toMap(
            e -> Collections.singleton(new Attribute<>(TARGET, e.getKey())),
            e -> Collections.singleton(e.getValue().doubleValue()
                / Service.configuration.MAX_GLOBAL_HTTP_CLIENT_CONNECTIONS
                * 100)));
  }
}
