/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.util.metrics;

import static com.here.xyz.hub.util.metrics.base.Metric.MetricUnit.PERCENT;

import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.util.metrics.base.BareValuesMetric;
import java.util.Collection;
import java.util.Collections;

public class GlobalUsedRfcConnections extends BareValuesMetric {

  public GlobalUsedRfcConnections(String metricName) {
    super(metricName, PERCENT);
  }

  @Override
  protected Collection<Double> gatherValues() {
    return Collections.singleton((double) RemoteFunctionClient.getGlobalUsedConnectionsPercentage() * 100d);
  }
}
