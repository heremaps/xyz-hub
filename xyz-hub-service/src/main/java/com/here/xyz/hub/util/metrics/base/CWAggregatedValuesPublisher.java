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

package com.here.xyz.hub.util.metrics.base;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.here.xyz.hub.util.metrics.base.AggregatingMetric.AggregatedValues;

public class CWAggregatedValuesPublisher extends CloudWatchMetricPublisher<AggregatedValues> {

  public CWAggregatedValuesPublisher(Metric metric) {
    super(metric);
  }

  @Override
  protected void publishValues(AggregatedValues values) {
    publishValues(new MetricDatum().withStatisticValues(toStatisticSet(values)));
  }

  public static StatisticSet toStatisticSet(AggregatedValues values) {
    return new StatisticSet()
        .withMinimum(values.minimum)
        .withMaximum(values.maximum)
        .withSum(values.sum)
        .withSampleCount(values.sampleCount);
  }
}
