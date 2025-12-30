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

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.metrics.base.Metric.MetricUnit;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CloudWatchMetricPublisher<V> extends MetricPublisher<V> {

  private static final Logger logger = LogManager.getLogger();
  private static final int MAX_DATA_BATCH_SIZE = 20;

  private static CloudWatchClient _client;

  protected static final String namespace = "XYZ/Hub";
  private static final String mainDimensionName = "ServiceName";

  private volatile Dimension mainDimension;
  private volatile StandardUnit unit;

  public CloudWatchMetricPublisher(Metric metric) {
    super(metric, 30);
    mainDimension = Dimension.builder()
        .name(mainDimensionName)
        .value("XYZ-Hub-" + Service.configuration.ENVIRONMENT_NAME)
        .build();
    this.unit = mapUnit(metric.getUnit());
  }

  @Override
  protected abstract void publishValues(V values);

  protected void publishValues(MetricDatum datum) {
    publishValues(Collections.singletonList(datum));
  }

  protected void publishValues(List<MetricDatum> data) {
    if (data.size() > MAX_DATA_BATCH_SIZE) {
      //TODO: Implement once GcDurationMetric was refactored to be an AggregatingMetric
    }

    List<MetricDatum> updatedData = data.stream()
        .map(datum -> datum.toBuilder()
            .metricName(getMetricName())
            .unit(unit)
            .dimensions(mainDimension)
            .build())
        .collect(java.util.stream.Collectors.toList());

    PutMetricDataRequest request = PutMetricDataRequest.builder()
        .namespace(namespace)
        .metricData(updatedData)
        .build();

    try {
      getClient().putMetricData(request);
    }
    catch (Exception e) {
      logger.error("Error publishing metric {}:", getMetricName(), e);
    }
  }

  private static StandardUnit mapUnit(MetricUnit unit) {
    switch (unit) {
      case COUNT: return StandardUnit.COUNT;
      case PERCENT: return StandardUnit.PERCENT;
      case BYTES: return StandardUnit.BYTES;
      case MILLISECONDS: return StandardUnit.MILLISECONDS;
      default: return StandardUnit.NONE;
    }
  }

  private static CloudWatchClient getClient() {
    if (_client == null)
      _client = CloudWatchClient.create();
    return _client;
  }
}
