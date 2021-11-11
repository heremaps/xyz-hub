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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.util.metrics.base.Metric.MetricUnit;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CloudWatchMetricPublisher<V> extends MetricPublisher<V> {

  private static final Logger logger = LogManager.getLogger();
  private static final int MAX_DATA_BATCH_SIZE = 20;

  private static String region;
  private static AmazonCloudWatch _client;

  protected static final String namespace = "XYZ/Hub";
  private static final String mainDimensionName = "ServiceName";
  Dimension dimension;
  StandardUnit unit;

  public CloudWatchMetricPublisher(Metric metric) {
    super(metric, 30);
    dimension = new Dimension()
        .withName(mainDimensionName)
        .withValue("XYZ-Hub-" + Service.configuration.ENVIRONMENT_NAME);
    this.unit = mapUnit(metric.getUnit());
    if (region == null)
      region = Service.configuration != null && Service.configuration.AWS_REGION != null ? Service.configuration.AWS_REGION : "none";
  }

  @Override
  protected abstract void publishValues(V values);

  protected void publishValues(MetricDatum datum) {
    publishValues(Collections.singletonList(datum));
  }

  protected void publishValues(List<MetricDatum> data) {
    if (data.size() > MAX_DATA_BATCH_SIZE) {

    }

    data.forEach(datum -> datum
        .withMetricName(getMetricName())
        .withUnit(unit)
        .withDimensions(dimension, new Dimension().withName("region").withValue(region)));

    PutMetricDataRequest request = new PutMetricDataRequest()
        .withNamespace(namespace)
        .withMetricData(data);

    try {
      getClient().putMetricData(request);
    }
    catch (Exception e) {
      logger.error("Error publishing metric {}:", getMetricName(), e);
    }
  }

  private static StandardUnit mapUnit(MetricUnit unit) {
    switch (unit) {
      case COUNT: return StandardUnit.Count;
      case PERCENT: return StandardUnit.Percent;
      case BYTES: return StandardUnit.Bytes;
      case MILLISECONDS: return StandardUnit.Milliseconds;
      default: return StandardUnit.None;
    }
  }

  private static AmazonCloudWatch getClient() {
    if (_client == null)
      _client = AmazonCloudWatchAsyncClientBuilder.defaultClient();
    return _client;
  }
}
