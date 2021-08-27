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

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.here.xyz.hub.util.metrics.base.AggregatingMetric.AggregatedValues;
import com.here.xyz.hub.util.metrics.base.AttributedMetricCollection.Attribute;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CWAttributedMetricCollectionPublisher<V> extends CloudWatchMetricPublisher<Map<Collection<Attribute>, V>> {

  private static final Logger logger = LogManager.getLogger();

  public CWAttributedMetricCollectionPublisher(Metric metric) {
    super(metric);
  }

  @Override
  protected void publishValues(Map<Collection<Attribute>, V> values) {
    List<MetricDatum> metricData = values
        .entrySet()
        .stream().map(e -> convertToMetricDatum(e.getKey(), e.getValue()))
        .filter(datum -> datum != null)
        .collect(Collectors.toList());
    if (metricData.size() == 0) {
      logger.debug("Nothing to publish for metric {}", getMetricName());
      return;
    }
    publishValues(metricData);
  }

  private MetricDatum convertToMetricDatum(Collection<Attribute> attributes, V value) {
    //Set all dimensions using the attributes
    MetricDatum datum = new MetricDatum()
        .withDimensions(attributes
            .stream()
            .map(attr -> new Dimension().withName(attr.key.toString()).withValue(attr.value.toString()))
            .collect(Collectors.toList()));

    //Convert the metric value
    if (value instanceof AggregatedValues) {
      AggregatedValues aggregatedValues = (AggregatedValues) value;
      if (aggregatedValues.sampleCount == 0)
        return null;
      return datum
          .withStatisticValues(CWAggregatedValuesPublisher.toStatisticSet(aggregatedValues));
    }
    else if (value instanceof Collection) {
      if (((Collection<?>) value).size() == 0)
        return null;
      return datum
          .withValues((Collection<Double>) value);
    }
    else
      throw new IllegalArgumentException("Can not publish metric value of type " + value.getClass().getSimpleName());
  }
}
