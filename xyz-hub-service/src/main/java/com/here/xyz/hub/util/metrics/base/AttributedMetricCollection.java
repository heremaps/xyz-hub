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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A delegating metric which consists of one or more attributed sub-metrics.
 * All sub-metrics belong to the same main-metric, so they must have the same name.
 * @param <V>
 */
public class AttributedMetricCollection<V> extends AttributedMetricCollector<V> {

  private Map<Collection<Attribute>, Metric<V>> metrics = new ConcurrentHashMap<>();

  public AttributedMetricCollection(String metricName, MetricUnit unit) {
    super(metricName, unit);
  }

  public void addMetric(Metric<V> metric, Attribute... attributes) {
    if (attributes.length == 0)
      throw new IllegalArgumentException("At least one attribute must be provided to add a metric");
    addMetric(metric, Arrays.asList(attributes));
  }

  public void addMetric(Metric<V> metric, Collection<Attribute> attributes) {
    if (!metric.getName().equals(getName()))
      throw new IllegalArgumentException("All metrics in the collection must have the same metric name.");
    metrics.put(attributes, metric);
  }

  @Override
  protected Map<Collection<Attribute>, V> gatherValues() {
    return metrics
        .entrySet()
        .stream()
        .collect(HashMap::new, (m, e) -> {
          final V metricValues = e.getValue().gatherValues();
          //Filter out metrics which have nothing to publish
          if (metricValues != null)
            m.put(e.getKey(), metricValues);
        }, HashMap::putAll);
  }

  public static class Attribute<K, V> {
    public Attribute(K key, V value) {
      this.key = key;
      this.value = value;
    }

    K key;
    V value;
  }
}
