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

package com.here.xyz.hub.util.metrics.base;

import com.here.xyz.hub.util.metrics.base.AggregatingMetric.AggregatedValues;
import java.util.concurrent.atomic.AtomicReference;

public class AggregatingMetric extends Metric<AggregatedValues> {

  private final AtomicReference<AggregatedValues> values = new AtomicReference<>(new AggregatedValues());

  public AggregatingMetric(String metricName, MetricUnit unit) {
    super(metricName, unit);
  }

  /**
   * Aggregates a new value into this metric.
   * @param value The value to add
   */
  public void addValue(double value) {
    /*
    Atomically incorporates the value either to the current values-object or to the new one - in case it
    has just been swapped by #gatherValues().
     */
    values.getAndUpdate(currentValue -> {
      AggregatedValues newValue = new AggregatedValues();
      if (currentValue.sampleCount > 0) {
        newValue.minimum = Math.min(value, currentValue.minimum);
        newValue.maximum = Math.max(value, currentValue.maximum);
      }
      else
        newValue.minimum = newValue.maximum = value;
      newValue.sum = currentValue.sum + value;
      newValue.sampleCount = currentValue.sampleCount + 1;
      return newValue;
    });
  }

  @Override
  protected AggregatedValues gatherValues() {
    AggregatedValues v = values.getAndSet(new AggregatedValues());
    if (v.sampleCount == 0) return null; //Nothing to publish
    return v;
  }

  public static class AggregatedValues {
    public double minimum;
    public double maximum;
    public double sum;
    public double sampleCount;
  }
}
