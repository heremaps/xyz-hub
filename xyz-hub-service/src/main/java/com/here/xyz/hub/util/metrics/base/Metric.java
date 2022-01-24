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

public abstract class Metric<V> {

  private String metricName;
  private MetricUnit unit;

  public Metric(String metricName, MetricUnit unit) {
    this.metricName = metricName;
    this.unit = unit;
  }

  public String getName() {
    return metricName;
  }

  public MetricUnit getUnit() {
    return unit;
  }

  protected abstract V gatherValues();

  public enum MetricUnit {
    COUNT,
    PERCENT,
    BYTES,
    MILLISECONDS
  }
}
