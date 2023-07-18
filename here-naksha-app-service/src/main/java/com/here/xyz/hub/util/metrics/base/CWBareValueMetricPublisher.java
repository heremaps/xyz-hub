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

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CWBareValueMetricPublisher extends CloudWatchMetricPublisher<Collection<Double>> {

  private static final Logger logger = LogManager.getLogger();

  public CWBareValueMetricPublisher(Metric metric) {
    super(metric);
  }

  @Override
  protected void publishValues(Collection<Double> values) {
    if (values == null || values.isEmpty()) {
      logger.debug("Nothing to publish for metric {}", getMetricName());
      return;
    }
    publishValues(new MetricDatum().withValues(values));
  }
}
