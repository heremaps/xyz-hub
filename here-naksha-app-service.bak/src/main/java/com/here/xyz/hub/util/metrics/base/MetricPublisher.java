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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MetricPublisher<V> {

  private static final Logger logger = LogManager.getLogger();
  public static final long DEFAULT_REFRESH_PERIOD = 60; // s

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private long startWaitTime;
  private long refreshPeriod = DEFAULT_REFRESH_PERIOD;
  private volatile Metric metric;

  public MetricPublisher(Metric<V> metric) {
    this.metric = metric;
    executor.scheduleAtFixedRate(
        () -> {
          try {
            final V values = metric.gatherValues();
            if (values == null) {
              logger.debug("Nothing to publish for metric {}", getMetricName());
              return;
            }
            publishValues(values);
            logger.debug("Published values for metric {}", getMetricName());
          } catch (Exception e) {
            logger.warn("Error gathering / publishing metric values {}:", getMetricName(), e);
          }
        },
        startWaitTime,
        refreshPeriod,
        TimeUnit.SECONDS);
    logger.info("Started publishing metric {}", getMetricName());
  }

  public MetricPublisher(Metric metric, long startWaitTime) {
    this(metric);
    this.startWaitTime = startWaitTime;
  }

  public MetricPublisher(Metric metric, long startWaitTime, long refreshPeriod) {
    this(metric, startWaitTime);
    this.refreshPeriod = refreshPeriod;
  }

  abstract void publishValues(V values);

  public String getMetricName() {
    return metric.getName();
  }

  public void stop() {
    executor.shutdown();
    logger.info("Stopped publishing metric {}", getMetricName());
  }
}
