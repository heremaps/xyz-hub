/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class AuroraAcuMonitor {
  private static final Logger logger = LogManager.getLogger();
  private static final long PERIOD_SEC = 60;

  private final String clusterId;
  private final Region region;
  private final CloudWatchClient cloudWatchClient;
  private final ScheduledFuture<?> task;

  private volatile double utilization = 0;

  public AuroraAcuMonitor(String clusterId, Region region, ScheduledExecutorService scheduler, CloudWatchClient cloudWatchClient) {
    this.clusterId = clusterId;
    this.region = region;
    this.cloudWatchClient = cloudWatchClient;
    this.task = scheduler.scheduleAtFixedRate(this::update, 0, PERIOD_SEC, TimeUnit.SECONDS);
  }

  public double getUtilization() {
    return utilization;
  }

  public void stop() {
    task.cancel(false);
  }

  private void update() {
    try {
      Metric metric = Metric.builder()
          .namespace("AWS/RDS")
          .metricName("ACUUtilization")
          .dimensions(Dimension.builder()
              .name("DBClusterIdentifier").value(clusterId).build())
          .build();

      MetricDataQuery query = MetricDataQuery.builder()
          .id("u")
          .metricStat(MetricStat.builder().metric(metric)
              .period((int) PERIOD_SEC).stat("Average").build())
          .returnData(true)
          .build();

      GetMetricDataResponse resp = cloudWatchClient.getMetricData(GetMetricDataRequest.builder()
          .metricDataQueries(query)
          .startTime(Instant.now().minusSeconds(PERIOD_SEC * 2))
          .endTime(Instant.now())
          .build());

      resp.metricDataResults().stream()
          .flatMap(r -> r.values().stream()
              .map(v -> new MetricPoint(v, r.timestamps().get(r.values().indexOf(v)))))
          .max(Comparator.comparing(p -> p.ts))
          .ifPresent(p -> utilization = p.val);

    } catch (Exception e) {
      logger.warn("ACU poll failed for {} in {}", clusterId, region, e);
      utilization = -1;
    }
  }

  private static class MetricPoint {
    private final double val;
    private final Instant ts;

    public MetricPoint(double val, Instant ts) {
      this.val = val;
      this.ts = ts;
    }

    public double val() {
      return val;
    }

    public Instant ts() {
      return ts;
    }
  }
}
