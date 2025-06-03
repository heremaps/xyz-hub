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

package com.here.xyz.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.AuroraAcuMonitor;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;

public class AuroraAcuMonitorTest {

  private static class DummyCloudWatchClient implements CloudWatchClient {

    @Override
    public GetMetricDataResponse getMetricData(GetMetricDataRequest request) {
      MetricDataResult result = MetricDataResult.builder()
          .id("u")
          .values(Collections.singletonList(50.0))
          .timestamps(Collections.singletonList(Instant.now()))
          .build();
      return GetMetricDataResponse.builder()
          .metricDataResults(Collections.singletonList(result))
          .build();
    }

    @Override
    public void close() {}

    @Override
    public String serviceName() {
      return "DummyCloudWatchClient";
    }
  }

  @Test
  public void testMonitorUpdatesUtilization() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, new DummyCloudWatchClient());

    Thread.sleep(2000);
    double acuValue = monitor.getUtilization();
    assertTrue(acuValue >= 0, "ACU value should be non-negative");
    monitor.stop();
    executor.shutdown();
  }
}
