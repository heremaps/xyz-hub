/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.AuroraAcuMonitor;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;

public class AuroraAcuMonitorTest {

  private static class DummyCloudWatchClient implements CloudWatchClient {

    private final AtomicReference<GetMetricDataRequest> lastRequest = new AtomicReference<>();
    private final CountDownLatch requestLatch = new CountDownLatch(1);
    private final GetMetricDataResponse response;
    private final RuntimeException toThrow;

    DummyCloudWatchClient(GetMetricDataResponse response) {
      this.response = response;
      this.toThrow = null;
    }

    DummyCloudWatchClient(RuntimeException toThrow) {
      this.response = null;
      this.toThrow = toThrow;
    }

    @Override
    public GetMetricDataResponse getMetricData(GetMetricDataRequest request) {
      lastRequest.set(request);
      requestLatch.countDown();
      if (toThrow != null) {
        throw toThrow;
      }
      return response;
    }

    GetMetricDataRequest awaitRequest() throws InterruptedException {
      assertTrue(requestLatch.await(5, TimeUnit.SECONDS), "Expected monitor to poll CloudWatch");
      return lastRequest.get();
    }

    @Override
    public void close() {
    }

    @Override
    public String serviceName() {
      return "DummyCloudWatchClient";
    }
  }

  private static void awaitUtilization(AuroraAcuMonitor monitor, double expected) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 3000;
    while (System.currentTimeMillis() < deadline) {
      if (Math.abs(monitor.getUtilization() - expected) < 0.0001) {
        return;
      }
      Thread.sleep(25);
    }
    assertEquals(expected, monitor.getUtilization(), 0.0001, "Monitor utilization did not reach expected value");
  }

  @Test
  public void testMonitorUpdatesUtilization() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    DummyCloudWatchClient client = new DummyCloudWatchClient(
        responseWith(List.of(50.0), List.of(Instant.now())));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, client);
    try {
      client.awaitRequest();
      awaitUtilization(monitor, 50.0);
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testClusterOnlyMonitorUsesClusterDimension() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    DummyCloudWatchClient client = new DummyCloudWatchClient(
        responseWith(List.of(30.0), List.of(Instant.now())));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, client);
    try {
      GetMetricDataRequest req = client.awaitRequest();
      List<Dimension> dimensions = req.metricDataQueries().get(0).metricStat().metric().dimensions();
      assertEquals(1, dimensions.size());
      assertEquals("DBClusterIdentifier", dimensions.get(0).name());
      assertEquals("dummy-cluster", dimensions.get(0).value());
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testRoleMonitorUsesClusterAndRoleDimensions() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    DummyCloudWatchClient client = new DummyCloudWatchClient(responseWith(List.of(22.0), List.of(Instant.now())));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", "WRITER", Region.EU_WEST_1, executor, client);
    try {
      GetMetricDataRequest req = client.awaitRequest();
      List<Dimension> dimensions = req.metricDataQueries().get(0).metricStat().metric().dimensions();

      assertEquals(2, dimensions.size());
      assertTrue(dimensions.stream().anyMatch(d -> d.name().equals("DBClusterIdentifier") && d.value().equals("dummy-cluster")));
      assertTrue(dimensions.stream().anyMatch(d -> d.name().equals("Role") && d.value().equals("WRITER")));

      awaitUtilization(monitor, 22.0);
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testMonitorUsesNewestTimestampValue() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    Instant older = Instant.now().minusSeconds(10);
    Instant newer = Instant.now();

    DummyCloudWatchClient client = new DummyCloudWatchClient(responseWith(List.of(90.0, 10.0), List.of(older, newer)));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, client);
    try {
      client.awaitRequest();
      awaitUtilization(monitor, 10.0);
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testMonitorSetsMinusOneOnPollingFailure() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    DummyCloudWatchClient client = new DummyCloudWatchClient(new RuntimeException("FAILED"));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, client);
    try {
      assertNotNull(client.awaitRequest());
      awaitUtilization(monitor, -1.0);
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testEmptyValuesResultsInMinusOne() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    DummyCloudWatchClient client = new DummyCloudWatchClient(responseWith(Collections.emptyList(), Collections.emptyList()));
    AuroraAcuMonitor monitor = new AuroraAcuMonitor("dummy-cluster", Region.EU_WEST_1, executor, client);
    try {
      client.awaitRequest();
      awaitUtilization(monitor, -1.0);
    } finally {
      monitor.stop();
      executor.shutdownNow();
    }
  }

  private static GetMetricDataResponse responseWith(List<Double> values, List<Instant> timestamps) {
    MetricDataResult result = MetricDataResult.builder()
        .id("u")
        .values(values)
        .timestamps(timestamps)
        .build();

    return GetMetricDataResponse.builder()
        .metricDataResults(Collections.singletonList(result))
        .build();
  }
}