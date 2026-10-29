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

package com.here.xyz.jobs.config;

import static com.here.xyz.jobs.config.StatisticsConfigClient.KIND_IMPORT;
import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.config.StatisticsConfigClient.StatisticsConfig;
import com.here.xyz.util.service.Core;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatisticsConfigClientIT {

  private static final String DYNAMO_HOST = System.getProperty("job.host", "localhost");
  private static final String TABLE_ARN = "arn:aws:dynamodb:" + DYNAMO_HOST + ":000000008000:table/xyz-job-statistics-local";
  private static final long RECORDED_AT = 1_700_000_000_000L;

  private StatisticsConfigClient client;
  private String catalog;

  @BeforeAll
  public static void initVertx() {
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
  }

  @BeforeEach
  public void setUp() {
    client = new StatisticsConfigClient(TABLE_ARN);
    client.initLocalTable();
    catalog = "hrn:here:data::olp-here:" + randomAlpha(6);
  }

  @Test
  public void storeStatisticsThenLoadReturnsAllVersionsOfTheSpace() {
    String spaceId = catalog + ":address";
    client.storeStatistics(List.of(
        statistics(spaceId, 2, Map.of("added", 1L, "updated", 3L, "deleted", 2L, "bytes", 415L)),
        statistics(spaceId, 10, Map.of("total", 25L, "bytes", 5060L))));

    List<StatisticsConfig> loaded = client.loadStatisticsForSpace(spaceId);

    //Sorted by the sort key: the zero padding puts version 10 after version 2
    assertEquals(List.of(2L, 10L), loaded.stream().map(StatisticsConfig::getVersion).toList());
    StatisticsConfig first = loaded.get(0);
    assertEquals("0000000002#import", first.getSk());
    assertEquals(catalog, first.getCatalogHrn());
    assertEquals("address", first.getLayerId());
    assertEquals("job1", first.getJobId());
    assertEquals(Map.of("added", 1L, "updated", 3L, "deleted", 2L, "bytes", 415L), first.getRequested());
    assertEquals(RECORDED_AT, first.getRecordedAt());
    assertEquals(RECORDED_AT / 1000 + 365L * 24 * 3600, first.getExpiresAt(), "items expire a year after they were recorded");
  }

  @Test
  public void loadIsScopedToTheRequestedSpace() {
    client.storeStatistics(List.of(
        statistics(catalog + ":address", 1, Map.of("total", 1L)),
        statistics(catalog + ":place", 1, Map.of("total", 2L))));

    assertEquals(1, client.loadStatisticsForSpace(catalog + ":address").size());
    assertTrue(client.loadStatisticsForSpace(catalog + ":unknown").isEmpty(), "an unknown space has no statistics");
  }

  @Test
  public void storingAVersionAgainReplacesIt() {
    String spaceId = catalog + ":address";
    client.storeStatistics(List.of(statistics(spaceId, 3, Map.of("added", 1L))));
    client.storeStatistics(List.of(statistics(spaceId, 3, Map.of("added", 5L))));

    List<StatisticsConfig> loaded = client.loadStatisticsForSpace(spaceId);
    assertEquals(1, loaded.size());
    assertEquals(Map.of("added", 5L), loaded.get(0).getRequested());
  }

  @Test
  public void storingNothingIsANoOp() {
    client.storeStatistics(List.of());
    client.storeStatistics(null);
  }

  private static StatisticsConfig statistics(String spaceId, long version, Map<String, Long> requested) {
    return new StatisticsConfig()
        .withSpaceId(spaceId)
        .withVersion(version)
        .withKind(KIND_IMPORT)
        .withJobId("job1")
        .withRequested(requested)
        .withRecordedAt(RECORDED_AT);
  }
}
