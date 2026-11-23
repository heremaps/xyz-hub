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

package com.here.xyz.hub.connectors.statistics;

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.StorageStatistics.SpaceByteSizes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StorageStatisticsProviderTest {

  @Test
  void resolvesOnlyPersistedPhysicalTableNames() {
    Map<String, String> tableNames = StorageStatisticsProvider.resolveTableNames(List.of(
        space("unique-space", Map.of(TABLE_NAME, "unique_table")),
        space("legacy-space", null)));

    assertEquals(Map.of("unique-space", "unique_table"), tableNames);
  }

  @Test
  void sendsOnlyMappingsRelevantToTheCurrentBatch() {
    Map<String, String> tableNames = StorageStatisticsProvider.tableNamesForBatch(
        List.of("second-space", "legacy-space"),
        Map.of("first-space", "first_table", "second-space", "second_table"));

    assertEquals(Map.of("second-space", "second_table"), tableNames);
  }

  @Test
  void setsTheStorageIdOfALazySpaceWithoutTable() {
    StorageStatistics stats = statistics(Map.of("lazy-space", tableLess()));

    StorageStatisticsProvider.addStorageIdsOfLazySpaces(stats,
        List.of(space("lazy-space", Map.of(TABLE_NAME, "lazy_table", LAZY_TABLE, true))));

    assertEquals("psql", stats.getByteSizes().get("lazy-space").getStorageId());
    assertEquals(0L, stats.getByteSizes().get("lazy-space").getContentBytes().getValue());
  }

  @Test
  void leavesAnEagerSpaceWithoutTableWithoutStorageId() {
    StorageStatistics stats = statistics(Map.of("eager-space", tableLess(), "legacy-space", tableLess()));

    StorageStatisticsProvider.addStorageIdsOfLazySpaces(stats, List.of(
        space("eager-space", Map.of(TABLE_NAME, "eager_table")),
        space("legacy-space", null)));

    assertNull(stats.getByteSizes().get("eager-space").getStorageId());
    assertNull(stats.getByteSizes().get("legacy-space").getStorageId());
  }

  @Test
  void keepsTheReportedStorageIdAndAddsNoEntries() {
    StorageStatistics stats = statistics(Map.of("lazy-space",
        new SpaceByteSizes().withStorageId("reported").withContentBytes(new Value<>(100L).withEstimated(true))));

    StorageStatisticsProvider.addStorageIdsOfLazySpaces(stats, List.of(
        space("lazy-space", Map.of(TABLE_NAME, "lazy_table", LAZY_TABLE, true)),
        space("unreported-space", Map.of(TABLE_NAME, "other_table", LAZY_TABLE, true))));

    assertEquals("reported", stats.getByteSizes().get("lazy-space").getStorageId());
    assertFalse(stats.getByteSizes().containsKey("unreported-space"));
  }

  private static StorageStatistics statistics(Map<String, SpaceByteSizes> byteSizes) {
    return new StorageStatistics().withByteSizes(new HashMap<>(byteSizes));
  }

  /**
   * @return The sizes a connector reports for a space whose table does not exist
   */
  private static SpaceByteSizes tableLess() {
    return new SpaceByteSizes().withContentBytes(new Value<>(0L).withEstimated(true));
  }

  private static Space space(String id, Map<String, Object> params) {
    Space space = new Space();
    space.setId(id);
    space.setStorage(new ConnectorRef().withId("psql").withParams(params));
    return space;
  }
}
