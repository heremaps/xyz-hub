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

package com.here.xyz.psql.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.util.Hasher;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GetStorageStatisticsTest {

  @Test
  void usesTheExplicitPhysicalTableName() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent()
        .withSpaceIdToTableName(Map.of("space-id", "physical_table"))
        .withConnectorParams(Map.of("enableHashedSpaceId", true));

    assertEquals("physical_table", GetStorageStatistics.resolvePhysicalTableName(event, "space-id"));
  }

  @Test
  void hashesLegacySpaceIdsWhenConfigured() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent()
        .withConnectorParams(Map.of("enableHashedSpaceId", true));

    assertEquals(Hasher.getHash("legacy-space"),
        GetStorageStatistics.resolvePhysicalTableName(event, "legacy-space"));
  }

  @Test
  void keepsLegacySpaceIdsWhenHashingIsDisabled() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent();

    assertEquals("legacy-space", GetStorageStatistics.resolvePhysicalTableName(event, "legacy-space"));
  }
}
