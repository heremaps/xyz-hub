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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
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

  private static Space space(String id, Map<String, Object> params) {
    Space space = new Space();
    space.setId(id);
    space.setStorage(new ConnectorRef().withId("psql").withParams(params));
    return space;
  }
}
