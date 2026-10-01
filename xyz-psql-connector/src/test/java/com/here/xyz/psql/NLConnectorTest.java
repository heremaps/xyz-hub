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

package com.here.xyz.psql;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.util.Hasher;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NLConnectorTest {

  @Test
  void resolvesPersistedTableNamesForCompositeSearches() {
    SearchForFeaturesEvent event = event(Map.of(
        TABLE_NAME, "composite_table",
        "extends", Map.of("spaceId", "logical-base", TABLE_NAME, "base_table")));

    assertEquals(List.of("composite_table", "base_table"), NLConnector.resolveSearchTables(event));
  }

  @Test
  void retainsHashedFallbackForLegacyCompositeSearches() {
    SearchForFeaturesEvent event = event(Map.of("extends", Map.of("spaceId", "logical-base")));

    assertEquals(List.of(Hasher.getHash("logical-composite"), Hasher.getHash("logical-base")),
        NLConnector.resolveSearchTables(event));
  }

  private static SearchForFeaturesEvent event(Map<String, Object> params) {
    SearchForFeaturesEvent event = new SearchForFeaturesEvent();
    event.setSpace("logical-composite");
    event.setParams(params);
    event.setConnectorParams(Map.of("enableHashedSpaceId", true));
    return event;
  }
}
