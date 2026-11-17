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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class IterateFeaturesTest {
  private static final Map<String, Object> ONE_LEVEL = Map.of("extends", Map.of("spaceId", "base-space", "version", 3));
  private static final Map<String, Object> TWO_LEVELS = Map.of("extends", Map.of("spaceId", "intermediate-space", "version", 2,
      "extends", Map.of("spaceId", "base-space", "version", 3)));

  @Test
  void compositeIterateLimitsInTheOrderOfThePageToken() throws Exception {
    String sql = iterateSql(ONE_LEVEL);
    assertTrue(sql.contains("limitQuery ORDER BY dataset, i LIMIT"), sql);
  }

  @Test
  void compositeIterateLimitsEachDataset() throws Exception {
    //The extension and the base
    assertEquals(2, count(iterateSql(ONE_LEVEL), "ORDER BY i LIMIT"), iterateSql(ONE_LEVEL));
    //The extension, the intermediate and the base
    assertEquals(3, count(iterateSql(TWO_LEVELS), "ORDER BY i LIMIT"), iterateSql(TWO_LEVELS));
  }

  @Test
  void otherCompositeQueriesLimitAnyRows() throws Exception {
    SearchForFeaturesEvent event = new SearchForFeaturesEvent();
    event.setLimit(7);
    event.setSpace("test-space");
    event.setConnectorParams(Map.of("connectorId", "test-connector"));
    event.setParams(TWO_LEVELS);
    event.setContext(DEFAULT);
    String sql = new SearchForFeatures<>(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    }.buildQuery(event).substitute().text();

    assertEquals(1, count(sql, "LIMIT"), sql);
    assertFalse(sql.contains("ORDER BY dataset"), sql);
  }

  private static String iterateSql(Map<String, Object> params) throws Exception {
    IterateFeaturesEvent event = new IterateFeaturesEvent();
    event.setLimit(7);
    event.setSpace("test-space");
    event.setConnectorParams(Map.of("connectorId", "test-connector"));
    event.setParams(params);
    event.setContext(DEFAULT);
    return new IterateFeatures<>(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    }.buildQuery(event).substitute().text();
  }

  private static int count(String text, String part) {
    Matcher matcher = Pattern.compile(Pattern.quote(part)).matcher(text);
    int count = 0;
    while (matcher.find())
      count++;
    return count;
  }
}
