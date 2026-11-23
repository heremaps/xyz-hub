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

import static com.here.xyz.util.db.pg.LazySpaceTables.ON_DEMAND_INDEXES;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GetStatisticsTest {
  private static final String WITHOUT_INDEX_PROPERTIES = "SELECT * FROM xyz_statistic_space(?, ?, ?, ?::TEXT[])";
  private static final String WITH_INDEX_PROPERTIES = "SELECT * FROM xyz_statistic_space(?, ?, ?, ?::TEXT[], ?::JSONB)";

  private static GetStatisticsEvent event(Map<String, Object> params) {
    return new GetStatisticsEvent()
        .withSpace("test-space")
        .withConnectorParams(Map.of("connectorId", "test-connector"))
        .withParams(params);
  }

  private static SQLQuery query(GetStatisticsEvent event) throws Exception {
    GetStatistics queryRunner = new GetStatistics(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    };
    return queryRunner.buildQuery(event);
  }

  /**
   * @return The substituted text of the query without its label comment
   */
  private static String text(SQLQuery query) {
    return query.substitute().text().replaceFirst("^/\\*labels\\(.*?\\)\\*/ ", "");
  }

  private static Map<String, Object> params(String table, boolean lazy, Map<String, Object> base) {
    Map<String, Object> params = new HashMap<>(Map.of(Space.TABLE_NAME, table));
    if (lazy)
      params.put(Space.LAZY_TABLE, true);
    if (base != null)
      params.put("extends", base);
    return params;
  }

  private static Map<String, Object> base(String table, boolean lazy) {
    Map<String, Object> base = params(table, lazy, null);
    base.put("spaceId", "base-space");
    return base;
  }

  private static Map<String, Object> withOnDemandIndexes(Map<String, Object> params, Object onDemandIndexes) {
    params.put(ON_DEMAND_INDEXES, onDemandIndexes);
    return params;
  }

  private static void assertWithoutIndexProperties(Map<String, Object> params) throws Exception {
    SQLQuery query = query(event(params));
    String sql = text(query);
    assertEquals(WITHOUT_INDEX_PROPERTIES, sql);
    assertEquals(4, query.parameters().size(), sql);
  }

  @Test
  void nonLazyEventsSendTheQueryWithoutIndexProperties() throws Exception {
    assertWithoutIndexProperties(params("t1", false, null));
    assertWithoutIndexProperties(params("t1", false, base("b1", false)));
    assertWithoutIndexProperties(null);
  }

  @Test
  void lazyEventWithRealTablesSendsNoIndexProperties() throws Exception {
    assertWithoutIndexProperties(withOnDemandIndexes(params("t1", true, null), List.of("a")));
    assertWithoutIndexProperties(withOnDemandIndexes(params("t1", false, base("b1", true)), List.of("a")));
  }

  @Test
  void neverWrittenLevelGetsTheValidOnDemandIndexes() throws Exception {
    SQLQuery query = query(event(withOnDemandIndexes(params(TEMPLATE_TABLE, true, null), List.of("a", "it's", "b"))));
    String sql = text(query);
    assertEquals(WITH_INDEX_PROPERTIES, sql);
    assertEquals(5, query.parameters().size(), sql);
    assertEquals("{\"" + TEMPLATE_TABLE + "\":[\"a\",\"b\"]}", query.parameters().get(4));
  }

  @Test
  void neverWrittenBaseGetsTheOnDemandIndexes() throws Exception {
    SQLQuery query = query(event(withOnDemandIndexes(params("t1", false, base(TEMPLATE_TABLE, true)), List.of("a"))));
    String sql = text(query);
    assertEquals(WITH_INDEX_PROPERTIES, sql);
    assertEquals(5, query.parameters().size(), sql);
    assertEquals("{\"" + TEMPLATE_TABLE + "\":[\"a\"]}", query.parameters().get(4));
  }

  @Test
  void neverWrittenLevelWithoutAListOfOnDemandIndexesSendsNoIndexProperties() throws Exception {
    assertWithoutIndexProperties(params(TEMPLATE_TABLE, true, null));
    assertWithoutIndexProperties(withOnDemandIndexes(params(TEMPLATE_TABLE, true, null), "a"));
  }
}
