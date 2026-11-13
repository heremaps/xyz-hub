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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GetFastStatisticsTest {

  private static GetStatisticsEvent event(Map<String, Object> params) {
    return new GetStatisticsEvent()
        .withSpace("test-space")
        .withFastMode(true)
        .withConnectorParams(Map.of("connectorId", "test-connector"))
        .withParams(params);
  }

  private static SQLQuery query(GetStatisticsEvent event) throws Exception {
    GetFastStatistics queryRunner = new GetFastStatistics(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    };
    return queryRunner.buildQuery(event);
  }

  private static Map<String, Object> params(String table, boolean lazy, Map<String, Object> base) {
    Map<String, Object> params = new HashMap<>(Map.of(Space.TABLE_NAME, table));
    if (lazy)
      params.put(Space.LAZY_TABLE, true);
    if (base != null)
      params.put("extends", base);
    return params;
  }

  @Test
  void lazyEventWithoutExtensionCastsTheTableAndPassesNoExtension() throws Exception {
    SQLQuery query = query(event(params("t1", true, null)));
    String sql = query.substitute().text();
    assertTrue(sql.contains("calculate_space_statistics(?::regclass, NULL::regclass, ?)"), sql);
    assertFalse(sql.contains("to_regclass"), sql);
    assertEquals(2, query.parameters().size(), sql);
    assertEquals("public.\"t1_head\"", query.parameters().get(0));
  }

  @Test
  void lazyEventWithExtensionCastsBothTables() throws Exception {
    //A lazy base is enough, the flag stays in the substituted params
    Map<String, Object> base = new HashMap<>(params("b1", true, null));
    base.put("spaceId", "base-space");
    SQLQuery query = query(event(params("t1", false, base)));
    String sql = query.substitute().text();
    assertTrue(sql.contains("calculate_space_statistics(?::regclass, ?::regclass, ?)"), sql);
    assertFalse(sql.contains("to_regclass"), sql);
    assertEquals(3, query.parameters().size(), sql);
    assertEquals("public.\"t1_head\"", query.parameters().get(0));
    assertEquals("public.\"b1_head\"", query.parameters().get(1));
  }

  @Test
  void otherEventsKeepToRegclass() throws Exception {
    for (Map<String, Object> params : new Map[]{params("t1", false, null), null,
        params("t1", false, new HashMap<>(Map.of("spaceId", "base-space", Space.TABLE_NAME, "b1")))}) {
      String sql = query(event(params)).substitute().text();
      assertTrue(sql.contains("calculate_space_statistics(to_regclass(?), to_regclass(?), ?)"), sql);
      assertFalse(sql.contains("::regclass"), sql);
    }
  }

  @Test
  void fullStatisticsReadTheHeadVersionFromTheWriterWhenThePrimaryIsPreferred() throws Exception {
    GetStatisticsEvent event = event(params("t1", true, null)).withFastMode(false);
    assertTrue(new GetStatistics(event).isUseReadReplica());
    //The safety net retries a read on the writer, the head version query takes isUseReadReplica() of this runner
    assertFalse(new GetStatistics(event.withPreferPrimaryDataSource(true)).isUseReadReplica());
  }
}
