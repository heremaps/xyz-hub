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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GetFeaturesByBBoxClusteredTest {

  private static GetFeaturesByBBoxEvent event(Map<String, Object> clusteringParams) {
    GetFeaturesByBBoxEvent event = new GetFeaturesByBBoxEvent();
    event.setSpace("test-space");
    event.setConnectorParams(Map.of("connectorId", "test-connector"));
    event.setBbox(new BBox(12, 51, 15, 54));
    event.setClusteringType("hexbin");
    event.setClusteringParams(clusteringParams);
    return event;
  }

  private static String sql(GetFeaturesByBBoxEvent event) throws Exception {
    GetFeaturesByBBoxClustered<GetFeaturesByBBoxEvent, ?> queryRunner = new GetFeaturesByBBoxClustered<>(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    };
    return queryRunner.buildQuery(event).substitute().text();
  }

  @Test
  void hexbinWithoutStatisticalProperty() throws Exception {
    assertTrue(sql(event(new HashMap<>())).contains("h3"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void hexbinWithStatisticalPropertyCanBeSubstituted(boolean pointMode) throws Exception {
    Map<String, Object> params = new HashMap<>(Map.of("property", "a.b", "absoluteResolution", 9));
    if (pointMode)
      params.put("pointmode", true);
    String sql = sql(event(params));
    assertTrue(sql.contains("jsonb_set('{}'::jsonb, "), sql);
    assertTrue(sql.contains("(jsondata#>> "), sql);
  }
}
