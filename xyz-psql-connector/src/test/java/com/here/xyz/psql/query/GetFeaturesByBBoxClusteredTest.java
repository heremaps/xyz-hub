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

import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void hexbinUsesTheH3Extension(boolean pointMode) throws Exception {
    Map<String, Object> params = new HashMap<>(Map.of("absoluteResolution", 9));
    if (pointMode)
      params.put("pointmode", true);
    String sql = sql(event(params));
    for (String expected : new String[]{"h3_ext.h3_cell_to_boundary_geometry(cc.h3)", "h3_ext.h3_cell_to_geometry(cc.h3)",
        "h3_ext.h3_is_pentagon(cc.h3)", "h3_ext.h3_get_hexagon_edge_length_avg(", "xyz_h3_cover(", "OPERATOR(h3_ext.@)",
        "xyz_h3_buffered_bbox("})
      assertTrue(sql.contains(expected), expected + " missing in: " + sql);
    //No Polygon typmod (a cell split at the antimeridian is a MultiPolygon), no centroid of the boundary, no functions of the port,
    //no inline buffered envelope (it wraps around at the antimeridian)
    for (String unexpected : new String[]{"::geometry(Polygon", "bit(64)", "h3togeoboundarydeg", "coveringDeg", "edgeLengthM",
        "h3ToGeoDeg", "st_centroid(geo)", "st_envelope( st_buffer("})
      assertFalse(sql.contains(unexpected), unexpected + " found in: " + sql);
  }

  @Test
  void hexbinBboxAtTheAntimeridianUsesTheSameFilter() throws Exception {
    GetFeaturesByBBoxEvent event = event(new HashMap<>(Map.of("absoluteResolution", 5)));
    event.setBbox(new BBox(179, 0, 180, 2));
    String sql = sql(event);
    //The request bbox is passed unchanged, the split at the antimeridian happens in SQL
    String expected = "xyz_h3_buffered_bbox(179.00000000000000, 0.00000000000000, 180.00000000000000, 2.00000000000000, "
        + "2.5 * h3_ext.h3_get_hexagon_edge_length_avg(5, 'm'))";
    assertTrue(sql.contains(expected), expected + " missing in: " + sql);
  }
}
