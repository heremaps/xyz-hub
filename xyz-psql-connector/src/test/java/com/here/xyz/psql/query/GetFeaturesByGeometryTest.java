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

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.models.geojson.implementation.Geometry;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class GetFeaturesByGeometryTest {
  private static final String POINT = """
      {"type": "Point", "coordinates": [8.5, 50.1]}""";
  private static final String LINE_STRING = """
      {"type": "LineString", "coordinates": [[8.5, 50.1], [8.6, 50.2]]}""";
  private static final String POLYGON = """
      {"type": "Polygon", "coordinates": [[[8.5, 50.1], [8.6, 50.1], [8.6, 50.2], [8.5, 50.2], [8.5, 50.1]]]}""";
  private static final String MULTI_POINT = """
      {"type": "MultiPoint", "coordinates": [[-9.1, 38.7], [12.5, 55.7]]}""";
  private static final String MULTI_LINE_STRING = """
      {"type": "MultiLineString", "coordinates": [[[-9.1, 38.7], [-9.0, 38.8]], [[12.5, 55.7], [12.6, 55.8]]]}""";
  private static final String MULTI_POLYGON = """
      {"type": "MultiPolygon", "coordinates": [
        [[[-9.1, 38.7], [-9.0, 38.7], [-9.0, 38.8], [-9.1, 38.8], [-9.1, 38.7]]],
        [[[12.5, 55.7], [12.6, 55.7], [12.6, 55.8], [12.5, 55.8], [12.5, 55.7]]]
      ]}""";
  private static final String GEOMETRY_COLLECTION = """
      {"type": "GeometryCollection", "geometries": [
        {"type": "Point", "coordinates": [-9.1, 38.7]},
        {"type": "Polygon", "coordinates": [[[12.5, 55.7], [12.6, 55.7], [12.6, 55.8], [12.5, 55.8], [12.5, 55.7]]]}
      ]}""";

  static String[] singlePartGeometries() {
    return new String[]{POINT, LINE_STRING, POLYGON};
  }

  static String[] multiPartGeometries() {
    return new String[]{MULTI_POINT, MULTI_LINE_STRING, MULTI_POLYGON, GEOMETRY_COLLECTION};
  }

  @ParameterizedTest
  @MethodSource("singlePartGeometries")
  void singlePartGeometriesKeepTheIndexedIntersects(String geometry) throws Exception {
    for (int radius : new int[]{0, 500}) {
      String sql = sql(event(geometry).withRadius(radius));
      assertTrue(sql.contains("ST_isvalid("), sql);
      assertTrue(sql.contains("ST_Intersects(geo, "), sql);
      assertFalse(sql.contains("ST_Dump"), sql);
      assertFalse(sql.contains("_ST_Intersects"), sql);
    }
  }

  @Test
  void h3IndexTakesPrecedenceOverAMultiPartGeometry() throws Exception {
    GetFeaturesByGeometryEvent event = event(MULTI_POINT).withH3Index("861fa3a07ffffff");
    assertFalse(GetFeaturesByGeometry.isMultiPartFilter(event));
    String sql = sql(event);
    assertTrue(sql.contains("h3ToGeoBoundaryDeg("), sql);
    assertTrue(sql.contains("ST_Intersects(geo, "), sql);
    assertFalse(sql.contains("ST_Dump"), sql);
    assertFalse(sql.contains("_ST_Intersects"), sql);
  }

  @ParameterizedTest
  @MethodSource("multiPartGeometries")
  void multiPartGeometriesProbeTheIndexPerPart(String geometry) throws Exception {
    for (int radius : new int[]{0, 500, -100}) {
      GetFeaturesByGeometryEvent event = event(geometry).withRadius(radius);
      assertTrue(GetFeaturesByGeometry.isMultiPartFilter(event));
      String sql = sql(event);
      assertTrue(sql.contains("ST_isvalid("), sql);
      assertTrue(sql.contains("geo && ANY(ARRAY(SELECT (ST_Dump("), sql);
      assertTrue(sql.contains("_ST_Intersects(geo, (SELECT "), sql);
      assertFalse(Pattern.compile("(?<!_)ST_Intersects\\(geo, ").matcher(sql).find(), sql);
      assertEquals(radius != 0, sql.contains("ST_Buffer("), sql);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 500})
  void clippingKeepsTheWholeFilterGeometry(int radius) throws Exception {
    String sql = sql(event(MULTI_POINT).withRadius(radius).withClip(true));
    assertTrue(sql.contains(radius != 0 ? "xyz_clip_geometry(geo, ST_Buffer(" : "xyz_clip_geometry(geo, st_force3d("), sql);
  }

  @Test
  void compositeQueriesProbeTheIndexPerPartInEveryBranch() throws Exception {
    GetFeaturesByGeometryEvent event = event(MULTI_POINT)
        .withRadius(500)
        .withParams(Map.of("extends", Map.of("spaceId", "base-space")))
        .withContext(DEFAULT);
    String sql = sql(event);
    assertEquals(2, count(sql, "ST_Dump("), sql);
    assertEquals(2, count(sql, "_ST_Intersects(geo, (SELECT "), sql);
  }

  private static GetFeaturesByGeometryEvent event(String geometry) throws Exception {
    return new GetFeaturesByGeometryEvent()
        .withSpace("test-space")
        .withConnectorParams(Map.of("connectorId", "test-connector"))
        .withGeometry(XyzSerializable.deserialize(geometry, Geometry.class));
  }

  private static String sql(GetFeaturesByGeometryEvent event) throws Exception {
    GetFeaturesByGeometry queryRunner = new GetFeaturesByGeometry(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    };
    return queryRunner.buildQuery(event).substitute().text();
  }

  private static int count(String text, String part) {
    Matcher matcher = Pattern.compile(Pattern.quote(part)).matcher(text);
    int count = 0;
    while (matcher.find())
      count++;
    return count;
  }
}
