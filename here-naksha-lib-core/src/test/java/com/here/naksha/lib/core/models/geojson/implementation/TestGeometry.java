/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.geojson.implementation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class TestGeometry {

  @Test
  public void test_geometryOf() throws Exception {
    final ObjectMapper mp = new ObjectMapper();

    XyzGeometry geometry = mp.readValue("{\"type\":\"Point\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzPoint);

    geometry = mp.readValue("{\"type\":\"MultiPoint\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzMultiPoint);

    geometry = mp.readValue("{\"type\":\"LineString\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzLineString);

    geometry = mp.readValue("{\"type\":\"MultiLineString\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzMultiLineString);

    geometry = mp.readValue("{\"type\":\"Polygon\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzPolygon);

    geometry = mp.readValue("{\"type\":\"MultiPolygon\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzMultiPolygon);

    geometry = mp.readValue("{\"type\":\"GeometryCollection\"}", XyzGeometry.class);
    assertTrue(geometry instanceof XyzGeometryCollection);
  }

  @Test
  public void test_validate() throws Exception {
    final ObjectMapper mp = new ObjectMapper();

    String json = "{\"type\":\"GeometryCollection\",\"geometries\":[]}";
    XyzGeometryCollection geometryCollection = mp.readValue(json, XyzGeometryCollection.class);
    geometryCollection.validate();

    json = "{\"type\":\"GeometryCollection\",\"geometries\":["
        + //
        "{\"type\":\"Point\",\"coordinates\":[0,0,0]},"
        + //
        "{\"type\":\"MultiPoint\",\"coordinates\":[]},"
        + //
        "{\"type\":\"MultiPoint\",\"coordinates\":[[0,0,0], [1,1,1]]}"
        + //
        "]}";
    geometryCollection = mp.readValue(json, XyzGeometryCollection.class);
    geometryCollection.validate();
  }
}
