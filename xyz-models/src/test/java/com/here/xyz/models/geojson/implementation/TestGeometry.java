/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;

import org.junit.Test;

public class TestGeometry {

  @Test
  public void test_geometryOf() throws Exception {
    final ObjectMapper mp = new ObjectMapper();

    Geometry geometry = mp.readValue("{\"type\":\"Point\"}", Geometry.class);
    assertTrue(geometry instanceof Point);

    geometry = mp.readValue("{\"type\":\"MultiPoint\"}", Geometry.class);
    assertTrue(geometry instanceof MultiPoint);

    geometry = mp.readValue("{\"type\":\"LineString\"}", Geometry.class);
    assertTrue(geometry instanceof LineString);

    geometry = mp.readValue("{\"type\":\"MultiLineString\"}", Geometry.class);
    assertTrue(geometry instanceof MultiLineString);

    geometry = mp.readValue("{\"type\":\"Polygon\"}", Geometry.class);
    assertTrue(geometry instanceof Polygon);

    geometry = mp.readValue("{\"type\":\"MultiPolygon\"}", Geometry.class);
    assertTrue(geometry instanceof MultiPolygon);

    geometry = mp.readValue("{\"type\":\"GeometryCollection\"}", Geometry.class);
    assertTrue(geometry instanceof GeometryCollection);
  }

  @Test
  public void test_validate() throws Exception {
    final ObjectMapper mp = new ObjectMapper();

    String json = "{\"type\":\"GeometryCollection\",\"geometries\":[]}";
    GeometryCollection geometryCollection = mp.readValue(json, GeometryCollection.class);
    geometryCollection.validate();

    json = "{\"type\":\"GeometryCollection\",\"geometries\":[" + //
        "{\"type\":\"Point\",\"coordinates\":[0,0,0]}," + //
        "{\"type\":\"MultiPoint\",\"coordinates\":[]}," + //
        "{\"type\":\"MultiPoint\",\"coordinates\":[[0,0,0], [1,1,1]]}" + //
        "]}";
    geometryCollection = mp.readValue(json, GeometryCollection.class);
    geometryCollection.validate();
  }

  //  @Test HERESUP-1283
  public void test_polygon_validate() throws Exception {
    final ObjectMapper mp = new ObjectMapper();

    // test invalid linestring with some duplicate coords
    assertThrows(InvalidGeometryException.class, 
                 () -> mp.readValue("{\"type\":\"Polygon\",\"coordinates\":[[[8.00,49.00,0],[8.00,49.01,0],[8.00,49.02,0],[8.00,49.00,0],[8.00,49.00,0]]]}", Geometry.class).validate() );
    
   // test valid linestring, no duplicate coords
    mp.readValue("{\"type\":\"Polygon\",\"coordinates\":[[[8.00,49.00,0],[8.00,49.01,0],[8.00,49.02,0],[8.00,49.03,0],[8.00,49.00,0]]]}", Geometry.class).validate();
  }

}
