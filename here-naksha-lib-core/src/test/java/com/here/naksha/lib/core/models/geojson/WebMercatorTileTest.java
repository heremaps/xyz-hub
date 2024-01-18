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
package com.here.naksha.lib.core.models.geojson;

import static com.here.naksha.lib.core.models.geojson.WebMercatorTile.forQuadkey;
import static com.here.naksha.lib.core.models.geojson.WebMercatorTile.forWeb;
import static com.here.naksha.lib.core.models.geojson.WebMercatorTile.x;
import static com.here.naksha.lib.core.models.geojson.WebMercatorTile.xy;
import static com.here.naksha.lib.core.models.geojson.WebMercatorTile.y;
import static org.junit.jupiter.api.Assertions.*;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.jupiter.api.Test;

public class WebMercatorTileTest {

  @Test
  public void test_pixel() {
    long pixel = xy(0, 1);
    assertEquals(0, x(pixel));
    assertEquals(1, y(pixel));

    pixel = xy(Integer.MIN_VALUE, Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, x(pixel));
    assertEquals(Integer.MIN_VALUE, y(pixel));

    pixel = xy(Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, x(pixel));
    assertEquals(Integer.MAX_VALUE, y(pixel));
  }

  @Test
  public void positiveExamples() {
    forQuadkey("012301230123");
    forQuadkey("10000");
    forQuadkey("0");
    forQuadkey("123");
    forWeb("0_0_0");
    forWeb("1_1_1");
    forWeb("2_3_1");
    forWeb("5_27_10");
    forWeb("10_800_800");
  }

  @Test
  public void invalidQuadIdentifier1() {
    assertThrows(IllegalArgumentException.class, () -> forQuadkey("052301230123"));
  }

  @Test
  public void invalidQuadIdentifier2() {
    assertThrows(IllegalArgumentException.class, () -> forQuadkey("1A000"));
  }

  @Test
  public void invalidQuadIdentifier3() {
    assertThrows(IllegalArgumentException.class, () -> forQuadkey("-9"));
  }

  @Test
  public void invalidQuadIdentifier4() {
    assertThrows(IllegalArgumentException.class, () -> forQuadkey("1234"));
  }

  @Test
  public void invalidQuadIdentifier5() {
    assertThrows(IllegalArgumentException.class, () -> forQuadkey("052301230123"));
  }

  @Test
  public void invalidQuadIdentifier6() {
    assertThrows(IllegalArgumentException.class, () -> forWeb("0_0_1"));
  }

  @Test
  public void invalidQuadIdentifier7() {
    assertThrows(IllegalArgumentException.class, () -> forWeb("0_0_1"));
  }

  @Test
  public void invalidQuadIdentifier8() {
    assertThrows(IllegalArgumentException.class, () -> forWeb("2_4_4"));
  }

  @Test
  public void invalidQuadIdentifier9() {
    assertThrows(IllegalArgumentException.class, () -> forWeb("5_10"));
  }

  @Test
  public void invalidQuadIdentifier10() {
    assertThrows(IllegalArgumentException.class, () -> forWeb("\"10_-1_10"));
  }

  @Test
  public void toQuadkey() {
    String quadkey = "120203302133";
    WebMercatorTile qp = forQuadkey(quadkey);
    assertEquals(quadkey, qp.asQuadkey());
  }

  @Test
  public void testGeometryFromTileId() {
    final String tileId = "120203302030322200";
    final double[][] expectedCoordinates = new double[][] {
      {8.6572265625, 50.12321958080243, Double.NaN},
      {8.6572265625, 50.124100042692376, Double.NaN},
      {8.658599853515625, 50.124100042692376, Double.NaN},
      {8.658599853515625, 50.12321958080243, Double.NaN},
      {8.6572265625, 50.12321958080243, Double.NaN},
    };
    final Geometry geo = WebMercatorTile.forQuadkey(tileId).getAsPolygon().getGeometry();
    final Coordinate[] coordinates = geo.getCoordinates();
    assertNotNull(coordinates);
    assertEquals(expectedCoordinates.length, coordinates.length, "Mismatch in number of coordinates.");
    // match each XYZ co-ordinate
    for (int i = 0; i < expectedCoordinates.length; i++) {
      assertEquals(
          expectedCoordinates[i][0],
          coordinates[i].getOrdinate(Coordinate.X),
          "Mismatch in X ordinate for co-ordinate at index position " + i);
      assertEquals(
          expectedCoordinates[i][1],
          coordinates[i].getOrdinate(Coordinate.Y),
          "Mismatch in Y ordinate for co-ordinate at index position " + i);
      assertEquals(
          expectedCoordinates[i][2],
          coordinates[i].getOrdinate(Coordinate.Z),
          "Mismatch in Z ordinate for co-ordinate at index position " + i);
    }
  }

  @Test
  public void testExtendedGeometryWithMargin() {
    final String tileId = "120203302030322200";
    final int margin = 20;
    final double[][] expectedCoordinates = new double[][] {
            {8.657119274139404, 50.12315079403522, Double.NaN},
            {8.657119274139404, 50.12416882809549, Double.NaN},
            {8.65870714187622, 50.12416882809549, Double.NaN},
            {8.65870714187622, 50.12315079403522, Double.NaN},
            {8.657119274139404, 50.12315079403522, Double.NaN},
    };
    final Geometry geo = WebMercatorTile.forQuadkey(tileId).getExtendedBBoxAsPolygon(margin).getGeometry();
    final Coordinate[] coordinates = geo.getCoordinates();
    assertNotNull(coordinates);
    assertEquals(expectedCoordinates.length, coordinates.length, "Mismatch in number of coordinates.");
    // match each XYZ co-ordinate
    for (int i = 0; i < expectedCoordinates.length; i++) {
      assertEquals(
              expectedCoordinates[i][0],
              coordinates[i].getOrdinate(Coordinate.X),
              "Mismatch in X ordinate for co-ordinate at index position " + i);
      assertEquals(
              expectedCoordinates[i][1],
              coordinates[i].getOrdinate(Coordinate.Y),
              "Mismatch in Y ordinate for co-ordinate at index position " + i);
      assertEquals(
              expectedCoordinates[i][2],
              coordinates[i].getOrdinate(Coordinate.Z),
              "Mismatch in Z ordinate for co-ordinate at index position " + i);
    }
  }
}
