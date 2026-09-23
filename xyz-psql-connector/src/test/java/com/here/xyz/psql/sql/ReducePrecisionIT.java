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

package com.here.xyz.psql.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.xyz.psql.query.branching.QueryTestBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Characterization test for {@code xyz_reduce_precision} in {@code /sql/ext.sql}, which is called once per feature
 * on every write path and had no test before.
 *
 * <p>Expected values come from the previous implementation, which reduced the precision by round tripping through
 * WKT. Each case is asserted at 8 digits, what every read path serializes with (see
 * {@code GetFeatures.GEOMETRY_DECIMAL_DIGITS}), and at the 9 digit PostGIS default used by
 * {@code SpaceWriter.getFeatureRow}, which is the assertion that catches snapping X and Y only. Stored bytes are
 * not asserted, they legitimately differ by around 1e-14. One case diverges and is pinned as such, see
 * {@code z-rounding-tie-accepted-divergence}.</p>
 */
public class ReducePrecisionIT extends QueryTestBase {

  /**
   * @param name          the case, used as the test display name
   * @param geometrySql   SQL constructing the input geometry
   * @param expectedJson8 GeoJSON at 8 decimal digits, the precision every read path uses
   * @param expectedJson9 GeoJSON at the 9 digit PostGIS default, the stricter assertion
   * @param expectedSrid  the SRID of the result
   * @param expectedType  the result of {@code GeometryType()}
   */
  record Fixture(String name, String geometrySql, String expectedJson8, String expectedJson9, int expectedSrid,
                 String expectedType) {
    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<Fixture> fixtures() {
    return Stream.of(
        //Coordinates beyond the 1e-8 grid are rounded to 8 decimal digits
        new Fixture("point-2d-high-precision",
            "ST_Force3D(ST_SetSRID(ST_MakePoint(8.1234567891234, 50.9876543219876), 4326))",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,0]}",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,0]}",
            4326, "POINT"),

        /*
         * The discriminating case. Z is reduced as well, which the two argument form of ST_SnapToGrid does not do.
         * At 8 digits both agree, so only the 9 digit assertion catches a replacement that forgets the Z ordinate.
         */
        new Fixture("point-3d-high-precision-z",
            "ST_SetSRID(ST_MakePoint(8.1234567891234, 50.9876543219876, 123.456789123456789), 4326)",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,123.45678912]}",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,123.45678912]}",
            4326, "POINT"),

        /*
         * The one accepted divergence, so this fixture pins CURRENT behavior while every other one pins the previous
         * behavior. A Z within about 1e-12 of a grid midpoint rounds the other way, by 1e-8: the round trip rounded
         * Z as decimal text where 8274.413173115 ties upward, snapping rounds the binary value which falls just
         * below half. Hit 4 of 150000 points with a random high precision Z, and impossible when Z is 0.
         */
        new Fixture("z-rounding-tie-accepted-divergence",
            "ST_SetSRID(ST_MakePoint(8.1234567891234, 50.9876543219876, 8274.413173115), 4326)",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,8274.41317311]}",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,8274.41317311]}",
            4326, "POINT"),

        //Coordinates already on the grid are untouched
        new Fixture("point-already-coarse",
            "ST_Force3D(ST_SetSRID(ST_MakePoint(8.5, 50.5), 4326))",
            "{\"type\":\"Point\",\"coordinates\":[8.5,50.5,0]}",
            "{\"type\":\"Point\",\"coordinates\":[8.5,50.5,0]}",
            4326, "POINT"),

        new Fixture("linestring",
            "ST_Force3D(ST_SetSRID(ST_GeomFromText('LINESTRING(8.1234567891234 50.1234567891234, "
                + "9.9876543219876 51.9876543219876)'), 4326))",
            "{\"type\":\"LineString\",\"coordinates\":[[8.12345679,50.12345679,0],[9.98765432,51.98765432,0]]}",
            "{\"type\":\"LineString\",\"coordinates\":[[8.12345679,50.12345679,0],[9.98765432,51.98765432,0]]}",
            4326, "LINESTRING"),

        new Fixture("polygon",
            "ST_Force3D(ST_SetSRID(ST_GeomFromText('POLYGON((8.1234567891234 50.1234567891234, "
                + "8.2234567891234 50.1234567891234, 8.2234567891234 50.2234567891234, "
                + "8.1234567891234 50.1234567891234))'), 4326))",
            "{\"type\":\"Polygon\",\"coordinates\":[[[8.12345679,50.12345679,0],[8.22345679,50.12345679,0],"
                + "[8.22345679,50.22345679,0],[8.12345679,50.12345679,0]]]}",
            "{\"type\":\"Polygon\",\"coordinates\":[[[8.12345679,50.12345679,0],[8.22345679,50.12345679,0],"
                + "[8.22345679,50.22345679,0],[8.12345679,50.12345679,0]]]}",
            4326, "POLYGON"),

        new Fixture("multipoint",
            "ST_Force3D(ST_SetSRID(ST_GeomFromText('MULTIPOINT((8.1234567891234 50.1234567891234),"
                + "(9.9876543219876 51.9876543219876))'), 4326))",
            "{\"type\":\"MultiPoint\",\"coordinates\":[[8.12345679,50.12345679,0],[9.98765432,51.98765432,0]]}",
            "{\"type\":\"MultiPoint\",\"coordinates\":[[8.12345679,50.12345679,0],[9.98765432,51.98765432,0]]}",
            4326, "MULTIPOINT"),

        new Fixture("multipolygon",
            "ST_Force3D(ST_SetSRID(ST_GeomFromText('MULTIPOLYGON(((0.1234567891234 0.1234567891234,"
                + "1.1234567891234 0.1234567891234,1.1234567891234 1.1234567891234,"
                + "0.1234567891234 0.1234567891234)))'), 4326))",
            "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[0.12345679,0.12345679,0],[1.12345679,0.12345679,0],"
                + "[1.12345679,1.12345679,0],[0.12345679,0.12345679,0]]]]}",
            "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[0.12345679,0.12345679,0],[1.12345679,0.12345679,0],"
                + "[1.12345679,1.12345679,0],[0.12345679,0.12345679,0]]]]}",
            4326, "MULTIPOLYGON"),

        new Fixture("geometrycollection",
            "ST_Force3D(ST_SetSRID(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(8.1234567891234 50.1234567891234),"
                + "LINESTRING(1.1234567891234 2.1234567891234,3.1234567891234 4.1234567891234))'), 4326))",
            "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\","
                + "\"coordinates\":[8.12345679,50.12345679,0]},{\"type\":\"LineString\","
                + "\"coordinates\":[[1.12345679,2.12345679,0],[3.12345679,4.12345679,0]]}]}",
            "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\","
                + "\"coordinates\":[8.12345679,50.12345679,0]},{\"type\":\"LineString\","
                + "\"coordinates\":[[1.12345679,2.12345679,0],[3.12345679,4.12345679,0]]}]}",
            4326, "GEOMETRYCOLLECTION"),

        new Fixture("polygon-empty",
            "ST_SetSRID(ST_GeomFromText('POLYGON EMPTY'), 4326)",
            "{\"type\":\"Polygon\",\"coordinates\":[]}",
            "{\"type\":\"Polygon\",\"coordinates\":[]}",
            4326, "POLYGON"),

        /*
         * An invalid geometry is returned unchanged by the ST_IsValid guard, so the precision is NOT reduced. The
         * 9 digit rendering shows that: it still carries 0.123456789 rather than the snapped 0.12345679.
         */
        new Fixture("invalid-self-intersecting",
            "ST_SetSRID(ST_GeomFromText('POLYGON((0.1234567891234 0.1234567891234, "
                + "1.1234567891234 1.1234567891234, 1.1234567891234 0.1234567891234, "
                + "0.1234567891234 1.1234567891234, 0.1234567891234 0.1234567891234))'), 4326)",
            "{\"type\":\"Polygon\",\"coordinates\":[[[0.12345679,0.12345679],[1.12345679,1.12345679],"
                + "[1.12345679,0.12345679],[0.12345679,1.12345679],[0.12345679,0.12345679]]]}",
            "{\"type\":\"Polygon\",\"coordinates\":[[[0.123456789,0.123456789],[1.123456789,1.123456789],"
                + "[1.123456789,0.123456789],[0.123456789,1.123456789],[0.123456789,0.123456789]]]}",
            4326, "POLYGON"),

        /*
         * Snapping collapses both vertices onto the same grid cell. The type is unchanged, so the GeometryType guard
         * lets the collapsed result through rather than returning the input.
         */
        new Fixture("collapsing-linestring",
            "ST_SetSRID(ST_GeomFromText('LINESTRING(8.000000001 50.000000001, 8.000000002 50.000000002)'), 4326)",
            "{\"type\":\"LineString\",\"coordinates\":[]}",
            "{\"type\":\"LineString\",\"coordinates\":[]}",
            4326, "LINESTRING"),

        //NaN is passed through rather than raising. GetFeatures scrubs it out of the response with a regexp.
        new Fixture("nan-coordinate",
            "ST_SetSRID(ST_MakePoint('NaN'::float8, 50.5), 4326)",
            "{\"type\":\"Point\",\"coordinates\":[NaN,50.5]}",
            "{\"type\":\"Point\",\"coordinates\":[NaN,50.5]}",
            4326, "POINT"),

        new Fixture("extremes",
            "ST_Force3D(ST_SetSRID(ST_MakePoint(-179.999999995, -89.999999995), 4326))",
            "{\"type\":\"Point\",\"coordinates\":[-180,-90,0]}",
            "{\"type\":\"Point\",\"coordinates\":[-180,-90,0]}",
            4326, "POINT"),

        /*
         * The SRID of the input is discarded and 4326 is imposed. That is what the WKT round trip did, by passing
         * 4326 to st_geomfromtext, so the replacement has to keep doing it.
         */
        new Fixture("non-4326-input",
            "ST_SetSRID(ST_MakePoint(1234567.891234, 987654.321987), 3857)",
            "{\"type\":\"Point\",\"coordinates\":[1234567.891234,987654.321987]}",
            "{\"type\":\"Point\",\"coordinates\":[1234567.891234,987654.321987]}",
            4326, "POINT")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixtures")
  void reducesPrecisionAsBefore(Fixture fixture) throws Exception {
    Reduced actual = reduce(fixture.geometrySql());

    assertEquals(fixture.expectedJson8(), actual.json8(),
        "Wrong GeoJSON at 8 digits, the precision every read path uses, for " + fixture.name());
    assertEquals(fixture.expectedJson9(), actual.json9(),
        "Wrong GeoJSON at the 9 digit default for " + fixture.name());
    assertEquals(fixture.expectedSrid(), actual.srid(), "Wrong SRID for " + fixture.name());
    assertEquals(fixture.expectedType(), actual.geometryType(), "Wrong geometry type for " + fixture.name());
  }

  @Test
  void passesNullThrough() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String json = new SQLQuery("SELECT ST_AsGeoJSON(xyz_reduce_precision(NULL::geometry, false)) AS gj")
          .run(dsp, rs -> rs.next() ? rs.getString("gj") : "<no row>");
      assertNull(json, "A NULL geometry has to stay NULL");
    }
  }

  private record Reduced(String json8, String json9, int srid, String geometryType) {}

  private Reduced reduce(String geometrySql) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return new SQLQuery("""
          SELECT ST_AsGeoJSON(xyz_reduce_precision(${{geo}}, false), 8) AS gj8,
                 ST_AsGeoJSON(xyz_reduce_precision(${{geo}}, false))    AS gj9,
                 ST_SRID(xyz_reduce_precision(${{geo}}, false))         AS srid,
                 GeometryType(xyz_reduce_precision(${{geo}}, false))    AS gtype
          """)
          .withQueryFragment("geo", geometrySql)
          .run(dsp, rs -> rs.next()
              ? new Reduced(rs.getString("gj8"), rs.getString("gj9"), rs.getInt("srid"), rs.getString("gtype"))
              : null);
    }
  }
}
