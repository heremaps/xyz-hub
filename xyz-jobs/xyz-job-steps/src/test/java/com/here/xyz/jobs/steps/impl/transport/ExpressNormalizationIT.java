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

package com.here.xyz.jobs.steps.impl.transport;

import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.getDataSourceProvider;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;

/**
 * Pins the behavior of {@code normalize_default_import_feature}, the per-feature normalization of the express
 * import writer.
 *
 * <h2>Why this test exists</h2>
 * <p>The normalization is a pure function of its input, which makes it the one part of the express writer whose
 * behavior can be described exhaustively instead of only being observed through a write. The expectations below
 * were captured from the implementation as it was before the normalization got optimized, so they document the
 * behavior that has to be preserved rather than the behavior somebody intended.</p>
 *
 * <p>Several of these cases pin subtleties which are easy to break while restructuring, in particular the order
 * in which {@code jsonb_strip_nulls} is applied:</p>
 * <ul>
 *   <li>Nulls are stripped from JSON <em>objects</em> only, array elements survive ({@code nested-nulls}).</li>
 *   <li>Stripping happens <em>before</em> the deleted flag is read, so an explicit {@code "deleted": null}
 *       disappears and the feature counts as not deleted ({@code deleted-null}).</li>
 *   <li>Stripping happens <em>before</em> the geometry is extracted, so {@code "geometry": null} is removed by
 *       the stripping rather than by the explicit geometry removal ({@code null-geometry}).</li>
 *   <li>A deleted feature is reduced to a stub, which drops everything else the input carried, including other
 *       properties ({@code deleted-true} loses its {@code keep} property).</li>
 * </ul>
 */
public class ExpressNormalizationIT {

  /**
   * Stands in for the random id of a feature which had none, both in the expected {@code jsondata} and in the
   * comparison, since the generated value can not be asserted.
   */
  private static final String GENERATED_ID = "<GENERATED_ID>";

  private static final String NO_GEOMETRY = "<null>";

  static Stream<Fixture> fixtures() {
    return Stream.of(
        new Fixture("plain-with-id",
            "{\"type\":\"Feature\",\"id\":\"f1\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50]},\"properties\":{\"a\":1}}",
            "f1",
            "{\"id\": \"f1\", \"type\": \"Feature\", \"properties\": {\"a\": 1, \"@ns:com:here:xyz\": {}}}",
            "{\"type\":\"Point\",\"coordinates\":[8,50,0]}",
            false),

        //Any input without a usable string id gets one generated
        new Fixture("empty-string-id",
            "{\"type\":\"Feature\",\"id\":\"\",\"properties\":{}}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("numeric-id",
            "{\"type\":\"Feature\",\"id\":42,\"properties\":{}}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("object-id",
            "{\"type\":\"Feature\",\"id\":{\"x\":1},\"properties\":{}}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("null-id",
            "{\"type\":\"Feature\",\"id\":null,\"properties\":{}}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("absent-id",
            "{\"type\":\"Feature\",\"properties\":{}}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        //The type is always forced to "Feature", whatever the input claimed
        new Fixture("absent-type",
            "{\"id\":\"f7\",\"properties\":{}}",
            "f7",
            "{\"id\": \"f7\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("wrong-type-value",
            "{\"type\":\"Point\",\"id\":\"f8\",\"properties\":{}}",
            "f8",
            "{\"id\": \"f8\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("empty-type",
            "{\"type\":\"\",\"id\":\"f9\",\"properties\":{}}",
            "f9",
            "{\"id\": \"f9\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        new Fixture("bbox-present",
            "{\"type\":\"Feature\",\"id\":\"f10\",\"bbox\":[0,0,1,1],\"properties\":{}}",
            "f10",
            "{\"id\": \"f10\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        //Properties and the xyz namespace are coerced into objects
        new Fixture("absent-properties",
            "{\"type\":\"Feature\",\"id\":\"f11\"}",
            "f11",
            "{\"id\": \"f11\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("non-object-properties",
            "{\"type\":\"Feature\",\"id\":\"f12\",\"properties\":\"nope\"}",
            "f12",
            "{\"id\": \"f12\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("absent-xyz-ns",
            "{\"type\":\"Feature\",\"id\":\"f13\",\"properties\":{\"a\":1}}",
            "f13",
            "{\"id\": \"f13\", \"type\": \"Feature\", \"properties\": {\"a\": 1, \"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("non-object-xyz-ns",
            "{\"type\":\"Feature\",\"id\":\"f14\",\"properties\":{\"@ns:com:here:xyz\":5}}",
            "f14",
            "{\"id\": \"f14\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        /*
         * A deleted feature is reduced to a stub. Note that the "keep" property and the geometry of the input are
         * intentionally dropped, and that the geometry column stays empty.
         */
        new Fixture("deleted-true",
            "{\"type\":\"Feature\",\"id\":\"f15\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,2]},"
                + "\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":true},\"keep\":\"me\"}}",
            "f15",
            "{\"id\": \"f15\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {\"deleted\": true}}}",
            NO_GEOMETRY, true),
        new Fixture("deleted-false",
            "{\"type\":\"Feature\",\"id\":\"f16\",\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":false}}}",
            "f16",
            "{\"id\": \"f16\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {\"deleted\": false}}}",
            NO_GEOMETRY, false),
        //An explicit null is stripped before the flag is read, so it does not count as a deletion
        new Fixture("deleted-null",
            "{\"type\":\"Feature\",\"id\":\"f17\",\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":null}}}",
            "f17",
            "{\"id\": \"f17\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        new Fixture("null-geometry",
            "{\"type\":\"Feature\",\"id\":\"f19\",\"geometry\":null,\"properties\":{}}",
            "f19",
            "{\"id\": \"f19\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        //A 2D geometry is forced to 3D
        new Fixture("geometry-2d",
            "{\"type\":\"Feature\",\"id\":\"f20\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8.5,50.5]},\"properties\":{}}",
            "f20",
            "{\"id\": \"f20\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            "{\"type\":\"Point\",\"coordinates\":[8.5,50.5,0]}", false),
        new Fixture("geometry-3d",
            "{\"type\":\"Feature\",\"id\":\"f21\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8.5,50.5,12.25]},\"properties\":{}}",
            "f21",
            "{\"id\": \"f21\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            "{\"type\":\"Point\",\"coordinates\":[8.5,50.5,12.25]}", false),
        //Coordinates are reduced to 8 decimal digits by xyz_reduce_precision
        new Fixture("high-precision",
            "{\"type\":\"Feature\",\"id\":\"f22\",\"geometry\":{\"type\":\"Point\","
                + "\"coordinates\":[8.1234567891234,50.9876543219876]},\"properties\":{}}",
            "f22",
            "{\"id\": \"f22\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            "{\"type\":\"Point\",\"coordinates\":[8.12345679,50.98765432,0]}", false),
        new Fixture("polygon",
            "{\"type\":\"Feature\",\"id\":\"f27\",\"geometry\":{\"type\":\"Polygon\","
                + "\"coordinates\":[[[0,0],[1,0],[1,1],[0,0]]]},\"properties\":{}}",
            "f27",
            "{\"id\": \"f27\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            "{\"type\":\"Polygon\",\"coordinates\":[[[0,0,0],[1,0,0],[1,1,0],[0,0,0]]]}", false),

        //Nulls are removed from objects at any depth, but array elements are kept
        new Fixture("nested-nulls",
            "{\"type\":\"Feature\",\"id\":\"f23\",\"properties\":{\"a\":null,\"b\":{\"c\":null,\"d\":1},\"e\":[null,1]}}",
            "f23",
            "{\"id\": \"f23\", \"type\": \"Feature\", \"properties\": {\"b\": {\"d\": 1}, \"e\": [null, 1], "
                + "\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        //Unknown top level keys of the input are preserved
        new Fixture("extra-top-level-keys",
            "{\"type\":\"Feature\",\"id\":\"f24\",\"customKey\":\"keep\",\"properties\":{}}",
            "f24",
            "{\"id\": \"f24\", \"type\": \"Feature\", \"customKey\": \"keep\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),

        new Fixture("empty-object",
            "{}",
            GENERATED_ID,
            "{\"id\": \"" + GENERATED_ID + "\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("null-properties",
            "{\"type\":\"Feature\",\"id\":\"f30\",\"properties\":null}",
            "f30",
            "{\"id\": \"f30\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        new Fixture("null-top-level-key",
            "{\"type\":\"Feature\",\"id\":\"f31\",\"customKey\":null,\"properties\":{}}",
            "f31",
            "{\"id\": \"f31\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false),
        //A string is accepted as a boolean by the deleted check
        new Fixture("deleted-string-true",
            "{\"type\":\"Feature\",\"id\":\"f32\",\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":\"true\"}}}",
            "f32",
            "{\"id\": \"f32\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {\"deleted\": true}}}",
            NO_GEOMETRY, true),
        //The stub of a deleted feature drops the geometry and any other namespace entry
        new Fixture("deleted-with-geometry",
            "{\"type\":\"Feature\",\"id\":\"f33\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[3,4,5]},"
                + "\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":true,\"other\":7}}}",
            "f33",
            "{\"id\": \"f33\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {\"deleted\": true}}}",
            NO_GEOMETRY, true),
        new Fixture("null-bbox",
            "{\"type\":\"Feature\",\"id\":\"f34\",\"bbox\":null,\"properties\":{}}",
            "f34",
            "{\"id\": \"f34\", \"type\": \"Feature\", \"properties\": {\"@ns:com:here:xyz\": {}}}",
            NO_GEOMETRY, false)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixtures")
  void normalizesAsBefore(Fixture fixture) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      Normalized actual = normalize(dsp, fixture.input());

      boolean idExpectedToBeGenerated = GENERATED_ID.equals(fixture.expectedFeatureId());
      assertEquals(idExpectedToBeGenerated, actual.idWasGenerated(),
          "Wrong id_was_generated for fixture " + fixture.name());

      if (idExpectedToBeGenerated) {
        assertNotNull(actual.featureId(), "An id should have been generated for fixture " + fixture.name());
        assertFalse(actual.featureId().isBlank(), "The generated id must not be blank for fixture " + fixture.name());
      }
      else
        assertEquals(fixture.expectedFeatureId(), actual.featureId(), "Wrong feature id for fixture " + fixture.name());

      //The random part of a generated id is masked, so that the rest of the document can still be compared
      String comparableJsondata = idExpectedToBeGenerated
          ? actual.jsondata().replace(actual.featureId(), GENERATED_ID) : actual.jsondata();
      assertEquals(fixture.expectedJsondata(), comparableJsondata, "Wrong jsondata for fixture " + fixture.name());

      assertEquals(fixture.expectedGeometry(), actual.geometry(), "Wrong geometry for fixture " + fixture.name());
      assertEquals(fixture.expectedIsDeleted(), actual.isDeleted(), "Wrong is_deleted for fixture " + fixture.name());
    }
  }

  /**
   * Anything which is not a JSON object has to be rejected as an illegal argument, so that a malformed line of an
   * import file does not silently end up as a feature.
   */
  @ParameterizedTest
  @ValueSource(strings = {"[1,2,3]", "\"hello\"", "42", "null", "true"})
  void rejectsInputWhichIsNoJsonObject(String input) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLException exception = assertThrows(SQLException.class, () -> normalize(dsp, input));
      assertEquals("XYZ40", exception.getSQLState());
      assertTrue(exception.getMessage().contains("Imported entity must be a GeoJSON object."),
          "Unexpected message: " + exception.getMessage());
    }
  }

  private Normalized normalize(DataSourceProvider dsp, String input) throws Exception {
    return new SQLQuery("""
        SELECT feature_id, normalized_jsondata::TEXT AS jsondata,
               COALESCE(ST_AsGeoJSON(normalized_geo), #{noGeometry}) AS geo,
               is_deleted, id_was_generated
          FROM normalize_default_import_feature(#{input})
        """)
        .withNamedParameter("input", input)
        .withNamedParameter("noGeometry", NO_GEOMETRY)
        .run(dsp, rs -> rs.next()
            ? new Normalized(rs.getString("feature_id"), rs.getString("jsondata"), rs.getString("geo"),
                rs.getBoolean("is_deleted"), rs.getBoolean("id_was_generated"))
            : null);
  }

  /**
   * @param expectedFeatureId The expected id, or {@link #GENERATED_ID} if the input carried no usable one
   * @param expectedGeometry The expected geometry as GeoJSON, or {@link #NO_GEOMETRY} if none is expected
   */
  record Fixture(String name, String input, String expectedFeatureId, String expectedJsondata,
      String expectedGeometry, boolean expectedIsDeleted) {

    @Override
    public String toString() {
      return name;
    }
  }

  private record Normalized(String featureId, String jsondata, String geometry, boolean isDeleted,
      boolean idWasGenerated) {}
}
