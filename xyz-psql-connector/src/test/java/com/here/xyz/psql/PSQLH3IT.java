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

package com.here.xyz.psql;

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Random;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;

/**
 * Hexbin clustering and the h3Index filter on top of the extensions h3 and h3_postgis.
 * The features of the different tests are far enough apart, so that the (buffered) bounding box of one test contains no feature
 * of another test. The exception are the antimeridian tests: their bounding boxes overlap, so they only assert on their own cell.
 */
public class PSQLH3IT extends PSQLAbstractIT {
  private static final String SPACE = PSQLH3IT.class.getSimpleName().toLowerCase() + "_" + Random.randomAlpha(5).toLowerCase();
  private static final String POLYGON = polygon(8.50, 50.10, 8.60, 50.20);
  private static final String SMALL_POLYGON = polygon(8.90, 50.40, 8.9001, 50.4001);
  private static final String LINE = "{\"type\":\"LineString\",\"coordinates\":[[9.20,50.05],[9.30,50.12]]}";
  //The center of the pentagon 860800007ffffff (resolution 6)
  private static final String PENTAGON_CENTER = point(10.536199075, 64.700000128);
  //The cell 857eb1c3fffffff (resolution 5) crosses the antimeridian and contains both points at +-179.99
  private static final String ANTIMERIDIAN_CELL = "857eb1c3fffffff";
  //The cell 857eb3b3fffffff (resolution 5) crosses the antimeridian with two parts of about equal size, and contains the four points
  //am-east-1, am-east-2, am-west-1 and am-west-2
  private static final String SPLIT_CELL = "857eb3b3fffffff";

  @BeforeAll
  public static void createFixture() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, SPACE);
    insert(List.of(
        feature("polygon", POLYGON),
        feature("small-polygon", SMALL_POLYGON),
        feature("line", LINE),
        feature("pentagon", PENTAGON_CENTER),
        feature("east", point(179.99, 0.55)),
        feature("west", point(-179.99, 0.55)),
        feature("east-outside", point(179.0, 0.55)),
        feature("am-east-1", point(179.97, 1.146), 1),
        feature("am-east-2", point(179.96, 1.15), 2),
        feature("am-west-1", point(-179.97, 1.146), 3),
        feature("am-west-2", point(-179.96, 1.15), 4)));
  }

  @AfterAll
  public static void deleteSpace() throws Exception {
    invokeDeleteTestSpace(defaultTestConnectorParams, SPACE, null);
  }

  @Test
  public void hexbinOverAPolygonCountsEveryOverlappedCell() throws Exception {
    long cells = coverCount(POLYGON, 7);
    assertTrue(cells > 1, "The polygon must cover several cells, but covers " + cells);

    FeatureCollection result = hexbin(new BBox(8.50, 50.10, 8.60, 50.20), Map.of("absoluteResolution", 7));
    assertEquals(cells, sumOfQty(result));
    for (Feature cell : result.getFeatures()) {
      String type = cell.getGeometry().getJTSGeometry().getGeometryType();
      assertTrue(Set.of("Polygon", "MultiPolygon").contains(type), type);
      assertFalse((Boolean) cell.getProperties().get("pentagon"), "A hexagon is no pentagon");
    }
  }

  @Test
  public void hexbinOverAPolygonSmallerThanACell() throws Exception {
    //With the containment mode "center", such a polygon would get no cell at all
    FeatureCollection result = hexbin(new BBox(8.89, 50.39, 8.91, 50.41), Map.of("absoluteResolution", 7));
    assertEquals(1, result.getFeatures().size());
    assertEquals(1L, sumOfQty(result));
  }

  @Test
  public void hexbinOverALine() throws Exception {
    long cells = coverCount(LINE, 7);
    assertTrue(cells > 1, "The line must cover several cells, but covers " + cells);

    FeatureCollection result = hexbin(new BBox(9.20, 50.05, 9.30, 50.12), Map.of("absoluteResolution", 7));
    assertEquals(cells, sumOfQty(result));
  }

  @Test
  public void hexbinMarksAPentagon() throws Exception {
    //The bounding box results in zoom level 8 and resolution 6
    FeatureCollection result = hexbin(new BBox(10.0, 64.5, 11.1, 64.9), Map.of());
    Feature pentagon = result.getFeatures().stream()
        .filter(cell -> "860800007ffffff".equals(cell.getProperties().get("kind_detail")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("The pentagon cell is missing in " + result.serialize()));
    assertEquals(6, ((Number) pentagon.getProperties().get("resolution")).intValue());
    assertTrue((Boolean) pentagon.getProperties().get("pentagon"));
  }

  @Test
  public void hexbinReturnsTheCellAcrossTheAntimeridian() throws Exception {
    //The cell that is split at the antimeridian is returned as a MultiPolygon and counts the points on both sides.
    //The buffered bounding box is split at the antimeridian as well, and its parts reach beyond +-180, so the cell is contained properly.
    FeatureCollection result = hexbin(new BBox(179.5, 0, 180, 1), Map.of("absoluteResolution", 5));
    Feature cell = cell(result, ANTIMERIDIAN_CELL);
    assertEquals("MultiPolygon", cell.getGeometry().getJTSGeometry().getGeometryType());
    assertEquals(2L, qty(cell));
    for (Feature any : result.getFeatures())
      for (Coordinate coordinate : any.getGeometry().getJTSGeometry().getCoordinates())
        assertTrue(coordinate.x >= -180 && coordinate.x <= 180, "Longitude out of range: " + coordinate.x);
  }

  @ParameterizedTest
  @CsvSource({"179, 0, 180, 2", "-180, 0, -179, 2"})
  public void hexbinCountsBothSidesOfTheAntimeridian(double west, double south, double east, double north) throws Exception {
    FeatureCollection result = hexbin(new BBox(west, south, east, north), Map.of("absoluteResolution", 5, "property", "value"));
    Feature cell = cell(result, SPLIT_CELL);
    assertEquals("MultiPolygon", cell.getGeometry().getJTSGeometry().getGeometryType());
    assertFalse((Boolean) cell.getProperties().get("pentagon"));
    Map<String, Object> statistics = (Map<String, Object>) ((Map<String, Object>) cell.getProperties().get("aggregation")).get("value");
    assertEquals(4L, ((Number) statistics.get("qty")).longValue());
    assertEquals(1.0, ((Number) statistics.get("min")).doubleValue());
    assertEquals(4.0, ((Number) statistics.get("max")).doubleValue());
    assertEquals(10.0, ((Number) statistics.get("sum")).doubleValue());
  }

  @ParameterizedTest
  @CsvSource({"179, 0, 180, 2", "-180, 0, -179, 2"})
  public void hexbinPointModeAcrossTheAntimeridian(double west, double south, double east, double north) throws Exception {
    FeatureCollection result = hexbin(new BBox(west, south, east, north), Map.of("absoluteResolution", 5, "pointmode", true));
    org.locationtech.jts.geom.Geometry geometry = cell(result, SPLIT_CELL).getGeometry().getJTSGeometry();
    //The true center of the cell, not the centroid of its split boundary. For the bbox at -180 it lies at +179.998, outside the bbox.
    assertEquals("Point", geometry.getGeometryType());
    double[] center = center(SPLIT_CELL);
    assertEquals(center[0], geometry.getCoordinate().x, 1e-6);
    assertEquals(center[1], geometry.getCoordinate().y, 1e-6);
  }

  @ParameterizedTest
  @ValueSource(ints = {127, 0})
  public void hexbinTileAtTheAntimeridian(int x) throws Exception {
    //Level 7 results in resolution 5
    BBox tileBBox = WebMercatorTile.forWeb(7, x, 63).getBBox(false);
    FeatureCollection result = deserializeResponse(invokeLambda(tileEvent(x, GEO_JSON, defaultTestConnectorParams)));
    Feature cell = cell(result, SPLIT_CELL);
    assertEquals(4L, qty(cell));
    //Clipped to the tile, so only the part on the side of the tile remains
    org.locationtech.jts.geom.Geometry geometry = cell.getGeometry().getJTSGeometry();
    assertEquals("Polygon", geometry.getGeometryType());
    for (Coordinate coordinate : geometry.getCoordinates())
      assertTrue(coordinate.x >= tileBBox.minLon() && coordinate.x <= tileBBox.maxLon(), "Outside of the tile: " + coordinate.x);
  }

  @ParameterizedTest
  @ValueSource(ints = {127, 0})
  public void hexbinMvtTileAtTheAntimeridian(int x) throws Exception {
    Map<String, Object> connectorParams = new HashMap<>(defaultTestConnectorParams);
    connectorParams.put("mvtSupport", true);
    //The type BinaryResponse is not registered for the JSON deserialization, so the response is read as a tree
    String serialized = invokeLambda(tileEvent(x, MVT, connectorParams));
    JsonNode response = new ObjectMapper().readTree(serialized);
    assertEquals("BinaryResponse", response.path("type").asText(), serialized);
    byte[] bytes = response.path("bytes").binaryValue();
    assertNotNull(bytes, "The tile must not be empty");
    //The connector module has no MVT decoder, but the property values are plain strings in the encoded tile
    assertTrue(new String(bytes, UTF_8).contains(SPLIT_CELL), "The cell " + SPLIT_CELL + " is missing");
  }

  //Proves today's envelope, bit for bit, for the bboxes of the existing ITs and for the cases that are excluded from the split
  @ParameterizedTest
  @CsvSource({"12, 54, 15, 51, 6", "11.25, 48.92249926375824, 22.5, 55.77657301866769, 4", "8.5, 50.1, 8.6, 50.2, 7",
      "10, 64.5, 11.1, 64.9, 6", "170, 0, -170, 10, 2", "-170, 0, 170, 10, 2", "-179.9999, -85, 180, 85, 2", "10, 84, 20, 89, 0",
      "170, 0, 175, 1, 9"})
  public void bufferedBboxAwayFromTheAntimeridianIsUnchanged(double west, double south, double east, double north, int resolution)
      throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        PreparedStatement statement = connection.prepareStatement(
            "SELECT ST_AsEWKB(xyz_h3_buffered_bbox(?, ?, ?, ?, 2.5 * h3_ext.h3_get_hexagon_edge_length_avg(?, 'm'))) = "
                + "ST_AsEWKB(st_envelope(st_buffer(ST_MakeEnvelope(?, ?, ?, ?, 4326)::geography, "
                + "2.5 * h3_ext.h3_get_hexagon_edge_length_avg(?, 'm'))::geometry))")) {
      for (int offset : new int[]{0, 5}) {
        statement.setDouble(offset + 1, west);
        statement.setDouble(offset + 2, south);
        statement.setDouble(offset + 3, east);
        statement.setDouble(offset + 4, north);
        statement.setInt(offset + 5, resolution);
      }
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        assertTrue(result.getBoolean(1));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"179, 0, 180, 2, 178.78, -179.78", "-180, 0, -179, 2, 179.78, -178.78"})
  public void bufferedBboxIsSplitAtTheAntimeridian(double west, double south, double east, double north, double eastPartWest,
      double westPartEast) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        PreparedStatement statement = connection.prepareStatement(
            "SELECT ST_GeometryType(b), ST_NumGeometries(b), "
                + "ST_XMin(ST_GeometryN(b, 1)), ST_XMax(ST_GeometryN(b, 1)), ST_YMin(ST_GeometryN(b, 1)), ST_YMax(ST_GeometryN(b, 1)), "
                + "ST_XMin(ST_GeometryN(b, 2)), ST_XMax(ST_GeometryN(b, 2)), ST_YMin(ST_GeometryN(b, 2)), ST_YMax(ST_GeometryN(b, 2)) "
                + "FROM xyz_h3_buffered_bbox(?, ?, ?, ?, 2.5 * h3_ext.h3_get_hexagon_edge_length_avg(5, 'm')) b")) {
      statement.setDouble(1, west);
      statement.setDouble(2, south);
      statement.setDouble(3, east);
      statement.setDouble(4, north);
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals("ST_MultiPolygon", result.getString(1));
        assertEquals(2, result.getInt(2));
        //The part east of the antimeridian, then the one west of it
        assertEquals(eastPartWest, result.getDouble(3), 0.01);
        assertEquals(181.0, result.getDouble(4));
        assertEquals(-181.0, result.getDouble(7));
        assertEquals(westPartEast, result.getDouble(8), 0.01);
        assertEquals(result.getDouble(5), result.getDouble(9));
        assertEquals(result.getDouble(6), result.getDouble(10));
      }
    }
  }

  @Test
  public void h3IndexAcrossTheAntimeridian() throws Exception {
    FeatureCollection result = deserializeResponse(invokeLambda(h3IndexEvent(ANTIMERIDIAN_CELL)));
    Set<String> ids = new TreeSet<>();
    for (Feature feature : result.getFeatures())
      ids.add(feature.getId());
    assertEquals(Set.of("east", "west"), ids);
  }

  @Test
  public void invalidH3IndexIsAnIllegalArgument() throws Exception {
    //A formatted, but invalid cell, a value that is out of range and a malformed value
    for (String h3Index : new String[]{"871faeba8fffff1", "ffffffffffffffff", "zz"}) {
      XyzResponse response = XyzSerializable.deserialize(invokeLambda(h3IndexEvent(h3Index)));
      assertInstanceOf(ErrorResponse.class, response, h3Index);
      assertEquals(ILLEGAL_ARGUMENT, ((ErrorResponse) response).getError(), h3Index);
      assertFalse(((ErrorResponse) response).getErrorMessage().contains(h3Index), "The message must not echo the input");
    }
  }

  private static GetFeaturesByGeometryEvent h3IndexEvent(String h3Index) {
    return new GetFeaturesByGeometryEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(SPACE)
        .withH3Index(h3Index);
  }

  private static FeatureCollection hexbin(BBox bbox, Map<String, Object> clusteringParams) throws Exception {
    GetFeaturesByBBoxEvent event = new GetFeaturesByBBoxEvent();
    event.setConnectorParams(defaultTestConnectorParams);
    event.setSpace(SPACE);
    event.setBbox(bbox);
    event.setClusteringType("hexbin");
    event.setClusteringParams(new HashMap<>(clusteringParams));
    FeatureCollection result = deserializeResponse(invokeLambda(event));
    assertNotNull(result);
    return result;
  }

  private static GetFeaturesByTileEvent tileEvent(int x, ResponseType responseType, Map<String, Object> connectorParams) {
    return new GetFeaturesByTileEvent()
        .withLevel(7)
        .withX(x)
        .withY(63)
        .withResponseType(responseType)
        .withBbox(WebMercatorTile.forWeb(7, x, 63).getBBox(false))
        .withClip(true)
        .withClusteringType("hexbin")
        .withClusteringParams(new HashMap<>())
        .withConnectorParams(connectorParams)
        .withSpace(SPACE);
  }

  private static Feature cell(FeatureCollection result, String h3Index) throws Exception {
    return result.getFeatures().stream()
        .filter(cell -> h3Index.equals(cell.getProperties().get("kind_detail")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("The cell " + h3Index + " is missing in " + result.serialize()));
  }

  private static long qty(Feature cell) {
    return ((Number) ((Map<String, Object>) cell.getProperties().get("aggregation")).get("qty")).longValue();
  }

  //The longitude and latitude of h3_cell_to_geometry() of the cell
  private static double[] center(String h3Index) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        PreparedStatement statement = connection.prepareStatement(
            "SELECT ST_X(c), ST_Y(c) FROM h3_ext.h3_cell_to_geometry(?::h3_ext.h3index) c")) {
      statement.setString(1, h3Index);
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        return new double[]{result.getDouble(1), result.getDouble(2)};
      }
    }
  }

  private static long sumOfQty(FeatureCollection result) throws Exception {
    long sum = 0;
    for (Feature cell : result.getFeatures())
      sum += ((Number) ((Map<String, Object>) cell.getProperties().get("aggregation")).get("qty")).longValue();
    return sum;
  }

  //The number of cells xyz_h3_cover() returns for the geometry
  private static long coverCount(String geoJson, int resolution) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        PreparedStatement statement = connection.prepareStatement(
            "SELECT count(*) FROM xyz_h3_cover(ST_SetSRID(ST_GeomFromGeoJSON(?), 4326), ?)")) {
      statement.setString(1, geoJson);
      statement.setInt(2, resolution);
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        return result.getLong(1);
      }
    }
  }

  private static void insert(List<Feature> features) throws Exception {
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(SPACE)
        .withTransaction(true)
        //The handler modifies the list
        .withInsertFeatures(new ArrayList<>(features)))));
  }

  private static Feature feature(String id, String geoJson) throws Exception {
    return new Feature()
        .withId(id)
        .withGeometry(XyzSerializable.deserialize(geoJson, Geometry.class))
        .withProperties(new Properties().withXyzNamespace(new XyzNamespace()));
  }

  private static Feature feature(String id, String geoJson, int value) throws Exception {
    return new Feature()
        .withId(id)
        .withGeometry(XyzSerializable.deserialize(geoJson, Geometry.class))
        .withProperties(new Properties().withXyzNamespace(new XyzNamespace()).with("value", value));
  }

  private static String point(double lon, double lat) {
    return "{\"type\":\"Point\",\"coordinates\":[" + lon + "," + lat + "]}";
  }

  private static String polygon(double west, double south, double east, double north) {
    return "{\"type\":\"Polygon\",\"coordinates\":[[[" + west + "," + south + "],[" + east + "," + south + "],[" + east + "," + north
        + "],[" + west + "," + north + "],[" + west + "," + south + "]]]}";
  }
}
