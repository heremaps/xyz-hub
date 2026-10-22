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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.models.geojson.coordinates.JTSHelper;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.Random;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

/**
 * Pins the results of spatial queries with multi-part filter geometries to the results of the plain
 * "ST_isvalid(F) AND ST_Intersects(geo, F)" predicate, evaluated directly on the space table.
 */
public class PSQLSpatialMultiPartIT extends PSQLAbstractIT {
  private static final double EARTH_RADIUS = 6_371_008.8;
  private static final GeometryFactory JTS = new GeometryFactory(new PrecisionModel(), 4326);
  private static final double[][] ANCHORS = {
      {-9.1393, 38.7223}, //Lisbon
      {-3.7038, 40.4168}, //Madrid
      {2.1734, 41.3851}, //Barcelona
      {2.3522, 48.8566}, //Paris
      {4.3517, 50.8503}, //Brussels
      {4.9041, 52.3676}, //Amsterdam
      {13.4050, 52.5200}, //Berlin
      {12.5683, 55.6761}, //Copenhagen
      {14.4378, 50.0755}, //Prague
      {16.3738, 48.2082}, //Vienna
      {12.4964, 41.9028}, //Rome
      {18.0686, 59.3293} //Stockholm
  };
  private static final int[] DISTANCES = {100, 300, 450, 480, 495, 505, 520, 600, 1000, 5000};
  private static final String SPACE_PREFIX = PSQLSpatialMultiPartIT.class.getSimpleName().toLowerCase() + "_"
      + Random.randomAlpha(5).toLowerCase();
  private static final String BASE = SPACE_PREFIX + "_base";
  private static final String EXTENSION = SPACE_PREFIX + "_ext";
  private static final Map<String, Object> EXTENSION_PARAMS = Map.of("extends", Map.of("spaceId", BASE));
  private static final Map<String, String> KINDS = new TreeMap<>();

  @BeforeAll
  public static void createFixture() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, BASE);
    invokeCreateTestSpace(defaultTestConnectorParams, new Space().withId(EXTENSION), EXTENSION_PARAMS);
    insert(BASE, null, buildFixture());
  }

  @AfterAll
  public static void deleteSpaces() throws Exception {
    invokeDeleteTestSpace(defaultTestConnectorParams, EXTENSION, EXTENSION_PARAMS);
    invokeDeleteTestSpace(defaultTestConnectorParams, BASE, null);
  }

  @Test
  public void multiPointWithRadius() throws Exception {
    assertSameAsLegacy(anchors(), 500);
  }

  @Test
  public void multiPointWithoutRadius() throws Exception {
    //Only the features exactly on an anchor intersect the points themselves
    assertSameAsLegacy(anchors(), 0);
  }

  @Test
  public void multiLineStringWithRadius() throws Exception {
    assertSameAsLegacy(JTS.createMultiLineString(new LineString[]{
        line(destination(ANCHORS[0], 50, 0), 400, 90),
        line(destination(ANCHORS[7], 50, 180), 400, 90)}), 100);
  }

  @Test
  public void multiPolygonWithoutRadius() throws Exception {
    assertSameAsLegacy(JTS.createMultiPolygon(new Polygon[]{square(ANCHORS[1], 700), square(ANCHORS[11], 700)}), 0);
  }

  @Test
  public void invalidMultiPolygonReturnsNothing() throws Exception {
    //Overlapping parts make the MultiPolygon invalid, ST_isvalid() then filters out everything
    Polygon first = square(ANCHORS[3], 700);
    Polygon second = square(destination(ANCHORS[3], 200, 90), 700);
    org.locationtech.jts.geom.MultiPolygon invalid = JTS.createMultiPolygon(new Polygon[]{first, second});
    assertFalse(invalid.isValid());
    assertEquals(Set.of(), legacyIds(BASE, invalid, 0));
    assertEquals(Set.of(), queryIds(event(BASE, invalid, 0)));
  }

  @Test
  public void geometryCollectionWithRadius() throws Exception {
    assertSameAsLegacy(JTS.createGeometryCollection(new org.locationtech.jts.geom.Geometry[]{
        point(ANCHORS[4]), square(ANCHORS[9], 600)}), 200);
  }

  @Test
  public void multiPolygonWithNegativeRadius() throws Exception {
    assertSameAsLegacy(JTS.createMultiPolygon(new Polygon[]{square(ANCHORS[2], 1400), square(ANCHORS[10], 1400)}), -100);
  }

  @Test
  public void multiPointWithRadiusAndPropertiesQuery() throws Exception {
    PropertyQueryList pql = new PropertyQueryList();
    pql.add(new PropertyQuery().withKey("properties.kind").withOperation(QueryOperation.EQUALS)
        .withValues(new ArrayList<>(List.of("point"))));
    PropertiesQuery propertiesQuery = new PropertiesQuery();
    propertiesQuery.add(pql);

    Set<String> expected = new TreeSet<>(legacyIds(BASE, anchors(), 500));
    expected.removeIf(id -> !"point".equals(KINDS.get(id)));
    assertFalse(expected.isEmpty());
    assertEquals(expected, queryIds(event(BASE, anchors(), 500).withPropertiesQuery(propertiesQuery)));
  }

  @Test
  public void multiPointWithRadiusAndClip() throws Exception {
    Map<String, String> expected = legacyClippedGeometries(BASE, anchors(), 500);
    assertFalse(expected.isEmpty());

    FeatureCollection result = query(event(BASE, anchors(), 500).withClip(true));
    Map<String, Geometry> actual = new TreeMap<>();
    for (Feature feature : result.getFeatures())
      actual.put(feature.getId(), feature.getGeometry());

    assertEquals(expected.keySet(), actual.keySet());
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      org.locationtech.jts.geom.Geometry expectedGeometry = toJts(XyzSerializable.deserialize(entry.getValue(), Geometry.class));
      org.locationtech.jts.geom.Geometry actualGeometry = toJts(actual.get(entry.getKey()));
      assertTrue(expectedGeometry.equalsExact(actualGeometry, 1e-7),
          entry.getKey() + ": expected " + expectedGeometry + " but was " + actualGeometry);
    }
  }

  @Test
  public void singlePartGeometriesWithRadius() throws Exception {
    assertSameAsLegacy(square(ANCHORS[5], 600), 500);
    assertSameAsLegacy(point(ANCHORS[6]), 500);
  }

  @Test
  public void compositeSpaceWithMultiPointAndRadius() throws Exception {
    Set<String> baseIds = legacyIds(BASE, anchors(), 500);
    String moved = "a3-p100-0";
    String deleted = "a8-p300-1";
    assertTrue(baseIds.contains(moved));
    assertTrue(baseIds.contains(deleted));

    //Move one included base feature 10 km away and add an extension-only feature next to an anchor
    insert(EXTENSION, EXTENSION_PARAMS, List.of(
        feature(moved, point(destination(ANCHORS[3], 10_000, 45)), "point"),
        feature("ext-only", point(destination(ANCHORS[6], 100, 200)), "point")));
    Map<String, String> deletes = new HashMap<>();
    deletes.put(deleted, null);
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withSpace(EXTENSION)
        .withConnectorParams(defaultTestConnectorParams)
        .withParams(EXTENSION_PARAMS)
        .withDeleteFeatures(deletes))));

    Set<String> expected = new TreeSet<>(baseIds);
    expected.remove(moved);
    expected.remove(deleted);
    expected.add("ext-only");

    assertEquals(expected, queryIds(event(EXTENSION, anchors(), 500).withParams(EXTENSION_PARAMS).withContext(DEFAULT)));
  }

  private static void assertSameAsLegacy(org.locationtech.jts.geom.Geometry filter, int radius) throws Exception {
    Set<String> expected = legacyIds(BASE, filter, radius);
    assertFalse(expected.isEmpty(), "The legacy predicate must match something, otherwise the comparison is vacuous");
    assertEquals(expected, queryIds(event(BASE, filter, radius)));
  }

  private static GetFeaturesByGeometryEvent event(String spaceId, org.locationtech.jts.geom.Geometry filter, int radius) {
    return new GetFeaturesByGeometryEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(spaceId)
        .withGeometry(Geometry.convertJTSGeometry(filter))
        .withRadius(radius)
        .withLimit(100_000);
  }

  private static FeatureCollection query(GetFeaturesByGeometryEvent event) throws Exception {
    FeatureCollection result = deserializeResponse(invokeLambda(event));
    assertNotNull(result);
    return result;
  }

  private static Set<String> queryIds(GetFeaturesByGeometryEvent event) throws Exception {
    Set<String> ids = new TreeSet<>();
    for (Feature feature : query(event).getFeatures())
      ids.add(feature.getId());
    return ids;
  }

  //The filter geometry exactly as GetFeaturesByGeometry builds it
  private static String legacyFilter(int radius) {
    return radius != 0
        ? "ST_Buffer(st_force3d(ST_GeomFromText(?))::geography, ?)::geometry"
        : "st_force3d(ST_GeomFromText(?, 4326))";
  }

  private static Set<String> legacyIds(String table, org.locationtech.jts.geom.Geometry filter, int radius) throws Exception {
    Set<String> ids = new TreeSet<>();
    for (String[] row : legacyQuery(table, "t.id, NULL", filter, radius))
      ids.add(row[0]);
    return ids;
  }

  private static Map<String, String> legacyClippedGeometries(String table, org.locationtech.jts.geom.Geometry filter, int radius)
      throws Exception {
    Map<String, String> geometries = new TreeMap<>();
    for (String[] row : legacyQuery(table, "t.id, xyz_as_geojson(xyz_clip_geometry(t.geo, f.g), 8)", filter, radius))
      geometries.put(row[0], row[1]);
    return geometries;
  }

  private static List<String[]> legacyQuery(String table, String selection, org.locationtech.jts.geom.Geometry filter, int radius)
      throws Exception {
    String sql = "WITH f AS (SELECT " + legacyFilter(radius) + " AS g) SELECT " + selection + " FROM public.\"" + table
        + "\" t, f WHERE ST_isvalid(f.g) AND ST_Intersects(t.geo, f.g)";
    List<String[]> rows = new ArrayList<>();
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, WKTHelper.geometryToWKT2d(Geometry.convertJTSGeometry(filter)));
      if (radius != 0)
        statement.setInt(2, radius);
      try (ResultSet result = statement.executeQuery()) {
        while (result.next())
          rows.add(new String[]{result.getString(1), result.getString(2)});
      }
    }
    return rows;
  }

  private static void insert(String spaceId, Map<String, Object> params, List<Feature> features) throws Exception {
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(spaceId)
        .withParams(params)
        .withTransaction(true)
        //The handler modifies the list
        .withInsertFeatures(new ArrayList<>(features)))));
  }

  private static List<Feature> buildFixture() {
    java.util.Random random = new java.util.Random(2816);
    List<Feature> features = new ArrayList<>();
    for (int a = 0; a < ANCHORS.length; a++) {
      double[] anchor = ANCHORS[a];
      String prefix = "a" + a;
      for (int distance : DISTANCES)
        for (int k = 0; k < 4; k++) {
          double bearing = k * 90 + random.nextDouble() * 90;
          features.add(feature(prefix + "-p" + distance + "-" + k, point(destination(anchor, distance, bearing)), "point"));
        }
      features.add(feature(prefix + "-anchor", point(anchor), "point"));
      double bearing = random.nextDouble() * 360;
      features.add(feature(prefix + "-line200", line(destination(anchor, 200, bearing), 1000, bearing + 90), "line"));
      features.add(feature(prefix + "-line2000", line(destination(anchor, 2000, bearing), 1000, bearing + 90), "line"));
      features.add(feature(prefix + "-polygon", square(anchor, 600), "polygon"));
      features.add(feature(prefix + "-multipolygon", JTS.createMultiPolygon(new Polygon[]{
          square(destination(anchor, 450, bearing + 180), 200),
          square(destination(anchor, 50_000, bearing), 200)}), "multipolygon"));
    }
    //Features inside the bounding box of the anchors, but far away from all of them
    for (int a = 0; a + 1 < ANCHORS.length; a++) {
      double[] middle = {(ANCHORS[a][0] + ANCHORS[a + 1][0]) / 2, (ANCHORS[a][1] + ANCHORS[a + 1][1]) / 2};
      features.add(feature("envelope-point-" + a, point(middle), "point"));
    }
    features.add(feature("envelope-polygon", square(new double[]{5.0, 45.0}, 5000), "polygon"));
    return features;
  }

  private static Feature feature(String id, org.locationtech.jts.geom.Geometry geometry, String kind) {
    KINDS.put(id, kind);
    return new Feature()
        .withId(id)
        .withGeometry(Geometry.convertJTSGeometry(geometry))
        .withProperties(new Properties().with("kind", kind).withXyzNamespace(new XyzNamespace()));
  }

  private static org.locationtech.jts.geom.MultiPoint anchors() {
    Point[] points = new Point[ANCHORS.length];
    for (int i = 0; i < ANCHORS.length; i++)
      points[i] = point(ANCHORS[i]);
    return JTS.createMultiPoint(points);
  }

  private static Point point(double[] lonLat) {
    return JTS.createPoint(new Coordinate(lonLat[0], lonLat[1]));
  }

  //A line of the given length through the center, in direction of the bearing
  private static LineString line(double[] center, double length, double bearing) {
    double[] start = destination(center, length / 2, bearing + 180);
    double[] end = destination(center, length / 2, bearing);
    return JTS.createLineString(new Coordinate[]{new Coordinate(start[0], start[1]), new Coordinate(end[0], end[1])});
  }

  //An axis-aligned square with the given side length around the center
  private static Polygon square(double[] center, double side) {
    double halfLat = destination(center, side / 2, 0)[1] - center[1];
    double halfLon = destination(center, side / 2, 90)[0] - center[0];
    return JTS.createPolygon(new Coordinate[]{
        new Coordinate(center[0] - halfLon, center[1] - halfLat),
        new Coordinate(center[0] + halfLon, center[1] - halfLat),
        new Coordinate(center[0] + halfLon, center[1] + halfLat),
        new Coordinate(center[0] - halfLon, center[1] + halfLat),
        new Coordinate(center[0] - halfLon, center[1] - halfLat)});
  }

  //Destination point on a sphere, given a start point, a distance in meters and a bearing in degrees
  private static double[] destination(double[] lonLat, double distance, double bearing) {
    double delta = distance / EARTH_RADIUS;
    double theta = Math.toRadians(bearing);
    double phi1 = Math.toRadians(lonLat[1]);
    double lambda1 = Math.toRadians(lonLat[0]);
    double phi2 = Math.asin(Math.sin(phi1) * Math.cos(delta) + Math.cos(phi1) * Math.sin(delta) * Math.cos(theta));
    double lambda2 = lambda1 + Math.atan2(Math.sin(theta) * Math.sin(delta) * Math.cos(phi1),
        Math.cos(delta) - Math.sin(phi1) * Math.sin(phi2));
    return new double[]{Math.toDegrees(lambda2), Math.toDegrees(phi2)};
  }

  private static org.locationtech.jts.geom.Geometry toJts(Geometry geometry) {
    org.locationtech.jts.geom.Geometry jts = JTSHelper.toGeometry(geometry);
    jts.normalize();
    return jts;
  }
}
