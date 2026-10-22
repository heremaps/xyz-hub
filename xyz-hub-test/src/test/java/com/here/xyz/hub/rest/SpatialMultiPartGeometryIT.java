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

package com.here.xyz.hub.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.coordinates.JTSHelper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

/**
 * Spatial queries with multi-part filter geometries whose parts are far apart, e.g. a MULTIPOINT spread over a continent.
 */
@Category(RestTests.class)
public class SpatialMultiPartGeometryIT extends TestSpaceWithFeature {
  private static final String BASE = "x-psql-spatial-multipart" + TEST_SUFFIX;
  private static final String EXTENSION = BASE + "-ext";
  private static final double EARTH_RADIUS = 6_371_008.8;
  private static final GeometryFactory JTS = new GeometryFactory(new PrecisionModel(), 4326);
  private static final double[][] ANCHORS = {
      {-9.1393, 38.7223}, //Lisbon
      {-3.7038, 40.4168}, //Madrid
      {2.3522, 48.8566}, //Paris
      {4.3517, 50.8503}, //Brussels
      {13.4050, 52.5200}, //Berlin
      {12.5683, 55.6761} //Copenhagen
  };
  private static final String REF_FEATURE = "ref-multipoint";

  @BeforeClass
  public static void setup() {
    remove(EXTENSION);
    remove(BASE);
    createSpaceWithId(BASE);
    createSpaceWithExtension(BASE);

    List<Feature> features = new ArrayList<>();
    for (int a = 0; a < ANCHORS.length; a++) {
      features.add(feature("a" + a + "-in", point(destination(ANCHORS[a], 100, 30 + a * 50))));
      features.add(feature("a" + a + "-out", point(destination(ANCHORS[a], 3000, 200 + a * 20))));
      features.add(feature("a" + a + "-polygon", square(destination(ANCHORS[a], 200, 120 + a * 30), 200)));
    }
    //Inside the bounding box of the anchors, but far away from all of them
    features.add(feature("envelope-point", point(new double[]{1.0, 45.0})));
    features.add(feature("envelope-polygon", square(new double[]{8.0, 47.0}, 2000)));
    features.add(feature(REF_FEATURE, anchors()));
    writeFeatures(BASE, features);
  }

  @AfterClass
  public static void tearDown() {
    remove(EXTENSION);
    remove(BASE);
  }

  @Test
  public void multiPointWithRadius() {
    assertEquals(expectedNearAnchors(), ids(postSpatial(BASE, anchors(), "radius=500")));
  }

  @Test
  public void multiPointWithRadiusAndClip() throws Exception {
    FeatureCollection result = postSpatial(BASE, anchors(), "radius=500&clip=true");
    assertEquals(expectedNearAnchors(), ids(result));
    for (Feature feature : result.getFeatures())
      for (Coordinate coordinate : toJts(feature.getGeometry()).getCoordinates())
        assertTrue(feature.getId() + " has a coordinate outside of the buffer: " + coordinate,
            distanceToClosestAnchor(coordinate) <= 600);
  }

  @Test
  public void multiPolygonWithoutRadius() {
    org.locationtech.jts.geom.MultiPolygon filter = JTS.createMultiPolygon(new Polygon[]{
        square(ANCHORS[0], 1000), square(ANCHORS[5], 1000)});
    Set<String> expected = Set.of("a0-in", "a0-polygon", "a5-in", "a5-polygon", REF_FEATURE);
    assertEquals(new TreeSet<>(expected), ids(postSpatial(BASE, filter, "radius=0")));
  }

  @Test
  public void compositeSpace() {
    //Override one included base feature by moving it far away, and add a feature only to the extension
    writeFeatures(EXTENSION, List.of(
        feature("a2-in", point(destination(ANCHORS[2], 10_000, 90))),
        feature("ext-in", point(destination(ANCHORS[4], 100, 300)))));

    Set<String> expected = expectedNearAnchors();
    expected.remove("a2-in");
    expected.add("ext-in");
    assertEquals(expected, ids(postSpatial(EXTENSION, anchors(), "radius=500")));
  }

  @Test
  public void referencedMultiPointWithRadius() {
    String body = given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + BASE + "/spatial?refSpaceId=" + BASE + "&refFeatureId=" + REF_FEATURE + "&radius=500")
        .then()
        .statusCode(OK.code())
        .extract().body().asString();
    assertEquals(expectedNearAnchors(), ids(deserialize(body)));
  }

  //The feature near every anchor plus the referenced MultiPoint, which consists of the anchors
  private static Set<String> expectedNearAnchors() {
    Set<String> expected = new TreeSet<>();
    for (int a = 0; a < ANCHORS.length; a++) {
      expected.add("a" + a + "-in");
      expected.add("a" + a + "-polygon");
    }
    expected.add(REF_FEATURE);
    return expected;
  }

  private static FeatureCollection postSpatial(String spaceId, org.locationtech.jts.geom.Geometry filter, String query) {
    String body = given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(((Geometry) Geometry.convertJTSGeometry(filter)).serialize())
        .when()
        .post(getSpacesPath() + "/" + spaceId + "/spatial?" + query)
        .then()
        .statusCode(OK.code())
        .extract().body().asString();
    return deserialize(body);
  }

  private static FeatureCollection deserialize(String body) {
    try {
      return XyzSerializable.deserialize(body);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<String> ids(FeatureCollection featureCollection) {
    Set<String> ids = new TreeSet<>();
    try {
      for (Feature feature : featureCollection.getFeatures())
        ids.add(feature.getId());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertFalse(ids.isEmpty());
    return ids;
  }

  private static void writeFeatures(String spaceId, List<Feature> features) {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new FeatureCollection().withFeatures(features).serialize())
        .when()
        .post(getSpacesPath() + "/" + spaceId + "/features")
        .then()
        .statusCode(OK.code());
  }

  private static Feature feature(String id, org.locationtech.jts.geom.Geometry geometry) {
    return new Feature()
        .withId(id)
        .withGeometry(Geometry.convertJTSGeometry(geometry))
        .withProperties(new Properties().with("name", id));
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
    double phi2 = Math.asin(Math.sin(phi1) * Math.cos(delta) + Math.cos(phi1) * Math.sin(delta) * Math.cos(theta));
    double lambda2 = Math.toRadians(lonLat[0]) + Math.atan2(Math.sin(theta) * Math.sin(delta) * Math.cos(phi1),
        Math.cos(delta) - Math.sin(phi1) * Math.sin(phi2));
    return new double[]{Math.toDegrees(lambda2), Math.toDegrees(phi2)};
  }

  private static double distanceToClosestAnchor(Coordinate coordinate) {
    double min = Double.MAX_VALUE;
    for (double[] anchor : ANCHORS)
      min = Math.min(min, haversine(anchor, new double[]{coordinate.x, coordinate.y}));
    return min;
  }

  private static double haversine(double[] a, double[] b) {
    double dPhi = Math.toRadians(b[1] - a[1]);
    double dLambda = Math.toRadians(b[0] - a[0]);
    double h = Math.pow(Math.sin(dPhi / 2), 2)
        + Math.cos(Math.toRadians(a[1])) * Math.cos(Math.toRadians(b[1])) * Math.pow(Math.sin(dLambda / 2), 2);
    return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(h));
  }

  private static org.locationtech.jts.geom.Geometry toJts(Geometry geometry) {
    return JTSHelper.toGeometry(geometry);
  }
}
