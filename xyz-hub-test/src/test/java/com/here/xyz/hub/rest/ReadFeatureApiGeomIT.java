/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadFeatureApiGeomIT extends TestSpaceWithFeature {
  @BeforeClass
  public static void setup() {
    remove();
    createSpace();
    addFeatures();
  }

  public static void addFeatures(){
    /** Write 11 Features:
     * 3x point
     * 1x multiPoint
     * 2x lineString
     * 1x multilineString
     * 2x polygon
     * 1x multipolygon
     * 1x without geometry
     * */
    given()
            .contentType(APPLICATION_GEO_JSON)
            .accept(APPLICATION_GEO_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(content("/xyz/hub/mixedGeometryTypes.json"))
            .when()
            .put("/spaces/x-psql-test/features")
            .then()
            .statusCode(OK.code())
            .body("features.size()", equalTo(11));
  }

  @AfterClass
  public static void tearDownClass() {
    remove();
  }

  @Test
  public void testGeometryTypeSearch() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=point").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(3));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=multipoint").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=linestring").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(2));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=multilinestring").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=polygon").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(2));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=multipolygon").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));

  }

  @Test
  public void testGeometryTypeWithPropertySearch() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=point&p.foo=1").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(2));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=polygon&p.foo=1").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=MULTIPOLYGON&p.foo=1&p.description=MultiPolygon").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=polygon&p.foo=3").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
  }

  @Test
  public void testGeometryTypeOnPropertyLevel() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?p.geometry.type=onPropertyLevel").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=point&p.geometry.type=onPropertyLevel").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=polygon&p.geometry.type=onPropertyLevel").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=.null&p.geometry.type=onPropertyLevel").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
  }

  @Test
  public void testFindNull() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=.null"). //&p.foo=1
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=.null&p.foo=1").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type=.null&p.foo=2").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(0));
  }

  @Test
  public void testFindNotNull() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type!=.null"). //&p.foo=1
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(10));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type!=.null&p.foo=1").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(8));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/search?f.geometry.type!=.null&p.foo!=2").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(8));
  }

  @Test
  public void testSpatialH3Search() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=851faeaffffffff").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(3));

    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=851faeaffffffff&p.foo=2").
            then().
            statusCode(OK.code()).
            body("features.size()", equalTo(1)).
            body("features[0].properties.foo", equalTo(2));
  }

  @Test
  public void testClipping() throws JsonProcessingException {
    final GeometryFactory gf = new GeometryFactory();
    /** Clipping with h3Index */
    String body = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=871faeba8ffffff&clip=true&radius=0").
            getBody().asString();

    FeatureCollection fc = XyzSerializable.deserialize(body);
    assertEquals(1,fc.getFeatures().size());
    Feature f = fc.getFeatures().get(0);
    Polygon fPolygon = (Polygon) f.getGeometry();

    //Polygon of input hexbin geom "871faeba8ffffff"
    Geometry h3Polgon = XyzSerializable.deserialize("{\"type\":\"Polygon\",\"coordinates\":[[[8.49566344,50.21819625],[8.497293,50.2065306],[8.51474719,50.2024344],[8.53057711,50.21000354],[8.52895268,50.22167014],[8.51149321,50.22576665],[8.49566344,50.21819625]]]}");

    //Check if all Coordinates of the clipped feature are inside the h3Polygon
    for (Coordinate c: fPolygon.getJTSGeometry().getCoordinates()) {
      Point p = gf.createPoint(c);
      assertTrue(p.within(h3Polgon.getJTSGeometry().buffer(0.00000001)));
      assertEquals(Double.valueOf(0), Double.valueOf(p.distance(h3Polgon.getJTSGeometry().buffer(0.00000001))));
    }

    /** Clipping OFF */
    body = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=871faeba8ffffff&clip=false").
            getBody().asString();

    fc = XyzSerializable.deserialize(body);
    assertEquals(1,fc.getFeatures().size());
    f = fc.getFeatures().get(0);
    fPolygon = (Polygon) f.getGeometry();

    String body2 = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=871faeba8ffffff").
            getBody().asString();

    FeatureCollection fc2 = XyzSerializable.deserialize(body2);
    Feature f2 = fc2.getFeatures().get(0);
    Polygon f2Polygon = (Polygon) f2.getGeometry();
    assertTrue(f2Polygon.getJTSGeometry().equalsExact(fPolygon.getJTSGeometry()));

    /** Clipping with h3Index and radius */
    body = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?h3Index=871faeba8ffffff&clip=true&radius=10000").
            getBody().asString();

    fc = XyzSerializable.deserialize(body);
    assertEquals(4,fc.getFeatures().size());

    //Polygon of input hexbin geom "871faeba8ffffff" extended with radius 10000
    //SELECT ST_AsGeojson(ST_Buffer(h3ToGeoBoundaryDeg( ('x' || '871faeba8ffffff' )::bit(60)::bigint )::geography, 10000)::geometry,8 )
    h3Polgon = XyzSerializable.deserialize("{\"type\":\"Polygon\",\"coordinates\":[[[8.41175525,50.29026668],[8.42759543,50.29784845],[8.45139173,50.30703288],[8.47755295,50.313031],[8.50505021,50.31560676],[8.53280119,50.31465873],[8.55971342,50.31022423],[8.57719975,50.3061206],[8.60104517,50.29882247],[8.62232456,50.28878766],[8.64028253,50.27637322],[8.65428333,50.26202059],[8.66383326,50.24623967],[8.66859786,50.22959062],[8.67018852,50.21792203],[8.66976791,50.19961435],[8.66358202,50.18174051],[8.65189315,50.16504045],[8.6351901,50.15020477],[8.6141672,50.1378464],[8.59834777,50.13028859],[8.57460714,50.12113278],[8.5485341,50.11515618],[8.52114507,50.11259161],[8.49350695,50.11353894],[8.46669614,50.11796127],[8.4492687,50.1220504],[8.42548964,50.12932001],[8.40424215,50.13932122],[8.38627736,50.15170128],[8.372232,50.16602326],[8.36260582,50.1817813],[8.3577436,50.19841828],[8.35608034,50.21008185],[8.35638484,50.22838761],[8.36246391,50.24627435],[8.37407163,50.26300008],[8.39073147,50.27787023],[8.41175525,50.29026668]]]}");
    for (Feature ff : fc.getFeatures()) {
      for (Coordinate c: ff.getGeometry().getJTSGeometry().getCoordinates()) {
        Point p = gf.createPoint(c);
        assertTrue(p.within(h3Polgon.getJTSGeometry().buffer(0.00000001)));
        assertEquals(Double.valueOf(0), Double.valueOf(p.distance(h3Polgon.getJTSGeometry().buffer(0.00000001))));
      }
    }

    /** Clipping with lat+lon and radius */
    body = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?lat=50.102964&lon=8.6709594&clip=true&radius=5500").
            getBody().asString();

    fc = XyzSerializable.deserialize(body);
    assertEquals(5,fc.getFeatures().size());

    Point midPoint = gf.createPoint(new Coordinate(8.6709594,50.102964));

    for (Feature ff : fc.getFeatures()) {
      for (Coordinate c: ff.getGeometry().getJTSGeometry().getCoordinates()) {
        Point p = gf.createPoint(c);
        //Decimal degree
        assertTrue(p.within(midPoint.buffer(0.077)));
      }
    }

    /** Clipping with refFeatureId */
    body = given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
            when().
            get("/spaces/x-psql-test/spatial?refSpaceId=x-psql-test&refFeatureId=foo_polygon&clip=true").
            getBody().asString();

    fc = XyzSerializable.deserialize(body);
    assertEquals(8,fc.getFeatures().size());

    //Take Polygon from mixedGeometryTypes.json
    String fooPolygonString = "{\"type\":\"Polygon\",\"coordinates\":[[[8.5459899,50.020093],[8.8199615,50.020093],[8.8199615,50.188330],[8.5459899,50.188330],[8.5459899,50.020093]]]}";
    Polygon fooPolygon = XyzSerializable.deserialize(fooPolygonString);
    //Check if all Coordinates are inside the fooPolygon
    for (Feature ff : fc.getFeatures()) {
      for (Coordinate c: ff.getGeometry().getJTSGeometry().getCoordinates()) {
        Point p = gf.createPoint(c);
        assertTrue(p.within(fooPolygon.getJTSGeometry().buffer(0.00000001)));
      }
    }

    /** Clipping with POST Geometry and negative radius */
    body = given().
            contentType(APPLICATION_JSON).
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body(fooPolygonString).
            when().post("/spaces/x-psql-test/spatial?clip=true&radius=-5000").
            getBody().asString();
    fc = XyzSerializable.deserialize(body);
    assertEquals(5,fc.getFeatures().size());

    for (Feature ff : fc.getFeatures()) {
      for (Coordinate c: ff.getGeometry().getJTSGeometry().getCoordinates()) {
        Point p = gf.createPoint(c);
        assertTrue(p.within(fooPolygon.getJTSGeometry()));
      }
    }
  }
}
