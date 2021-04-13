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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;

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
}
