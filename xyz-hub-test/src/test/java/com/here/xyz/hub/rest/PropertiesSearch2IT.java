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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

@SuppressWarnings("unused")
public class PropertiesSearch2IT extends TestSpaceWithFeature {

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
  public static void tearDown() {
    remove();
  }

  @Test
  public void testEqualsNull() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?p.bar=.null").
            then().
            body("features.size()", equalTo(10));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?p.foo.nested=.null").
            then().
            body("features.size()", equalTo(11));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.id=.null").
            then().
            body("features.size()", equalTo(0));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.fooroot=.null").
            then().
            body("features.size()", equalTo(9));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.fooroot.nested=.null").
            then().
            body("features.size()", equalTo(11));
  }

  @Test
  public void testEqualsNotNull() {
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?p.bar!=.null").
            then().
            body("features.size()", equalTo(1));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?p.foo.nested!=.null").
            then().
            body("features.size()", equalTo(0));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.id!=.null").
            then().
            body("features.size()", equalTo(11));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.fooroot!=.null").
            then().
            body("features.size()", equalTo(2));
    given().
            accept(APPLICATION_GEO_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get("/spaces/x-psql-test/search?f.fooroot.nested!=.null").
            then().
            body("features.size()", equalTo(0));
  }
}
