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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreInvalidFeaturesApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    createSpace();
  }

  @After
  public void tearDown() {
    remove();
  }


  @Test
  public void putFeatureWithEmptyCoordinatesObject() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"type\": \"Feature\", \"geometry\":{\"coordinates\":{},\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}").
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void putFeatureWithInvalidCoordinate() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"type\": \"Feature\", \"geometry\":{\"coordinates\":[1],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}").
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void putFeatureWithInvalidGeometryType() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"type\": \"Feature\", \"geometry\":{\"coordinates\":[-2.960847, 53.430828],\"type\":\"Points\"}}],\"type\":\"FeatureCollection\"}").
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void putFeatureWithEmptyGeometryObject() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"type\": \"Feature\", \"geometry\":{}}],\"type\":\"FeatureCollection\"}").
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void putFeatureWithInvalidGeometryObject() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"type\": \"Feature\", \"geometry\":1}],\"type\":\"FeatureCollection\"}").
        when().
        put("/spaces/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }
}
