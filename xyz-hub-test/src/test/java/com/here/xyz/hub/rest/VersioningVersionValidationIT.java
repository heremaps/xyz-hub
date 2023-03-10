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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningVersionValidationIT extends TestSpaceWithFeature {
  @Before
  public void before() {
    removeSpace(getSpaceId());
    createSpace();
  }

  @After
  public void after() {
    removeSpace(getSpaceId());
  }

  @Test
  public void testGetBboxWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + getSpaceId() + "/bbox?west=1&south=-1&east=-1&north=1&version=0")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testGetTileWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + getSpaceId() + "/tile/quadkey/03333?version=0")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testGetSpatialWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + getSpaceId() + "/spatial?lat=0&lon=0&radius=10000&version=0")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testPostSpatialWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_GEO_JSON)
        .when()
        .body("{\"type\":\"Point\",\"coordinates\":[1,1,0]}")
        .post(getSpacesPath() + "/" + getSpaceId() + "/spatial?radius=1500000&version=0")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testGetSearchWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + getSpaceId() + "/search?version=0&p.key1=value1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testGetIterateWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + getSpaceId() + "/iterate?version=0&limit=1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }
}
