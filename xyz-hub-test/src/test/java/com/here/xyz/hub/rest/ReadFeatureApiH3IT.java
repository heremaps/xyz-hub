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

import static com.here.xyz.hub.auth.TestAuthenticator.TEST_SUFFIX;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The h3Index parameter of /spatial: a malformed value is rejected by the Hub, a value that is not a valid H3 cell by the storage.
 */
@Category(RestTests.class)
public class ReadFeatureApiH3IT extends TestSpaceWithFeature {
  private static final String SPACE_ID = "x-h3-api" + TEST_SUFFIX;
  //The message of the Hub's check, so the tests prove that the Hub rejects the value and not the storage
  private static final String HUB_MESSAGE = "Invalid argument! 'h3Index' must be an H3 cell index of 15 or 16 hexadecimal digits.";

  @BeforeClass
  public static void setup() throws Exception {
    removeSpace(SPACE_ID);
    createSpaceWithCustomStorage(SPACE_ID, "psql", null);
    postFeatures(SPACE_ID, new FeatureCollection().withFeatures(List.of(new Feature()
        .withId("in-cell")
        .withGeometry(new Point().withCoordinates(new PointCoordinates(8.5131, 50.2141)))
        .withProperties(new Properties()))), AuthProfile.ACCESS_ALL);
  }

  @AfterClass
  public static void tearDown() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void validH3Index() {
    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/spatial?h3Index=871faeba8ffffff")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1));
  }

  @Test
  public void malformedH3IndexIsABadRequest() {
    for (String h3Index : new String[]{"zz", "871faeba8fffff", "871faeba8ffffff12", "871faeba8fffffg"})
      getSpatial(h3Index)
          .statusCode(BAD_REQUEST.code())
          .body("errorMessage", equalTo(HUB_MESSAGE));
  }

  @Test
  public void invalidH3CellIsABadRequest() {
    //Formatted correctly, but not a valid H3 cell, so rejected by the storage
    for (String h3Index : new String[]{"871faeba8fffff1", "ffffffffffffffff"})
      getSpatial(h3Index)
          .statusCode(BAD_REQUEST.code())
          .body("errorMessage", equalTo("Invalid h3Index: not a valid H3 cell."));
  }

  @Test
  public void malformedH3IndexIsABadRequestOnPost() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"type\":\"Point\",\"coordinates\":[8.5131,50.2141]}")
        .when()
        .post(getSpacesPath() + "/" + SPACE_ID + "/spatial?h3Index=zz")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo(HUB_MESSAGE));
  }

  private static ValidatableResponse getSpatial(String h3Index) {
    return given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/spatial?h3Index=" + h3Index)
        .then();
  }
}
