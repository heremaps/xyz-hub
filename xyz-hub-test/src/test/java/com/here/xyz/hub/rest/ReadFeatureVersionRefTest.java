/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class ReadFeatureVersionRefTest extends TestSpaceWithFeature {

  /*
  TODO:

  - Allow creating a branch on HEAD (should be even the default) <- resolve HEAD during its creation
  - Check why a POST on an existing branch (without a change) responds with 204
  - Allow to create tags on branches
  - Do not allow to create a tag with the same name of an existing branch and vice versa
   */

  private void createBranch() {
    given()
        .contentType(APPLICATION_JSON)
        .body("""
            {
              "id": "branch1",
              "baseRef": "2"
            }
            """)
        .when()
        .post(getSpacesPath()+ "/" + getSpaceId() + "/branches")
        .then()
        .statusCode(OK.code());
  }

  private void createTag() {
    given()
        .contentType(APPLICATION_JSON)
        .body("""
            {"id": "tag1"}
            """)
        .when()
        .post(getSpacesPath() + "/" + getSpaceId() + "/tags")
        .then()
        .statusCode(OK.code());
  }

  @BeforeEach
  public void setup() {
    removeSpace(getSpaceId());
    createSpaceWithVersionsToKeep(getSpaceId(), 2);
    addFeature(getSpaceId(), new Feature().withId("F1").withProperties(new Properties().with("a", 1)).withGeometry(new Point().withCoordinates(new PointCoordinates(1, 1))));
    createTag();
    addFeature(getSpaceId(), new Feature().withId("F1").withProperties(new Properties().with("b", 2)).withGeometry(new Point().withCoordinates(new PointCoordinates(2, 2))));
    createBranch();
  }

  @AfterEach
  public void tearDown() {
    removeSpace(getSpaceId());
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD1", "head", "notExistingTag"})
  public void getFeatureByIdInRefNegative(String ref) {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/features/F1?versionRef=" + ref)
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void getFeaturesByIdsInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/features?id=F1&versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void getFeatureByIdInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/features/F1?versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("properties.a", equalTo(1));

    if (!"tag1".equals(ref))
      response.body("properties.b", equalTo(2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void getFeaturesByBboxInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/bbox?west=-10&north=10&east=10&south=-10&versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void getFeaturesByTileInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/tile/quadkey/1?versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void searchFeaturesSpatiallyInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/spatial?lat=0&lon=0&radius=1000000&versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void searchFeaturesSpatiallyInRefWithPostedGeometry(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"type\":\"Point\",\"coordinates\":[0,0]}")
        .post(getSpacesPath() + "/" + getSpaceId() + "/spatial?radius=1000000&versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }


  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void searchFeaturesInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/search?versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEAD", "branch1", "tag1"})
  public void iterateFeaturesInRef(String ref) {
    ValidatableResponse response = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get(getSpacesPath() + "/" + getSpaceId() + "/iterate?versionRef=" + ref)
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    checkBProperty(ref, response);
  }

  private static void checkBProperty(String ref, ValidatableResponse response) {
    if (!"tag1".equals(ref))
      response.body("features.properties.b", hasItem(2));
  }

}
