/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RestTests.class)
public class DeleteFeatureApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    createSpace();
    addFeatures();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void deleteFeatureById() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features/Q4201688")
        .then()
        .statusCode(NO_CONTENT.code());

    countFeatures(251);
  }

  private void deleteMultipleFeaturesByIdWithResponse(String acceptType) {
    //We try as well to delete the feature with the ID Q336088 twice (CMEKB-889)
    given()
        .accept(acceptType)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?id=Q1362236&id=Q4201688&id=Q336088&id=Q336088")
        .then()
        .statusCode(OK.code())
        .body("deleted", hasItems("Q1362236", "Q4201688", "Q336088"));

    countFeatures(249);
  }

  @Test
  public void deleteMultipleFeaturesByIdWithResponse() {
    deleteMultipleFeaturesByIdWithResponse(APPLICATION_JSON);
  }

  @Test
  public void deleteMultipleFeaturesByIdWithGeoJsonResponse() {
    deleteMultipleFeaturesByIdWithResponse(APPLICATION_GEO_JSON);
  }

  @Test
  public void deleteMultipleFeaturesById() {
    //We try as well to delete the feature with the ID Q336088 twice. (CMEKB-889)
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?id=Q1362236&id=Q4201688&id=Q336088&id=Q336088")
        .then()
        .statusCode(NO_CONTENT.code());

    countFeatures(249);
  }

  @Test
  public void deleteFeatureByIdNonExisting() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features/Q12345678")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  private void deleteFeatureByTagsWithResponse(String acceptType) {
    given()
        .accept(acceptType)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=soccer")
        .then()
        .statusCode(OK.code());
    //TODO: check the body once CMEKB-2627 is done

    countFeatures(83);
  }

  @Test
  public void deleteFeatureByTagsWithResponse() {
    deleteFeatureByTagsWithResponse(APPLICATION_JSON);
  }

  @Test
  public void deleteFeatureByTagsWithGeoJsonResponse() {
    deleteFeatureByTagsWithResponse(APPLICATION_GEO_JSON);
  }

  @Test
  public void deleteFeatureByTags() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=soccer")
        .then()
        .statusCode(NO_CONTENT.code());

    countFeatures(83);
  }

  private void deleteFeaturesByTagsNonExistingWithResponse(String acceptType) {
    given()
        .accept(acceptType)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=soccer")
        .then()
        .statusCode(OK.code())
        .body("$", not(hasKey("deleted")));
  }

  @Test
  public void deleteFeaturesByTagsNonExistingWithResponse() {
    deleteFeaturesByTagsNonExistingWithResponse(APPLICATION_JSON);
  }

  @Test
  public void deleteFeaturesByTagsNonExistingWithGeoJsonResponse() {
    deleteFeaturesByTagsNonExistingWithResponse(APPLICATION_GEO_JSON);
  }

  @Test
  public void deleteFeatureByTagsNonExisting() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=soccer")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  private void deleteAllWithResponse(String acceptType) {
    given()
        .accept(acceptType)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=*")
        .prettyPeek()
        .then()
        .statusCode(OK.code());
    //TODO: check the body once CMEKB-2627 is done

    countFeatures(0);
  }

  @Test
  public void deleteAllWithResponse() {
    deleteAllWithResponse(APPLICATION_JSON);
  }

  @Test
  public void deleteAllWithGeoJsonResponse() {
    deleteAllWithResponse(APPLICATION_GEO_JSON);
  }

  @Test
  public void deleteAll() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features?tags=*")
        .then()
        .statusCode(NO_CONTENT.code());

    countFeatures(0);
  }

  @Test
  public void deleteFeatureNonExistingSpace() {
    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/x-psql-test-non-existing/features?tags=*")
        .then()
        .statusCode(NOT_FOUND.code());
  }
}
