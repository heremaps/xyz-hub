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

package com.here.xyz.hub.auth;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

import com.here.xyz.hub.rest.TestSpaceWithFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FeatureApiAuthIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setup() {
    remove();
    createSpace();
    addFeatures();
  }

  @AfterClass
  public static void tearDownClass() {
    remove();
  }

  @Test
  public void testFeatureByIdWithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        when().
        get(getSpacesPath() + "/x-psql-test/features/Q2838923").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void readByBoundingBoxSmallWithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        when().
        get(getSpacesPath() + "/x-psql-test/bbox?north=23.13&west=113.32&south=23.14&east=113.33").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void testSearchSpaceRequestWithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        when().
        get(getSpacesPath() + "/x-psql-test/search?limit=100").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void testIterateSpaceWithoutHandleRequestWithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        when().
        get(getSpacesPath() + "/x-psql-test/iterate?limit=500").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void testReadingFeatureByTileIdWithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        when().
        get(getSpacesPath() + "/x-psql-test/tile/quadkey/2100300120310022.geojson").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void updateFeatureById_put_WithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        put(getSpacesPath() + "/x-psql-test/features/Q2838923?addTags=baseball&removeTags=soccer").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void updateFeatureById_post_WithOtherOwner() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2)).
        body(content("/xyz/hub/updateFeatureById.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball&removeTags=soccer").
        then().
        statusCode(FORBIDDEN.code());
  }

  @Test
  public void deleteFeatureByIdWithOtherOwner() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .delete(getSpacesPath() + "/x-psql-test/features/Q4201688")
        .then()
        .statusCode(FORBIDDEN.code());
  }
}
