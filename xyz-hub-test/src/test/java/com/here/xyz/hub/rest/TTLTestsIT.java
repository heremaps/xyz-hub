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


import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.restassured.RestAssured.given;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotSame;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;

import io.vertx.core.json.JsonObject;
import org.junit.BeforeClass;
import org.junit.Test;

public class TTLTestsIT extends TestWithSpaceCleanup {

  public static final String RANDOM_FEATURE_SPACE = "random_feature_test";

  public static void removeSpaces() {
    removeSpace(RANDOM_FEATURE_SPACE);
  }

  @BeforeClass
  public static void setupClass() {
    removeSpaces();
  }

  @Test
  public void A_createSpaceWithTTL() {
    JsonObject space = new JsonObject()
        .put("title", "Cache Test")
        .put("cacheTTL", 300);

    cleanUpId = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .body(space.encode())
        .when()
        .post("/spaces")
        .then()
        .statusCode(200)
        .body("cacheTTL", equalTo(300))
        .extract()
        .path("id");
  }

  /**
   * This test will fail if Redis is not running.
   * Please start a Redis instance or skip this test.
   */
  @Test
  public void testCacheWithGeoJson() {
    createRandomFeatureSpace();

    String id = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0")
        .then()
        .extract().body().path("features[0].id");

    String id_cached = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0")
        .then()
        .extract().body().path("features[0].id");

    assertEquals(id, id_cached);
  }

  /**
   * This test will fail if Redis is not running.
   * Please start a Redis instance or skip this test.
   */
  @Test
  public void testCacheWithMvt() {
    createRandomFeatureSpace();

    byte[] body = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0.mvt")
        .body().asByteArray();

    byte[] body_cached = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0.mvt")
        .body()
        .asByteArray();

    assertArrayEquals(body, body_cached);
  }

  @Test
  public void testCacheSkipping() {
    createRandomFeatureSpace();

    String id = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0")
        .then()
        .extract().body().path("features[0].id");

    String id_skip1 = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0?skipCache=true")
        .then()
        .extract().body().path("features[0].id");

    assertNotSame(id, id_skip1);

    String id_skip2 = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .when()
        .get("/spaces/" + RANDOM_FEATURE_SPACE + "/tile/web/0_0_0?skipCache=true")
        .then()
        .extract().body().path("features[0].id");

    assertNotSame(id_skip1, id_skip2);
  }

  private void createRandomFeatureSpace() {
    cleanUpId = RANDOM_FEATURE_SPACE;

    JsonObject space = new JsonObject()
        .put("id", RANDOM_FEATURE_SPACE)
        .put("storage", new JsonObject().put("id", "test"))
        .put("title", "Cache Test")
        .put("cacheTTL", 3000);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .body(space.encode())
        .when()
        .post("/spaces")
        .then()
        .statusCode(200);
  }
}
