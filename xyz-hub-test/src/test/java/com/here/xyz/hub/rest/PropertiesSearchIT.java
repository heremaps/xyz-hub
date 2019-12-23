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
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.response.ValidatableResponse;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Durations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

;

public class PropertiesSearchIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setup() {
    remove();
    createSpace();
    addFeatures();
  }

  @AfterClass
  public static void tearDown() {
    remove();
  }

  @Test
  public void testGreaterThan() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.capacity>50000").
        then().
        body("features.size()", equalTo(133));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.capacity=gt=50000").
        then().
        body("features.size()", equalTo(133));
  }

  @Test
  public void testLessThan() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.capacity>50000").
        then().
        body("features.size()", equalTo(133));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.capacity=gt=50000").
        then().
        body("features.size()", equalTo(133));
  }

  @Test
  public void testEquals() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.capacity=50000").
        then().
        body("features.size()", equalTo(17));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?p.name=Arizona Stadium").
        then().
        body("features.size()", equalTo(1)).
        body("features[0].properties.name", equalTo("Arizona Stadium"));
  }

  @Test
  public void errorTest() {
    final ValidatableResponse response = given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/createErrorTestSpace.json")).
        when().post("/spaces").then();

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/illegal_argument/search?p.capacity=gt=50000").prettyPeek().
        then().
        statusCode(400);

    removeSpace("illegal_argument");
  }

  @Test
  public void testEqualsWithSystemProperty() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.id=Q1370732").
        then().
        body("features.size()", equalTo(1));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.id='Q1370732'").
        then().
        body("features.size()", equalTo(1));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.id=33333333").
        then().
        body("features.size()", equalTo(0));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.createdAt<=" + System.currentTimeMillis()).
        then().
        body("features.size()", equalTo(252));
  }

  /**
   * Test is commented out because it takes too long to execute, since the indexes should be created by the connector for the test be valid.
   * Only kept here for future reference
   * @throws Exception
   */
  //@Test
  public void testCreatedAtAndUpdatedAtWith10ThousandFeaturesPlus()  throws Exception {
    add10ThousandFeatures();

    await()
        .atMost(1, TimeUnit.MINUTES)
        .pollInterval(Durations.ONE_SECOND)
        .until(() ->
          "PARTIAL".equals(given().
              accept(APPLICATION_JSON).
              headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
              when().
              get("/spaces/x-psql-test/statistics").prettyPeek().
              then().extract().body().path("properties.searchable")
        ));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.createdAt>0?limit=1").
        then().
        body("features.size()", equalTo(1));

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test/search?f.updatedAt>0?limit=1").
        then().
        body("features.size()", equalTo(1));
  }
}
