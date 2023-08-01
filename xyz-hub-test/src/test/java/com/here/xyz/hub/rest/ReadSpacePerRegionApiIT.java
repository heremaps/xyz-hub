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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import com.here.xyz.hub.connectors.models.Space;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReadSpacePerRegionApiIT extends TestSpaceWithFeature {
  static List<String> cleanupSpaceIds = new ArrayList<>();
  static final String SPACE_ID_NO_REGION = "test-s-no-region";
  static final String SPACE_ID_REGION_A_1 = "test-s-region-a-1";
  static final String SPACE_ID_REGION_A_2 = "test-s-region-a-2";
  static final String SPACE_ID_REGION_B = "test-s-region-b";

  @BeforeClass
  public static void setupClass() {
    cleanupSpaceIds.add(SPACE_ID_NO_REGION);
    cleanupSpaceIds.add(SPACE_ID_REGION_A_1);
    cleanupSpaceIds.add(SPACE_ID_REGION_A_2);
    cleanupSpaceIds.add(SPACE_ID_REGION_B);
    cleanupSpaceIds.forEach(TestWithSpaceCleanup::removeSpace);
  }

  @Before
  public void setup() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new Space().withTitle("test space no region").withId(SPACE_ID_NO_REGION))
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new Space().withRegion("a").withTitle("test space region a #1").withId(SPACE_ID_REGION_A_1))
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new Space().withRegion("a").withTitle("test space region a #2").withId(SPACE_ID_REGION_A_2))
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new Space().withRegion("b").withTitle("test space region b").withId(SPACE_ID_REGION_B))
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());
  }

  @After
  public void tearDown() {
    cleanupSpaceIds.forEach(TestWithSpaceCleanup::removeSpace);
  }

  @Test
  public void readSpacesWithRegion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?region=a")
        .then()
        .statusCode(OK.code()).extract().body().asString();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?region=a")
        .then()
        .statusCode(OK.code())
        .body("$.size()", equalTo(2))
        .body("id", hasItems(SPACE_ID_REGION_A_1, SPACE_ID_REGION_A_2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?region=b")
        .then()
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("id", hasItems(SPACE_ID_REGION_B));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?region=x")
        .then()
        .statusCode(OK.code())
        .body("$.size()", equalTo(0));
  }

  @Test
  public void readSpacesWithContentUpdatedAtAndRegion() {
    long contentUpdatedAtSpaceA2 = given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/" + SPACE_ID_REGION_A_2)
        .then()
        .statusCode(OK.code())
        .extract()
        .body()
        .path("contentUpdatedAt");

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?region=a&contentUpdatedAt=gt=" + --contentUpdatedAtSpaceA2)
        .then()
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("id", hasItems(SPACE_ID_REGION_A_2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?contentUpdatedAt=gt="+ Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli() +"&region=b")
        .then()
        .statusCode(OK.code())
        .body("$.size()", equalTo(1))
        .body("id", hasItems(SPACE_ID_REGION_B));
  }
}
