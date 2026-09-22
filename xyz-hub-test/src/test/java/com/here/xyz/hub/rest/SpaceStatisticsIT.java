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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import io.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class SpaceStatisticsIT extends TestSpaceWithFeature {

  @AfterClass
  public static void tearDownClass() {
    removeAllSpaces();
  }

  @Before
  public void setup() {
    removeAllSpaces();
    createSpaceWithCustomStorage(EXTENSIBLE_SPACE_ID, "psql", null);
  }

  @After
  public void tearDown() {
    removeAllSpaces();
  }

  public static void removeAllSpaces() {
    removeSpace(EXTENSIBLE_SPACE_ID);
    removeSpace(EXTENDING_SPACE_ID);
  }

  @Test
  public void spaceStatistics() {
    ValidatableResponse statisticsResponse = getStatistics(EXTENSIBLE_SPACE_ID, null);
    statisticsResponse
            .body("contentUpdatedAt.value", greaterThan(0L))
            .body("contentUpdatedAt.estimated", equalTo(true));
  }

  @Test
  public void spaceWithExtensionStatistics() {
    createExtension();

    ValidatableResponse statisticsResponse = getStatistics(EXTENDING_SPACE_ID, null);
    statisticsResponse
            .body("contentUpdatedAt.value", greaterThan(0L))
            .body("contentUpdatedAt.estimated", equalTo(true));

    long extensionUpdatedAt = statisticsResponse.extract().body().path("contentUpdatedAt.value");

    statisticsResponse = getStatistics(EXTENDING_SPACE_ID, "SUPER");
    statisticsResponse
            .body("contentUpdatedAt.value", lessThan(extensionUpdatedAt))
            .body("contentUpdatedAt.estimated", equalTo(true));

    statisticsResponse = getStatistics(EXTENDING_SPACE_ID, "EXTENSION");
    statisticsResponse
            .body("contentUpdatedAt.value", equalTo(extensionUpdatedAt))
            .body("contentUpdatedAt.estimated", equalTo(true));

    statisticsResponse = getStatistics(EXTENDING_SPACE_ID, "DEFAULT");
    statisticsResponse
            .body("contentUpdatedAt.value", equalTo(extensionUpdatedAt))
            .body("contentUpdatedAt.estimated", equalTo(true));
  }

  public ValidatableResponse getStatistics(String spaceId, String context) {
    context = (context == null || context.isEmpty()) ? "" : "?context="+context;
    return given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/" + spaceId + "/statistics" + context).
            then().
            statusCode(OK.code());
  }

  public void createExtension() {
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(content("/xyz/hub/createSpaceWithExtension.json"))
            .when()
            .post("/spaces")
            .then()
            .statusCode(OK.code())
            .body("extends.spaceId", equalTo(EXTENSIBLE_SPACE_ID));

    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .get("/spaces/" + EXTENDING_SPACE_ID)
            .then()
            .statusCode(OK.code())
            .body("id", equalTo(EXTENDING_SPACE_ID))
            .body("extends.spaceId", equalTo(EXTENSIBLE_SPACE_ID));
  }
}
