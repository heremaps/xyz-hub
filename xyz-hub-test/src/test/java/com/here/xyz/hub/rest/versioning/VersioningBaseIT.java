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

package com.here.xyz.hub.rest.versioning;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.hub.rest.TestSpaceWithFeature;

public class VersioningBaseIT extends TestSpaceWithFeature {

  protected static void createSpace(String spaceId, String spacePath, long versionsToKeep) {
    createSpace(spaceId, spacePath, versionsToKeep, false);
  }

  protected static void createSpace(String spaceId, String spacePath, long versionsToKeep, boolean enableUUID) {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\""+spaceId+"\",\"title\":\""+spaceId+"\",\"versionsToKeep\":"+versionsToKeep+",\"enableUUID\":"+enableUUID+"}")
        .when()
        .post(spacePath)
        .then()
        .statusCode(OK.code());
  }

  protected static void setup() {
    String spaceId = "x-psql-test";
    removeSpace(spaceId);
    createSpace(spaceId, getCreateSpacePath(), 10);
    addFeatures(spaceId);
  }

  protected static void tearDown() {
    removeSpace("x-psql-test");
  }

  protected static void countExpected(int expected) {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/x-psql-test/iterate")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(expected));
  }
}
