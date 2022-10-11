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
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestCompositeSpace extends TestSpaceWithFeature {

  @Before
  public void setup() {
    removeSpace("x-psql-test-ext-ext");
    removeSpace("x-psql-test-ext");
    removeSpace("x-psql-test");
    createSpace();
    createSpaceWithExtension("x-psql-test");
    createSpaceWithExtension("x-psql-test-ext");
  }

  @After
  public void tearDown() {
    removeSpace("x-psql-test-ext-ext");
    removeSpace("x-psql-test-ext");
    removeSpace("x-psql-test");
  }

  private static void createSpaceWithExtension(String extendingSpaceId) {
    String extensionId = extendingSpaceId + "-ext";
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\":\""+extensionId+ "\",\"title\":\"x-psql-test-extension\",\"extends\":{\"spaceId\":\""+extendingSpaceId+"\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(extensionId))
        .body("extends.spaceId", equalTo(extendingSpaceId));
  }
}
