/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReaderApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    removeSpace(getSpaceId());
  }

  @Before
  public void setup() {
    createSpace();
    addFeatures();
  }

  @After
  public void teardown() {
    removeSpace(getSpaceId());
  }

  @Test
  public void createReader() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .put("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("XYZ_1"))
        .body("spaceId", equalTo(getSpaceId()))
        .body("version", equalTo(1));
  }

  @Test
  public void deleteReader() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .put("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("XYZ_1"))
        .body("spaceId", equalTo(getSpaceId()))
        .body("version", equalTo(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void getReaderVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/readers/XYZ_1/version")
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .put("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/readers/XYZ_1/version")
        .then()
        .statusCode(OK.code())
        .body("version", equalTo(1));
  }

  @Test
  public void increaseReaderVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .put("/spaces/" + getSpaceId() + "/readers/XYZ_1")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"version\": 999}")
        .post("/spaces/" + getSpaceId() + "/readers/XYZ_1/version")
        .then()
        .statusCode(OK.code())
        .body("version", equalTo(999));
  }
}
