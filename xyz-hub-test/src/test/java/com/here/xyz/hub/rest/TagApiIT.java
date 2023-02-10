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

import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TagApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    removeSpace(getSpaceId());
  }

  @Before
  public void setup() {
    createSpaceWithVersionsToKeep(getSpaceId(), 2, false);
  }

  @After
  public void teardown() {
    removeSpace(getSpaceId());
  }

  private ValidatableResponse _createReader(){
    return given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\"XYZ_1\"}")
        .post("/spaces/" + getSpaceId() + "/tags")
        .then();
  }

  @Test
  public void createTag() {
    _createReader()
        .statusCode(OK.code())
        .body("id", equalTo("XYZ_1"))
        .body("spaceId", equalTo(getSpaceId()))
        .body("version", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void deleteTag() {
    _createReader();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getTagVersion() {
    _createReader();

        given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code())
        .body("version", equalTo(0));
  }

  @Test
  public void updateTagVersion() {
    _createReader();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"version\":999}")
        .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code())
        .body("version", equalTo(999));
  }
}
