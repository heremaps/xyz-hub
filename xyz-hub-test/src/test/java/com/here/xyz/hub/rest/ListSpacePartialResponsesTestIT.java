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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.response.ValidatableResponse;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ListSpacePartialResponsesTestIT extends TestSpaceWithFeature {

  private static Set<String> cleanUpIds = new HashSet<>();
  
  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "other", false));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "other", false));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "other", false));
  }

  @After
  public void tearDown() {
    remove();
    cleanUpIds.forEach(TestSpaceWithFeature::removeSpace);
  }

  private static ValidatableResponse listSpaces(int handle, int limit, String owner) {
    return given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=" + owner + "&handle=" + handle + "&limit=" + limit)
      .then();
  }

  @Test
  public void testListSpaces() {
    given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=me")
      .then()
      .statusCode(OK.code())
      .body("$.size()", equalTo(5));
    

    given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=*")
      .then()
      .statusCode(OK.code())
      .body("$.size()", equalTo(7));


    given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
      .when()
      .get("/spaces?owner=me")
      .then()
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));
    

    given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
      .when()
      .get("/spaces?owner=*")
      .then()
      .statusCode(OK.code())
      .body("$.size()", equalTo(6));
  }

  // @Test
  public void testListSpacesHandle0Limit10() {
    listSpaces(0, 10, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(5));

    listSpaces(0, 10, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(7));
  }

  @Test
  public void testListSpacesHandle1Limit10() {
    listSpaces(1, 10, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(4));

    listSpaces(1, 10, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(6));
  }

  @Test
  public void testListSpacesHandle0Limit3() {
    listSpaces(0, 3, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));

    listSpaces(0, 3, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));
  }


  @Test
  public void testListSpacesHandle3Limit3() {
    listSpaces(3, 3, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(2));

    listSpaces(3, 3, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));
  }

  @Test
  public void testListSpacesHandle6Limit3() {
    listSpaces(6, 3, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(0));
    

    listSpaces(6, 3, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));
  }
}
