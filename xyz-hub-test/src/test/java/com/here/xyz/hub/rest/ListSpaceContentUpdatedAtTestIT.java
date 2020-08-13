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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ListSpaceContentUpdatedAtTestIT extends TestSpaceWithFeature {

  private static Set<String> cleanUpIds = new HashSet<>();

  private static long timestampMe;
  private static long timestampMeAnother;
  private static long timestampStar;
  private static long timestampStarAnother;

  @BeforeClass
  public static void setupClass() {
    remove();
    
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "other", false));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_1_ADMIN, "other", false));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "other", false));
  }

  @AfterClass
  public static void tearDown() {
    remove();
    cleanUpIds.forEach(TestSpaceWithFeature::removeSpace);
  }

  private static ValidatableResponse listSpaces(String contentUpdatedAtCondition, String owner) {
    return given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=" + owner + "&contentUpdatedAt" + contentUpdatedAtCondition)
      .then();
  }

  @Test
  public void testListSpaces() {
    final ValidatableResponse response1 = given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=me")
      .then();

      response1.statusCode(OK.code())
      .body("$.size()", equalTo(5));
    
      // sort contentUpdateAt and find a timestamp for other tests
      long [] ts1 = new long[5];
      for(int i = 0; i<5; i++ ){
        ts1[i] = response1.extract().path("[%s].contentUpdatedAt", String.valueOf(i));
      }
      Arrays.sort(ts1);

      timestampMe = ts1[3];
      timestampMeAnother = ts1[4];

    final ValidatableResponse response2 = given()
      .contentType(APPLICATION_JSON)
      .accept(APPLICATION_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .when()
      .get("/spaces?owner=*")
      .then();

      response2.statusCode(OK.code())
      .body("$.size()", equalTo(7));

      // sort contentUpdateAt and find a timestamp for other tests
      long [] ts = new long[7];
      for(int i = 0; i<7; i++ ){
        ts[i] = response2.extract().path("[%s].contentUpdatedAt", String.valueOf(i));
      }
      Arrays.sort(ts);

      timestampStar = ts[3];
      timestampStarAnother = ts[4];

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

  @Test
  public void testListSpacesEquals() {
    // ts not found
    listSpaces("=1", "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(0));

    // ts found
    listSpaces("=" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));

    // both ts found
    listSpaces("=" + timestampStar + "," + timestampStarAnother, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(2));
    
    // one ts found, the other one not
    listSpaces("=" + timestampStar + ",0", "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));

    // ts not found
    listSpaces("=0", "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(0));

    // ts found
    listSpaces("=" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));
  
    // both ts found
    listSpaces("=" + timestampMe + "," + timestampMeAnother, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(2));

    // one ts found, the other one not
    listSpaces("=" + timestampMe + ",0", "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));
  }

  @Test
  public void testListSpacesNotEquals() {
    listSpaces("!=1", "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(7));

    listSpaces("!=" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(6));

    listSpaces("!=0", "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(5));

    listSpaces("!=" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(4));
  }

  @Test
  public void testListSpacesGreater() {
    listSpaces(">" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));

    listSpaces(">" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(1));
  }

  @Test
  public void testListSpacesGreaterEquals() {
    listSpaces(">=" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(4));

    listSpaces(">=" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(2));
  }

  @Test
  public void testListSpacesLess() {
    listSpaces("<" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));

    listSpaces("<" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(3));
  }

  @Test
  public void testListSpacesLessEquals() {
    listSpaces("<=" + timestampStar, "*")
      .statusCode(OK.code())
      .body("$.size()", equalTo(4));

    listSpaces("<=" + timestampMe, "me")
      .statusCode(OK.code())
      .body("$.size()", equalTo(4));
  }
}
