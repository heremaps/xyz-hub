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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_REQUIRED;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class SpaceInactiveIT extends TestSpaceWithFeature {

  private final List<String> spaceIds = new ArrayList<>();

  private String SPACE_ID;

  private String createInactiveSpace() {
    String spaceId = createSpaceWithRandomId();
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("""
            {
              "versionsToKeep": 10,
              "active": false
            }
            """)
        .when()
        .patch(getSpacesPath() + "/" + spaceId)
        .then()
        .statusCode(OK.code());

    return spaceId;
  }

  @Before
  public void before() {
    SPACE_ID = createInactiveSpace();

    spaceIds.clear();
    spaceIds.add(SPACE_ID);
  }

  @After
  public void after() {
    spaceIds.forEach(TestWithSpaceCleanup::removeSpace);
    spaceIds.clear();
  }

  @Test
  public void createSpaceWithActiveFalse() {
    String spaceId = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("""
            {
              "title": "test",
              "active": false
            }
            """)
        .when()
        .post(getSpacesPath())
        .then()
        .statusCode(OK.code())
        .body("active", equalTo(false))
        .extract()
        .path("id");
    spaceIds.add(spaceId);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + spaceId)
        .then()
        .statusCode(OK.code())
        .body("$", hasKey("active"))
        .body("active", equalTo(false));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + spaceId)
        .then()
        .statusCode(OK.code())
        .body("$", not(hasKey("active")));
  }

  //TODO: split the following test into multiple tests
  @Test
  public void readFeaturesInactiveSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/features?id=1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/features/1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/statistics")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/bbox?west=1&north=1&east=1&south=1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/tile/quadkey/0")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/spatial?lat=0&lon=0&radius=1000")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
              "type": "Point",
              "coordinates": [0,0]
            }
            """)
        .post(getSpacesPath() + "/" + SPACE_ID + "/spatial")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/search?f.id=1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/iterate")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void putFeaturesInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
              "type": "Feature",
              "id": "F1",
              "properties": {
                "prop1": "val1"
              }
            }
            """)
        .put(getSpacesPath() + "/" + SPACE_ID + "/features")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void postFeaturesInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
              "type": "Feature",
              "id": "F1",
              "properties": {
                "prop1": "val1"
              }
            }
            """)
        .post(getSpacesPath() + "/" + SPACE_ID + "/features")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void deleteFeaturesOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/features?id=1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void putFeatureOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
              "type": "Feature",
              "id": "F1",
              "properties": {
                "prop1": "val1"
              }
            }
            """)
        .put(getSpacesPath() + "/" + SPACE_ID + "/features/F1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void patchFeatureOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
              "type": "Feature",
              "properties": {
                "prop1": "val1"
              }
            }
            """)
        .patch(getSpacesPath() + "/" + SPACE_ID + "/features/F1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void deleteFeatureFromInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/features/F1")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void getSubscriptionOfInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/subscriptions/S1")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void listSubscriptionsOfInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/subscriptions")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void postSubscriptionOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
               "id": "my-subscription-id",
               "destination": "string",
               "config": {
                 "type": "PER_FEATURE"
               }
            }
            """)
        .post(getSpacesPath() + "/" + SPACE_ID + "/subscriptions")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void putSubscriptionOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .body("""
            {
               "id": "my-subscription-id",
               "destination": "string",
               "config": {
                 "type": "PER_FEATURE"
               }
            }
            """)
        .put(getSpacesPath() + "/" + SPACE_ID + "/subscriptions/S1")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void deleteNonExistingSubscriptionFromInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/subscriptions/S1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void readChangesetsInactiveSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/changesets?startVersion=0&endVersion=10")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void getChangesetOfInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/changesets/5")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void deleteChangesetsFromInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/changesets?version<5")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void getTagOfInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID + "/tags/T1")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void postTagOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .body("""
            {
                "id": "T1"
            }
            """)
        .post(getSpacesPath() + "/" + SPACE_ID + "/tags")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }

  @Test
  public void patchTagsOnInactiveSpaceNegative() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .body("""
            {
                "version": 100
            }
            """)
        .patch(getSpacesPath() + "/" + SPACE_ID + "/tags/T1")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code())
        .extract().asPrettyString();
  }

  @Test
  public void deleteTagFromInactiveSpaceNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/tags/T1")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code());
  }
}
