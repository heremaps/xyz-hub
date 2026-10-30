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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.restassured.response.ValidatableResponse;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateExtensionSpaceIT extends TestSpaceWithFeature {

  private static final String EXTENDS_SPACE_ID = "x-psql-extends" + TEST_SUFFIX;
  private static final String SECOND_EXTENDS_SPACE_ID = "x-psql-second-extends" + TEST_SUFFIX;
  private static final String THIRD_EXTENDS_SPACE_ID = "x-psql-third-extends" + TEST_SUFFIX;
  private static final String NO_EXTENSION_SPACE_ID = "x-psql-no-extension" + TEST_SUFFIX;

  private Set<String> cleanUpIds = new HashSet<>();

  @BeforeClass
  public static void setupClass() {
    removeAllSpaces();
    createSpaceWithCustomStorage(EXTENSIBLE_SPACE_ID, "psql", null);
    createSpace();
  }

  @AfterClass
  public static void tearDownClass() {
    removeAllSpaces();
  }

  @Before
  public void setup() { cleanUpIds.clear(); }

  @After
  public void tearDown() {
    for (String cleanUpId : cleanUpIds) {
      removeSpace(cleanUpId);
    }
  }

  public static void removeAllSpaces() {
    removeSpace(DEFAULT_SPACE_ID);
    removeSpace(EXTENSIBLE_SPACE_ID);
    removeSpace(EXTENDS_SPACE_ID);
    removeSpace(THIRD_EXTENDS_SPACE_ID);
  }

  @Test // should pass
  public void createSpaceWithExtension() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithExtension.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .body("extends.spaceId", equalTo(EXTENSIBLE_SPACE_ID));
    cleanUpIds.add(response.extract().path("id"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + EXTENDING_SPACE_ID)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(EXTENDING_SPACE_ID))
        .body("extends.spaceId", equalTo(EXTENSIBLE_SPACE_ID));
  }

  @Test // should pass
  public void createSpaceWith2LevelsExtension() {
    ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithExtension.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());

    cleanUpIds.add(response.extract().path("id"));

    response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + SECOND_EXTENDS_SPACE_ID + "\", \"title\": \"" + SECOND_EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + EXTENDING_SPACE_ID + "\"}}")
        .when()
        .post("/spaces")
        .then();
    cleanUpIds.add(response.extract().path("id"));

    response.statusCode(OK.code())
        .body("extends.spaceId", equalTo(EXTENDING_SPACE_ID));
  }

  @Test // should fail
  public void createSpaceWith3LevelsExtension() {
    ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithExtension.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());

    cleanUpIds.add(response.extract().path("id"));

    response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + SECOND_EXTENDS_SPACE_ID + "\", \"title\": \"" + SECOND_EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + EXTENDING_SPACE_ID + "\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());

    cleanUpIds.add(response.extract().path("id"));

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + THIRD_EXTENDS_SPACE_ID + "\", \"title\": \"" + THIRD_EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + SECOND_EXTENDS_SPACE_ID + "\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test // should fail
  public void createSpaceWithNonExistingExtension() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + EXTENDS_SPACE_ID + "\", \"title\": \"" + EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"unexisting-space-id\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  //@Test // should fail
  public void createSpaceNotAuthorizedExtension() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .body(content("/xyz/hub/createSpaceWithExtension.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test // should fail
  public void createSpaceWithExtensionAndSearchableProperties() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + EXTENDS_SPACE_ID + "\", \"title\": \"" + EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + DEFAULT_SPACE_ID + "\"}, \"searchableProperties\":{\"name\": true}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test // should fail
  public void modifySpaceExtension() {
    ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithExtension.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());

    cleanUpIds.add(response.extract().path("id"));

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\": {\"spaceId\": \"" + DEFAULT_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + EXTENDING_SPACE_ID)
        .then()
        .statusCode(OK.code());
  }

  @Test // should fail
  public void createSpaceWithExtensionAndStorage() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\": \"" + EXTENDS_SPACE_ID + "\", \"title\": \"" + EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + DEFAULT_SPACE_ID + "\"}, \"storage\":{\"id\": \"psql\", \"params\":{\"foo\": \"bar\"}}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("Validation failed. The properties 'storage' and 'extends' cannot be set together."));
  }

  @Test // should fail
  public void createSpaceWithExtensionNotSupported() {
    createSpaceWithCustomStorage(NO_EXTENSION_SPACE_ID, "inMemory", null);
    cleanUpIds.add(NO_EXTENSION_SPACE_ID);

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + EXTENDS_SPACE_ID + "\", \"title\": \"" + EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + NO_EXTENSION_SPACE_ID + "\"}}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test // should fail
  public void createSpaceSelfExtending() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"" + EXTENDS_SPACE_ID + "\", \"title\": \"" + EXTENDS_SPACE_ID + "\", \"extends\":{\"spaceId\":\"" + EXTENDS_SPACE_ID + "\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }
}
