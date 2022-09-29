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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.response.ValidatableResponse;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateSpaceWithExtensionApiIT extends TestSpaceWithFeature {

  private Set<String> cleanUpIds = new HashSet<>();

  @BeforeClass
  public static void setupClass() {
    removeAllSpaces();
    createSpaceWithCustomStorage("x-psql-test-extensible", "psql", null);
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
    removeSpace("x-psql-test");
    removeSpace("x-psql-test-extensible");
    removeSpace("x-psql-extends");
    removeSpace("x-psql-third-extends");
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
        .body("extends.spaceId", equalTo("x-psql-test-extensible"));
    cleanUpIds.add(response.extract().path("id"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-extending-test")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-extending-test"))
        .body("extends.spaceId", equalTo("x-psql-test-extensible"));
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
        .body("{\"id\": \"x-psql-second-extends\", \"title\": \"x-psql-second-extends\", \"extends\":{\"spaceId\":\"x-psql-extending-test\"}}")
        .when()
        .post("/spaces")
        .then();
    cleanUpIds.add(response.extract().path("id"));

    response.statusCode(OK.code())
        .body("extends.spaceId", equalTo("x-psql-extending-test"))
        .body("storage.params.extends.spaceId", equalTo("x-psql-extending-test"))
        .body("storage.params.extends.extends.spaceId", equalTo("x-psql-test-extensible"));
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
        .body("{\"id\": \"x-psql-second-extends\", \"title\": \"x-psql-second-extends\", \"extends\":{\"spaceId\":\"x-psql-extending-test\"}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());

    cleanUpIds.add(response.extract().path("id"));

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"x-psql-third-extends\", \"title\": \"x-psql-third-extends\", \"extends\":{\"spaceId\":\"x-psql-second-extends\"}}")
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
        .body("{\"id\": \"x-psql-extends\", \"title\": \"x-psql-extends\", \"extends\":{\"spaceId\":\"unexisting-space-id\"}}")
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
        .body("{\"id\": \"x-psql-extends\", \"title\": \"x-psql-extends\", \"extends\":{\"spaceId\":\"x-psql-test\"}, \"searchableProperties\":{\"name\": true}}")
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
        .body("{\"extends\": {\"spaceId\": \"x-psql-test\"}}")
        .when()
        .patch("/spaces/x-psql-extending-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test // should fail
  public void createSpaceWithExtensionAndStorage() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\": \"x-psql-extends\", \"title\": \"x-psql-extends\", \"extends\":{\"spaceId\":\"x-psql-test\"}, \"storage\":{\"id\": \"psql\", \"params\":{\"foo\": \"bar\"}}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("Validation failed. The properties 'storage' and 'extends' cannot be set together."));
  }

  @Test // should fail
  public void createSpaceWithExtensionNotSupported() {
    createSpaceWithCustomStorage("x-psql-no-extension", "inMemory", null);
    cleanUpIds.add("x-psql-no-extension");

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\": \"x-psql-extends\", \"title\": \"x-psql-extends\", \"extends\":{\"spaceId\":\"x-psql-no-extension\"}}}")
        .when()
        .post("/spaces")
        .then()
        .statusCode(BAD_REQUEST.code());
  }
}
