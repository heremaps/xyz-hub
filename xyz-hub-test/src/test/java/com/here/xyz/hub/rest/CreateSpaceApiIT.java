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
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Test;

public class CreateSpaceApiIT extends TestSpaceWithFeature {

  private String cleanUpId;

  @After
  public void tearDown() {
      if (cleanUpId != null) {
        removeSpace(cleanUpId);
      }
  }

  @Test
  public void basicCreateSpace() {
    cleanUpId = "x-psql-test";
    createSpace();
  }


  @Test
  public void createSpaceWithTheSameId() {
    cleanUpId = "x-psql-test";
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpace.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test"))
        .body("title", equalTo("My Demo Space"))
        .body("storage.id", equalTo("psql"));

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpace.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(CONFLICT.code());
  }

  @Test
  public void createSpaceWithoutId() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithoutId.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("My Demo Space"))
        .body("storage.id", equalTo("psql"))
        .body("createdAt", greaterThan(0L))
        .body("updatedAt", greaterThan(0L))
        .body("tags", nullValue());
  }

  @Test
  public void createSpaceWithSearchablePropertiesPositive() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSpaceWithSearchableProperties.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response
        .statusCode(OK.code())
        .body("searchableProperties.name", equalTo(true))
        .body("searchableProperties.other", equalTo(false));
  }

  @Test
  public void createSpaceWithSearchablePropertiesNegative() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSpaceWithSearchablePropertiesConnectorC1.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createSpaceWithInvalidJson() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithInvalidJson.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(400);
  }

  @Test
  public void createSpaceWithCopyright() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithCopyright.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("My Demo Space"))
        .body("copyright.size()", is(2))
        .body("copyright.label", hasItems("Copyright Label", "Copyright Label 2"))
        .body("copyright.alt", hasItems("Description", "Description 2"));
  }

  @Test
  public void createSpaceWithListener() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_LISTENER))
        .body(content("/xyz/hub/createSpaceWithListener.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("Test Space Listener"))
        .body("listeners", notNullValue())
        .body("listeners.size()", is(1))
        .body("listeners.listener-test", notNullValue())
        .body("listeners.listener-test.size()", is(1))
        .body("listeners.listener-test[0].eventTypes.size()", is(1));
  }

  @Test
  public void createSpaceWithProcessor() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body(content("/xyz/hub/createSpaceWithProcessor.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("Test Space Processor"))
        .body("processors", notNullValue())
        .body("processors.size()", is(1))
        .body("processors.rule-tagger", notNullValue())
        .body("processors.rule-tagger.size()", is(1))
        .body("processors.rule-tagger[0].eventTypes.size()", is(2));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + cleanUpId)
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void createSpaceWithPsql() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_PSQL))
        .body(content("/xyz/hub/createSpacePsql.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("My Demo Space"));
  }

  @Test
  public void createSpaceWithListenerUnauthorized() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/createSpaceWithListener.json"))
        .when()
        .post("/spaces")
        .then();

    response.statusCode(FORBIDDEN.code());
  }

  @Test
  public void createSpaceWithoutAdmin() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"description\":\"test\"}")
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("test"))
        .body("description", equalTo("test"));
  }

  @Test
  public void createSpaceWithNotAllowedStorage() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body(content("/xyz/hub/createSpaceWithNotAllowedStorage.json"))
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(FORBIDDEN.code());
  }
  @Test
  public void createSpaceWithClient() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"description\":\"test\", \"client\":{\"a\":1}}")
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code())
        .body("id", notNullValue())
        .body("title", equalTo("test"))
        .body("description", equalTo("test"))
        .body("client.a", equalTo(1));
  }

  @Test
  public void createSpaceWithTooLargeClientObject() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body(
            "{\"title\":\"test\", \"description\":\"test\", \"client\":{\"a\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}}")
        .when()
        .post("/spaces")
        .then();

    response
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("The property client is over the allowed limit of 1024 bytes."));
  }

  @Test
  public void createSpaceWithInvalidListeners() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"description\":\"test\", \"listeners\":{\"a\":{}}}}")
        .when()
        .post("/spaces")
        .then();

    cleanUpId = response.extract().path("id");

    response.statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createSpaceWithVersionsToKeepNegativeNumber() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":-1}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createSpaceWithVersionsToKeepAsString() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":\"abc\"}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createSpaceWithVersionsToKeepPositive() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":10}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code());
  }

  @Test
  public void createSpaceWithVersionsToKeepZero() {
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":0}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createSpaceWithVersionsToKeepTooBig() {
    ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":1000000002}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(BAD_REQUEST.code());

    response = given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .body("{\"title\":\"test\", \"versionsToKeep\":1000000001}")
        .when()
        .post("/spaces")
        .then();
    cleanUpId = response.extract().path("id");

    response.statusCode(OK.code());
  }
}
