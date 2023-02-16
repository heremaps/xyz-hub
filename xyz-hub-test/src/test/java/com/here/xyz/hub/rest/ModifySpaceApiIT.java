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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ModifySpaceApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    createSpace();
  }

  @After
  public void tearDown() {
    removeSpace("x-psql-test");
  }

  @Test
  public void setSearchablePropertiesPositive() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/updateSpaceWithSearchableProperties.json"))
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("searchableProperties.name", equalTo(true))
        .body("searchableProperties.other", equalTo(false));
  }

  @Test
  public void setSearchablePropertiesNegative() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/updateSpaceWithSearchablePropertiesConnectorC1.json"))
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void removeAllListeners() {
    addListener("x-psql-test");

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"listeners\": null}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("listeners", nullValue());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("listeners", nullValue());
  }

  @Test
  public void removeAllProcessors() {
    addProcessor("x-psql-test");

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"processors\": null}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("processors", nullValue());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("processors", nullValue());
  }

  @Test
  public void addProcessorToExistingSpace() {
    addProcessor("x-psql-test");

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("processors", notNullValue())
        .body("processors.size()", is(1))
        .body("processors.rule-tagger", notNullValue())
        .body("processors.rule-tagger.size()", is(1))
        .body("processors.rule-tagger[0].eventTypes.size()", is(2));
  }

  @Test
  public void testConnectorResponseInModifiedSpace() {
    addProcessor("x-psql-test");

    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body("{\"description\": \"Added description\"}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("title", is("Test Space Processor"))
        .body("description", is("Added description"))
        .body("storage", notNullValue())
        .body("storage.id", is("psql"))
        .body("processors", notNullValue())
        .body("processors.size()", is(1))
        .body("processors.rule-tagger", notNullValue())
        .body("processors.rule-tagger.size()", is(1))
        .body("processors.rule-tagger[0].eventTypes.size()", is(2));
  }

  @Test
  public void testRemoveStorage() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER))
        .body("{\"storage\": null}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void patchWithoutChange() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"title\": \"My Demo Space\"}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void patchVersionsToKeepToZero() {
    final ValidatableResponse response = given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"versionsToKeep\": 0}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void patchExistingVersionsToKeepBiggerThanZeroToZero() {
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("\"versionsToKeep\": 0}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void patchExistingVersionsToKeepFromOneToTwo() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"versionsToKeep\": 2}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void patchExistingVersionsToKeepFromTenToOne() {
    cleanUpId = "x-psql-test-v2k-10";
    given()
        .accept(APPLICATION_JSON)
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\":\""+cleanUpId+"\",\"title\": \"v2k10\",\"versionsToKeep\": 10}")
        .when()
        .post("/spaces/")
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"versionsToKeep\": 2}")
        .when()
        .patch("/spaces/" + cleanUpId)
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"versionsToKeep\": 1}")
        .when()
        .patch("/spaces/" + cleanUpId)
        .then()
        .statusCode(BAD_REQUEST.code());
  }
}
