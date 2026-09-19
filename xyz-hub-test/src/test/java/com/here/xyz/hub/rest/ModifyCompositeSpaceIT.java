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
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_REQUIRED;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.TimeUnit;
import org.awaitility.Durations;
import org.junit.Test;

public class ModifyCompositeSpaceIT extends TestCompositeSpace {

  @Test
  public void updateMutableSpaceProperties() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"title\":\"x-psql-test-ext-new-title\",\"description\":\"a test space which extends " + DEFAULT_SPACE_ID + "\"}")
        .when()
        .patch("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(EXTENSION_SPACE_ID))
        .body("extends.spaceId", equalTo(DEFAULT_SPACE_ID))
        .body("title", equalTo("x-psql-test-ext-new-title"))
        .body("description", equalTo("a test space which extends " + DEFAULT_SPACE_ID));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(EXTENSION_SPACE_ID))
        .body("title", equalTo("x-psql-test-ext-new-title"))
        .body("extends.spaceId", equalTo(DEFAULT_SPACE_ID));
  }

  @Test
  public void updateSpacePropertiesSamePayload() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\":\"" + EXTENSION_SPACE_ID + "\",\"title\":\"" + EXTENSION_SPACE_ID + "\",\"extends\":{\"spaceId\":\"" + DEFAULT_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void updateImmutableSpaceProperties() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"searchableProperties\":{\"property1\":true}}")
        .when()
        .patch("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"storage\":{\"id\":\"psql\",\"params\":{\"foo\":\"bar\"}}}")
        .when()
        .patch("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("Validation failed. The properties 'storage' and 'extends' cannot be set together."));
  }

  @Test
  public void updateExtendsProperty() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"non-existing-space\"}}")
        .when()
        .patch("/spaces/" + EXTENSION_EXTENSION_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("The space " + EXTENSION_EXTENSION_SPACE_ID + " cannot extend the space non-existing-space because it does not exist."));

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + DEFAULT_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + EXTENSION_EXTENSION_SPACE_ID)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(EXTENSION_EXTENSION_SPACE_ID))
        .body("extends.spaceId", equalTo(DEFAULT_SPACE_ID));
  }

  @Test
  public void updateExtendsSelfExtending() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + EXTENSION_EXTENSION_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + EXTENSION_EXTENSION_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsMoreThan2Levels() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + EXTENSION_EXTENSION_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + SECOND_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteExtendedSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/" + DEFAULT_SPACE_ID)
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void deleteDanglingCompositeSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/" + DEFAULT_SPACE_ID)
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void updateExtendsSelfReference() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + DEFAULT_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + DEFAULT_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsCyclicReference() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + EXTENSION_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + DEFAULT_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsCyclicReference2Levels() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + EXTENSION_EXTENSION_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + DEFAULT_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deactivateCompositeSpacesOnParentDelete() throws InterruptedException {
    removeSpace(DEFAULT_SPACE_ID);

    //The deactivation of the extending spaces is propagated asynchronously
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
        .untilAsserted(() -> given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .when()
            .get("/spaces/" + EXTENSION_SPACE_ID)
            .then()
            .statusCode(OK.code())
            .body("active", equalTo(false)));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + EXTENSION_SPACE_ID + "/iterate")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    //Takes some time till parent deletion got propagated. If we are to fast - the db-query will hit the delted table.
    Thread.sleep(100);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/" + EXTENSION_EXTENSION_SPACE_ID + "/iterate")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void testExtendedSpaceStorageMismatchError() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"" + THIRD_SPACE_ID + "\"}}")
        .when()
        .patch("/spaces/" + EXTENSION_SPACE_ID)
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("code", equalTo("E318408"))
        .body("title", equalTo("Extended Space storage mismatch"));
  }
}
