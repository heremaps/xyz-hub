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
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

public class ModifyCompositeSpaceIT extends TestCompositeSpace {

  @Test
  public void updateMutableSpaceProperties() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"title\":\"x-psql-test-ext-new-title\",\"description\":\"a test space which extends x-psql-test\"}")
        .when()
        .patch("/spaces/x-psql-test-ext")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test-ext"))
        .body("extends.spaceId", equalTo("x-psql-test"))
        .body("title", equalTo("x-psql-test-ext-new-title"))
        .body("description", equalTo("a test space which extends x-psql-test"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test-ext"))
        .body("title", equalTo("x-psql-test-ext-new-title"))
        .body("extends.spaceId", equalTo("x-psql-test"));
  }

  @Test
  public void updateSpacePropertiesSamePayload() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"id\":\"x-psql-test-ext\",\"title\":\"x-psql-test-ext\",\"extends\":{\"spaceId\":\"x-psql-test\"}}")
        .when()
        .patch("/spaces/x-psql-test-ext")
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
        .patch("/spaces/x-psql-test-ext")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"storage\":{\"id\":\"psql\",\"params\":{\"foo\":\"bar\"}}}")
        .when()
        .patch("/spaces/x-psql-test-ext")
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
        .patch("/spaces/x-psql-test-ext-ext")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", equalTo("The space x-psql-test-ext-ext cannot extend the space non-existing-space because it does not exist."));

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test\"}}")
        .when()
        .patch("/spaces/x-psql-test-ext-ext")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test-ext-ext"))
        .body("extends.spaceId", equalTo("x-psql-test"));
  }

  @Test
  public void updateExtendsSelfExtending() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test-ext-ext\"}}")
        .when()
        .patch("/spaces/x-psql-test-ext-ext")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsMoreThan2Levels() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test-ext-ext\"}}")
        .when()
        .patch("/spaces/x-psql-test-2")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteExtendedSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/x-psql-test")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void deleteDanglingCompositeSpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/x-psql-test")
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/x-psql-test-ext")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void updateExtendsSelfReference() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test\"}}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsCyclicReference() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test-ext\"}}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateExtendsCyclicReference2Levels() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test-ext-ext\"}}")
        .when()
        .patch("/spaces/x-psql-test")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deactivateCompositeSpacesOnParentDelete() throws InterruptedException {
    removeSpace("x-psql-test");

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext")
        .then()
        .statusCode(OK.code())
        .body("active", equalTo(false));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/iterate")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());

    //Takes some time till parent deletion got propagated. If we are to fast - the db-query will hit the delted table.
    Thread.sleep(100);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext-ext/iterate")
        .then()
        .statusCode(PRECONDITION_REQUIRED.code());
  }

  @Test
  public void testExtendedSpaceStorageMismatchError() {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"extends\":{\"spaceId\":\"x-psql-test-3\"}}")
        .when()
        .patch("/spaces/x-psql-test-ext")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("code", equalTo("E318408"))
        .body("title", equalTo("Extended Space storage mismatch"));
  }
}
