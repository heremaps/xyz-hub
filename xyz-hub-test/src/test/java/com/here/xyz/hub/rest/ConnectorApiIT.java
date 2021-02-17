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

import org.junit.*;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;

public class ConnectorApiIT extends RestAssuredTest {

  @After
  public void deleteConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .when()
        .delete("/connectors/test-connector")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("test-connector"));
  }

  @Before
  public void createConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnector.json"))
        .when()
        .post("/connectors")
        .then()
        .statusCode(CREATED.code())
        .body("id", equalTo("test-connector"));
  }

  @Test
  public void createConnectorWithoutId() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnectorWithoutId.json"))
        .when()
        .post("/connectors")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void replaceConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnectorReplace.json"))
        .when()
        .put("/connectors/test-connector")
        .then()
        .statusCode(OK.code())
        .body("connectionSettings.minConnections", equalTo(64))
        .body("connectionSettings.maxConnections", equalTo(256))
        .body("capabilities.propertySearch", equalTo(false));
  }

  @Test
  public void replaceConnectorWithMissMatchingUrl() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnector.json"))
        .when()
        .put("/connectors/wrongConnectorId")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void patchConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnectorPatch.json"))
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(OK.code())
        .body("connectionSettings.minConnections", equalTo(0))
        .body("connectionSettings.maxConnections", equalTo(128))
        .body("capabilities.propertySearch", equalTo(true))
        .body("capabilities.preserializedResponseSupport", equalTo(false));
  }

  @Test
  public void patchConnectorAdminParamsTrusted() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body("{\"trusted\":true}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void patchConnectorAdminParamsSkipAutoDisable() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body("{\"skipAutoDisable\":true}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void patchConnectorAdminParamsOwner() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS))
        .body("{\"owner\":\"newFakeOwner\"}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }
}