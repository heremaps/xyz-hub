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

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.*;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;

public class ConnectorApiIT extends RestAssuredTest {

  @BeforeClass
  public static void setup() {
    removeAll();
  }

  @After
  public void teardown() {
    removeAll();
  }

  private static void removeAll() {
    //Delete all connectors which have potentially been created during the test
    removeConnector(AuthProfile.ACCESS_ALL, "test-connector");
    removeConnector(AuthProfile.ACCESS_ALL, "test-connector2");
    removeConnector(AuthProfile.ACCESS_ALL, "xyz-connector");
  }

  private static ValidatableResponse removeConnector(AuthProfile profile, String connectorId) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .when()
        .delete("/connectors/" + connectorId)
        .then();
  }

  private void addTestConnector() {
    addConnector(AuthProfile.ACCESS_ALL,"/xyz/hub/connectors/embeddedConnector.json");
  }

  private ValidatableResponse addConnector(AuthProfile profile, String contentFile) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .body(content(contentFile))
        .when()
        .post("/connectors")
        .then();
  }

  @Test
  public void createConnector() {
    addConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS, "/xyz/hub/connectors/embeddedConnector.json")
        .statusCode(CREATED.code())
        .body("id", equalTo("test-connector"));
  }

  @Test
  public void createConnectorWithAnyIdPositive() {
    addConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS_WITH_PREFIX_ID,
        "/xyz/hub/connectors/embeddedConnectorWithOtherId.json")
        .statusCode(CREATED.code())
        .body("id", equalTo("xyz-connector"));
  }

  @Test
  public void createConnectorWithAnyIdNegative() {
    addConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS_WITH_PREFIX_ID, "/xyz/hub/connectors/embeddedConnector.json")
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createConnectorWithoutId() {
    addConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS, "/xyz/hub/connectors/embeddedConnectorWithoutId.json")
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createConnectorForDifferentUserAsAdmin() {
    addConnector(AuthProfile.ACCESS_ALL, "/xyz/hub/connectors/embeddedConnectorWithOwner2.json")
        .statusCode(CREATED.code());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .when()
        .get("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2_WITH_MANAGE_CONNECTORS))
        .when()
        .get("/connectors/test-connector")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void patchConnectorWithMisMatchingUrl() {
    addTestConnector();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .body(content("/xyz/hub/connectors/embeddedConnector.json"))
        .when()
        .patch("/connectors/wrongConnectorId")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteConnector() {
    addTestConnector();

    String connectorId = "test-connector";
    removeConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS, connectorId)
        .statusCode(OK.code())
        .body("id", equalTo(connectorId));
  }

  @Test
  public void patchConnector() {
    addTestConnector();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
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
  public void patchConnectorAdminParams() {
    addTestConnector();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .body("{\"skipAutoDisable\":true}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .body("{\"owner\":\"newFakeOwner\"}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .body("{\"trusted\":true}")
        .when()
        .patch("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void getConnectorFromOtherOwner() {
    addTestConnector();

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2_WITH_MANAGE_CONNECTORS))
        .when()
        .get("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void getConnectorWithRestrictedRights() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID))
        .when()
        .get("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createConnectorWithInsufficientRights() {
    addConnector(AuthProfile.ACCESS_OWNER_1_MANAGE_ALL_SPACES_ONLY, "/xyz/hub/connectors/embeddedConnector.json")
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createConnectorWithInsufficientRights2() {
    addConnector(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID, "/xyz/hub/connectors/embeddedConnector.json")
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void getConnectorWithInsufficientRights() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID))
        .when()
        .get("/connectors/test-connector")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void createConnectorWithSufficientAndMorePreciseRights() {
    addTestConnector();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID))
        .body(content("/xyz/hub/connectors/embeddedConnector2.json"))
        .when()
        .post("/connectors")
        .then()
        .statusCode(CREATED.code())
        .body("id", equalTo("test-connector2"));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID))
        .when()
        .delete("/connectors/test-connector2")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("test-connector2"));
  }

  @Test
  public void getGlobalConnectorWithInsufficientRights() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_PSQL))
        .when()
        .get("/connectors/psql")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void getGlobalConnectorWithInsufficientRightsQuery() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_PSQL))
        .when()
        .get("/connectors/?id=psql")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void updateGlobalConnectorWithInsufficientRights() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_PSQL))
        .body(content("/xyz/hub/connectors/embeddedConnectorWithoutId.json"))
        .when()
        .patch("/connectors/psql")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void getUnknownConnectorByQuery() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .when()
        .get("/connectors/?id=unknown")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getUnknownConnector() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS))
        .when()
        .get("/connectors/unknown")
        .then()
        .statusCode(NOT_FOUND.code());
  }
}