/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.rest.httpconnector;

import com.here.xyz.hub.PsqlHttpVerticle;
import com.here.xyz.hub.auth.TestAuthenticator;
import com.here.mapcreator.ext.naksha.NakshaManageDb;
import com.here.xyz.psql.SQLQueryExt;
import com.here.xyz.psql.config.PSQLConfig;
import io.vertx.core.json.JsonObject;
import org.junit.*;

import java.util.HashMap;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.RestAssuredConfig.config;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

public class HCMaintenanceTestIT {

  private static String ecps;
  private final static String connectorUri = config().fullHttpConnectorUri;
  private static String defaultConnector;
  private static NakshaManageDb mc;
  private static HashMap<String, String> authHeaders;

  private static final String testSpace = "x-psql-test";

  @BeforeClass
  public static void setupClass() throws Exception {
    authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer " + TestAuthenticator.AuthProfile.ACCESS_ALL.jwt_string);

    defaultConnector = "psql";
    retrieveConfig();
    mc = initMaintenanceClient();
    deleteTestResources();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/initialization?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(OK.code());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteTestResources();
  }

  public static NakshaManageDb initMaintenanceClient() {
    PsqlHttpVerticle.DB_INITIAL_POOL_SIZE = 1;
    PsqlHttpVerticle.DB_MIN_POOL_SIZE = 1;
    PsqlHttpVerticle.DB_MAX_POOL_SIZE = 1;

    PsqlHttpVerticle.DB_ACQUIRE_RETRY_ATTEMPTS = 1;
    PsqlHttpVerticle.DB_ACQUIRE_INCREMENT = 1;

    PsqlHttpVerticle.DB_CHECKOUT_TIMEOUT = 10;
    PsqlHttpVerticle.DB_TEST_CONNECTION_ON_CHECKOUT = true;

    return new NakshaManageDb();
  }

  public static void deleteTestResources() throws Exception {
    final String ecpsPhrase = config().ecpsPhrase;
    final String ecpsJson = config().ecpsJson;
    final String ecpsEncrypted = PSQLConfig.encryptECPS(ecpsJson, ecpsPhrase);
    NakshaManageDb.MaintenanceInstance dbInstance = mc.getClient(
        defaultConnector, ecpsEncrypted, ecpsPhrase);
    SQLQueryExt query = new SQLQueryExt("DELETE from xyz_config.db_status where connector_id='TestConnector'");

    //delete connector entry
    mc.executeQueryWithoutResults(query, dbInstance.getSource());

    //delete space
    given()
        .contentType(APPLICATION_JSON)
        .headers(authHeaders)
        .accept(APPLICATION_JSON)
        .when()
        .delete(config().fullHubUri + "/spaces/" + testSpace);
  }

  public static void retrieveConfig() {
    final String response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(authHeaders)
        .when()
        .get(config().fullHubUri + "/connectors/psql-http")
        .getBody().asString();

    JsonObject connector = new JsonObject(response);
    JsonObject params = connector.getJsonObject("params");
    ecps = params.getString("ecps");
  }

  @Test
  public void testHealthCheck() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/health")
        .then()
        .statusCode(OK.code())
        .body("status.result", equalTo("OK"))
        .body("status.timestamp", notNullValue())
        .body("reporter.name", equalTo("HERE HTTP-Connector"))
        .body("reporter.version", notNullValue())
        .body("reporter.upSince", notNullValue())
        .body("reporter.buildDate", notNullValue());
  }

  @Test
  public void testPSQLStatusWithExistingConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/status?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(OK.code())
        .body("initialized", equalTo(true))
        .body("extensions.size", greaterThan(1))
        .body("extensions", hasItem("postgis"))
        .body("scriptVersions.h3", greaterThan(1))
        .body("scriptVersions.ext", greaterThan(1));
  }

  @Test
  public void testPSQLStatusWithWrongECPSConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/status?connectorId=" + defaultConnector + "p&ecps=NA")
        .then()
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", notNullValue());
  }

  @Test
  public void testPSQLStatusWithNotExistingConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/status?connectorId=NA&ecps=" + ecps)
        .then()
        .statusCode(NOT_FOUND.code())
        .body("errorMessage", notNullValue());
  }

  @Test
  public void maintainExistingConnector() {
    long curTime = System.currentTimeMillis();

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/maintain/indices?connectorId=" + defaultConnector + "&ecps=" + ecps + "&autoIndexing=true")
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/status?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(OK.code())
        .body("maintenanceStatus.AUTO_INDEXING.maintainedAt", greaterThan(curTime))
        .body("maintenanceStatus.AUTO_INDEXING.maintenanceRunning.size", equalTo(0));
  }

  @Test
  public void maintainNotExistingConnector() {
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/maintain/indices?connectorId=NA&ecps=" + ecps + "&autoIndexing=true")
        .then()
        .statusCode(METHOD_NOT_ALLOWED.code())
        .body("errorMessage", equalTo("Database not initialized!"));
  }

  @Test
  public void initializeNewConnector() {

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/initialization?connectorId=TestConnector&ecps=" + ecps)
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/status?connectorId=TestConnector&ecps=" + ecps)
        .then()
        .statusCode(OK.code())
        .body("initialized", equalTo(true))
        .body("extensions.size", greaterThan(1))
        .body("extensions", hasItem("postgis"))
        .body("scriptVersions.h3", greaterThan(1))
        .body("scriptVersions.ext", greaterThan(1));
  }

  @Test
  public void maintainSpace() {

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/maintain/space/" + testSpace + "?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(authHeaders)
        .when()
        .body("{\"id\": \"" + testSpace + "\",\"title\": \"test\",\"enableHistory\" : true,\"searchableProperties\" : {\"foo\" :true}}")
        .post(config().fullHubUri + "/spaces")
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/maintain/space/" + testSpace + "?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(CONFLICT.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .post(connectorUri + "/maintain/space/" + testSpace + "?connectorId=" + defaultConnector + "&ecps=" + ecps + "&force=true")
        .then()
        .statusCode(OK.code());

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .when()
        .get(connectorUri + "/maintain/space/" + testSpace + "?connectorId=" + defaultConnector + "&ecps=" + ecps)
        .then()
        .statusCode(OK.code())
        .body("idxCreationFinished", equalTo(true))
        .body("idxAvailable.size", equalTo(8))
        .body("idxManual.searchableProperties.foo", equalTo(true))
        .body("idxManual.sortableProperties", nullValue());
  }
}
