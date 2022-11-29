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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.PsqlHttpVerticle;
import com.here.xyz.hub.auth.TestAuthenticator;
import com.here.xyz.hub.config.MaintenanceClient;
import com.here.xyz.hub.rest.RestAssuredConfig;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.PSQLConfig;
import io.vertx.core.json.JsonObject;
import org.junit.*;

import java.util.HashMap;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;

public class HCMaintenanceTestIT {
    private static String ecps;
    private static String host;
    private static String defaultConnector;
    private static MaintenanceClient mc;
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
                .post(host+"/initialization?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(OK.code());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        deleteTestResources();
    }

    public static MaintenanceClient initMaintenanceClient() throws Exception {
        PsqlHttpVerticle.DB_INITIAL_POOL_SIZE = 1;
        PsqlHttpVerticle.DB_MIN_POOL_SIZE = 1;
        PsqlHttpVerticle.DB_MAX_POOL_SIZE = 1;

        PsqlHttpVerticle.DB_ACQUIRE_RETRY_ATTEMPTS = 1;
        PsqlHttpVerticle.DB_ACQUIRE_INCREMENT = 1;

        PsqlHttpVerticle.DB_CHECKOUT_TIMEOUT = 10;
        PsqlHttpVerticle.DB_TEST_CONNECTION_ON_CHECKOUT = true;

        return new MaintenanceClient();
    }

    public static void deleteTestResources() throws Exception {
        String psqlHost = System.getenv().containsKey("PSQL_HOST") ? System.getenv("PSQL_HOST") : "localhost";
        String localhostECPS = PSQLConfig.encryptECPS("{\"PSQL_HOST\":\""+psqlHost+"\"}", "local");
        MaintenanceClient.MaintenanceInstance dbInstance = mc.getClient(defaultConnector, localhostECPS, "local");
        SQLQuery query = new SQLQuery("DELETE from xyz_config.db_status where connector_id='TestConnector'");

        //delete connector entry
        mc.executeQueryWithoutResults(query, dbInstance.getSource());

        //delete space
        given()
                .contentType(APPLICATION_JSON)
                .headers(authHeaders)
                .accept(APPLICATION_JSON)
                .when()
                .delete(RestAssuredConfig.config().fullHubUri+"/spaces/"+testSpace);
    }

    public static void retrieveConfig() {
        final String response = given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .headers(authHeaders)
                .when()
                .get(RestAssuredConfig.config().fullHubUri+"/connectors/psql-http")
                .getBody().asString();

        JsonObject connector = new JsonObject(response);
        JsonObject params = connector.getJsonObject("params");

        ecps = params.getString("ecps");
        host = RestAssuredConfig.config().fullHttpConnectorUri;
    }

    @Test
    public void testHealthCheck() {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/health")
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
    public void testPSQLStatusWithExistingConnector() throws JsonProcessingException {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/status?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(OK.code())
                .body("initialized", equalTo(true))
                .body("extensions.size", greaterThan(1))
                .body("extensions", hasItem("postgis"))
                .body("scriptVersions.h3", greaterThan(1))
                .body("scriptVersions.ext", greaterThan(1));
    }

    @Test
    public void testPSQLStatusWithWrongECPSConnector() throws JsonProcessingException {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/status?connectorId="+defaultConnector+"p&ecps=NA")
                .then()
                .statusCode(BAD_REQUEST.code())
                .body("errorMessage", notNullValue());
    }

    @Test
    public void testPSQLStatusWithNotExistingConnector() throws JsonProcessingException {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/status?connectorId=NA&ecps="+ecps)
                .then()
                .statusCode(NOT_FOUND.code())
                .body("errorMessage", notNullValue());
    }

    @Test
    public void maintainExistingConnector() throws JsonProcessingException {
        long curTime = System.currentTimeMillis();

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/indices?connectorId="+defaultConnector+"&ecps="+ecps+"&autoIndexing=true")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/status?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(OK.code())
                .body("maintenanceStatus.AUTO_INDEXING.maintainedAt", greaterThan(curTime))
                .body("maintenanceStatus.AUTO_INDEXING.maintenanceRunning.size", equalTo(0));
    }

    @Test
    public void maintainNotExistingConnector() throws JsonProcessingException {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/indices?connectorId=NA&ecps="+ecps+"&autoIndexing=true")
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
                .post(host+"/initialization?connectorId=TestConnector&ecps="+ecps)
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/status?connectorId=TestConnector&ecps="+ecps)
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
                .get(host+"/maintain/space/"+testSpace+"?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(NOT_FOUND.code());

       given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .headers(authHeaders)
                .when()
                .body("{\"id\": \""+testSpace+"\",\"title\": \"test\",\"enableHistory\" : true,\"searchableProperties\" : {\"foo\" :true}}")
                .post(RestAssuredConfig.config().fullHubUri +"/spaces")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/space/"+testSpace+"?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(CONFLICT.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/space/"+testSpace+"?connectorId="+defaultConnector+"&ecps="+ecps+"&force=true")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/maintain/space/"+testSpace+"?connectorId="+defaultConnector+"&ecps="+ecps)
                .then()
                .statusCode(OK.code())
                .body("idxCreationFinished", equalTo(true))
                .body("idxAvailable.size", equalTo(13))
                .body("idxManual.searchableProperties.foo", equalTo(true))
                .body("idxManual.sortableProperties", nullValue());
    }
}
