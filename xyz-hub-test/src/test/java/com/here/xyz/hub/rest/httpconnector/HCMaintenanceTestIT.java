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

package com.here.xyz.hub.rest.httpconnector;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.auth.TestAuthenticator;
import com.here.xyz.hub.rest.RestAssuredConfig;
import java.util.HashMap;
import org.junit.BeforeClass;
import org.junit.Test;

public class HCMaintenanceTestIT {
    private static String host = RestAssuredConfig.config().fullHttpConnectorUri;;
    private static String defaultConnector;
    private static HashMap<String, String> authHeaders;
    private static final String testSpace = "x-psql-test";

    @BeforeClass
    public static void setupClass() throws Exception {
        authHeaders = new HashMap<>();
        authHeaders.put("Authorization", "Bearer " + TestAuthenticator.AuthProfile.ACCESS_ALL.jwt_string);
        defaultConnector = "psql";

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/connectors/"+defaultConnector+"/initialization")
                .then()
                .statusCode(OK.code());
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
    public void testPSQLStatusWithExistingConnector() {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/connectors/"+defaultConnector+"/status")
                .then()
                .statusCode(OK.code())
                .body("initialized", equalTo(true))
                .body("extensions.size()", greaterThan(1))
                .body("extensions", hasItem("postgis"))
                .body("scriptVersions.h3", greaterThan(1))
                .body("scriptVersions.ext", greaterThan(1));
    }

    @Test
    public void testPSQLStatusWithNotExistingConnector() throws JsonProcessingException {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/connectors/NA/status")
                .then()
                .statusCode(BAD_REQUEST.code())
                .body("errorMessage", notNullValue());
    }

    @Test
    public void maintainExistingConnector() {
        long curTime = System.currentTimeMillis();

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/connectors/"+defaultConnector+"/maintain/indices")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/connectors/"+defaultConnector+"/status")
                .then()
                .statusCode(OK.code())
                .body("maintenanceStatus.AUTO_INDEXING.maintainedAt", greaterThan(curTime))
                .body("maintenanceStatus.AUTO_INDEXING.maintenanceRunning.size()", equalTo(0));
    }

    @Test
    public void maintainNotExistingConnector() {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/connectors/NA/maintain/indices")
                .then()
                .statusCode(BAD_REQUEST.code())
                .body("errorMessage", equalTo("Connector [NA] cant get found/loaded!"));
    }

    @Test
    public void initializeNewConnector() {

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/connectors/c1/initialization")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/connectors/"+defaultConnector+"/status")
            .prettyPeek()
                .then()
                .statusCode(OK.code())
                .body("initialized", equalTo(true))
                .body("extensions", hasItem("postgis"))
                .body("extensions.size()", greaterThan(1))
                .body("scriptVersions.h3", greaterThan(1))
                .body("scriptVersions.ext", greaterThan(1));
    }

    @Test
    public void maintainSpace() {
        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .headers(authHeaders)
                .when()
                .delete(RestAssuredConfig.config().fullHubUri +"/spaces/"+testSpace);

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/maintain/spaces/"+testSpace)
                .then()
                .statusCode(NOT_FOUND.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .headers(authHeaders)
                .when()
                .body("{\"id\": \""+testSpace+"\",\"title\": \"test\",\"enableGlobalVersioning\" : true,\"searchableProperties\" : {\"foo\" :true}}")
                .post(RestAssuredConfig.config().fullHubUri +"/spaces")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/spaces/"+testSpace)
                .then()
                .statusCode(CONFLICT.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .post(host+"/maintain/spaces/"+testSpace+"?force=true")
                .then()
                .statusCode(OK.code());

        given()
                .contentType(APPLICATION_JSON)
                .accept(APPLICATION_JSON)
                .when()
                .get(host+"/maintain/spaces/"+testSpace)
                .then()
                .statusCode(OK.code())
                .body("idxCreationFinished", equalTo(true))
                .body("idxAvailable.size()", equalTo(13))
                .body("idxManual.searchableProperties.foo", equalTo(true))
                .body("idxManual.sortableProperties", nullValue());
    }
}
