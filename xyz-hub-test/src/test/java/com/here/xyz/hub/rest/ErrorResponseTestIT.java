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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import io.vertx.core.json.Json;
import org.junit.After;
import org.junit.Test;

public class ErrorResponseTestIT extends TestSpaceWithFeature {

  private String cleanUpId;

  @After
  public void tearDown() {
    if (cleanUpId != null) {
      removeSpace(cleanUpId);
    }
  }

  @Test
  public void nonExistingConnector() {
    String spaceId = "illegal_argument";
    Space space = new Space()
        .withStorage(new ConnectorRef().withId("xyz-non-existent-test"))
        .withId(spaceId)
        .withTitle(spaceId);
    cleanUpId = spaceId;

    removeSpace(cleanUpId);

    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(Json.encode(space)).
        when().post("/spaces").then().
        statusCode(NOT_FOUND.code());
  }

  @Test
  public void errorOnCreateSpace() {
    String spaceId = "unknown_id_will_not_be_created";
    Space space = new Space()
        .withStorage(new ConnectorRef().withId("test"))
        .withId(spaceId)
        .withTitle(spaceId);
    cleanUpId = spaceId;

    removeSpace(cleanUpId);

    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(Json.encode(space)).
        when().post("/spaces").then().
        statusCode(BAD_GATEWAY.code());
  }

  @Test
  public void ILLEGAL_ARGUMENT() {
    String spaceId = "IllegalArgument";
    Space space = new Space()
        .withStorage(new ConnectorRef().withId("test"))
        .withId(spaceId)
        .withTitle(spaceId);
    cleanUpId = spaceId;

    removeSpace(cleanUpId);

    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(Json.encode(space)).
        when().post("/spaces").then().
        statusCode(OK.code());

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        when().get("/spaces/" + spaceId + "/features?id=1").then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void BAD_GATEWAY() {
    String spaceId = "BadGateway";
    Space space = new Space()
        .withStorage(new ConnectorRef().withId("test"))
        .withId(spaceId)
        .withTitle(spaceId);
    cleanUpId = spaceId;

    removeSpace(cleanUpId);

    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(Json.encode(space)).
        when().post("/spaces").then().
        statusCode(OK.code());

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        when().get("/spaces/" + spaceId + "/features?id=1").then().
        statusCode(BAD_GATEWAY.code());
  }

  @Test
  public void EXCEPTION() {
    String spaceId = "Exception";
    Space space = new Space()
        .withStorage(new ConnectorRef().withId("test"))
        .withId(spaceId)
        .withTitle(spaceId);
    cleanUpId = spaceId;

    removeSpace(cleanUpId);

    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(Json.encode(space)).
        when().post("/spaces").then().
        statusCode(OK.code());

    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        when().get("/spaces/" + spaceId + "/features?id=1").then().
        statusCode(BAD_GATEWAY.code());
  }

  @Test
  public void testFailedEntries() {
    cleanUpId = "x-failing";
    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/createSpaceWithFailingPreProcessor.json")).
        when().post("/spaces").then().
        statusCode(OK.code());

    Feature point = Feature.createEmptyFeature().
        withId("F001").
        withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    given().
        contentType(APPLICATION_GEO_JSON).
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(point.serialize()).
        when().post("/spaces/x-failing/features").then().
        statusCode(OK.code()).
        body("failed.size()", equalTo(2));

    removeSpace(cleanUpId);
  }

  @Test
  public void testErrorAndFailed() {
    cleanUpId = "x-failing";
    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/createSpaceWithFailingAndErrorPreProcessor.json")).
        when().post("/spaces").then().
        statusCode(OK.code());

    Feature point = Feature.createEmptyFeature().
        withId("F001").
        withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    given().
        contentType(APPLICATION_GEO_JSON).
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(point.serialize()).
        when().post("/spaces/x-failing/features").then().
        statusCode(BAD_REQUEST.code()).
        body("type", equalTo("ErrorResponse")).
        body("errorMessage", equalTo("Test Error"));

    removeSpace(cleanUpId);
  }

  @Test
  public void testsExceptionPreProcessor() {
    cleanUpId = "x-failing";
    given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/createSpaceWithExceptionPreProcessor.json")).
        when().post("/spaces").then().
        statusCode(OK.code());

    Feature point = Feature.createEmptyFeature().
        withId("F001").
        withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    given().
        contentType(APPLICATION_GEO_JSON).
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(point.serialize()).
        when().post("/spaces/x-failing/features").then().
        statusCode(BAD_GATEWAY.code());

    removeSpace(cleanUpId);
  }
}
