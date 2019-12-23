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
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.NOT_FOUND;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

import com.jayway.restassured.http.Method;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpecComplianceApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    createSpace();
    addFeatures();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void testUnauthorized() {
    if ("JWT".equals(System.getProperty("xyz.hub.auth"))) {
      checkUnauthorized(Method.GET, APPLICATION_JSON, "/spaces");
      checkUnauthorized(Method.GET, APPLICATION_JSON, "/spaces/x-psql-test");
      checkUnauthorized(Method.POST, APPLICATION_JSON, "/spaces");
      checkUnauthorized(Method.PATCH, APPLICATION_JSON, "/spaces/x-psql-test");
      checkUnauthorized(Method.DELETE, APPLICATION_JSON, "/spaces/x-psql-test");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features?id=Q3495887");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features/Q3495887");
      checkUnauthorized(Method.GET, APPLICATION_JSON, "/spaces/x-psql-test/statistics");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/bbox");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/tile/quadkey/x-tile-id");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/search");
      checkUnauthorized(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/iterate");
      checkUnauthorized(Method.PUT, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features");
      checkUnauthorized(Method.POST, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features");
      checkUnauthorized(Method.DELETE, APPLICATION_JSON, "/spaces/x-psql-test/features");
      checkUnauthorized(Method.PUT, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features/Q3495887");
      checkUnauthorized(Method.PATCH, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features/Q3495887");
      checkUnauthorized(Method.DELETE, APPLICATION_JSON, "/spaces/x-psql-test/features/Q3495887");
    }
  }

  @Test
  public void testNotFound() {
    checkNotFound(Method.GET, APPLICATION_JSON, "/non-existing-path");
    checkNotFound(Method.GET, APPLICATION_JSON, "/spaces/non-existing-path");
    checkNotFound(Method.PATCH, APPLICATION_JSON, "/spaces/non-existing-path");
    checkNotFound(Method.DELETE, APPLICATION_JSON, "/spaces/non-existing-path");
    checkNotFound(Method.GET, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features/non-existing-path");
    checkNotFound(Method.GET, APPLICATION_JSON, "/spaces/non-existing-path/statistics");
    checkNotFound(Method.GET, APPLICATION_GEO_JSON, "/spaces/non-existing-path/bbox?east=1&west=1&north=1&south=1");
    checkNotFound(Method.GET, APPLICATION_GEO_JSON, "/spaces/non-existing-path/search");
    checkNotFound(Method.GET, APPLICATION_GEO_JSON, "/spaces/non-existing-path/iterate");
    checkNotFound(Method.DELETE, APPLICATION_JSON, "/spaces/non-existing-path/features?id=non-existing-feature");
    checkNotFound(Method.PATCH, APPLICATION_GEO_JSON, "/spaces/x-psql-test/features/non-existing-path");
    checkNotFound(Method.DELETE, APPLICATION_JSON, "/spaces/x-psql-test/features/non-existing-path");
  }

  private void checkUnauthorized(final Method method, final String accept, final String path) {
    RequestSpecification reqSpec = given().
        accept(accept).
        contentType(accept).
        when();

    Response res;

    switch(method) {
      case GET: res = reqSpec.get(path); break;
      case POST: res = reqSpec.contentType(accept).post(path); break;
      case PUT: res = reqSpec.contentType(accept).put(path); break;
      case PATCH: res = reqSpec.contentType(accept).patch(path); break;
      case DELETE: res = reqSpec.delete(path); break;
      default: return;
    }

    res.
        then().
        statusCode(UNAUTHORIZED.code()).
        header("Content-Type", "application/json").
        body("type", equalTo("error")).
        body("statusCode", equalTo(401)).
        body("reasonPhrase", equalTo("Unauthorized")).
        body("message", not(isEmptyOrNullString())).
        body("streamId", not(isEmptyOrNullString()));
  }

  private void checkNotFound(final Method method, final String accept, final String path) {
    RequestSpecification reqSpec = given().
        accept(accept).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN));

    Response res;

    switch(method) {
      case GET: res = reqSpec.when().get(path); break;
      case POST: res = reqSpec.contentType(APPLICATION_JSON).body("{}").post(path); break;
      case PUT: res = reqSpec.contentType(APPLICATION_JSON).body("{}").put(path); break;
      case PATCH: res = reqSpec.contentType(APPLICATION_JSON).body("{}").patch(path); break;
      case DELETE: res = reqSpec.when().delete(path); break;
      default: return;
    }

    res.
        then().
        statusCode(NOT_FOUND.code()).
        header("Content-Type", "application/json");
  }
}
