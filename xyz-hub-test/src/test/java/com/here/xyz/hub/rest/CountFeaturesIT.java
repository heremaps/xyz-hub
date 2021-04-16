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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.http.HttpHeaders.IF_NONE_MATCH;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.jayway.restassured.RestAssured;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CountFeaturesIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setup() {
    remove();
    createSpace();
    addFeatures();
  }

  @AfterClass
  public static void tearDownClass() {
    remove();
  }

  @Test
  public void testNotExistingForCount() {
    given().
        accept(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get("/spaces/x-psql-test1/count").
        then().
        statusCode(NOT_FOUND.code());
  }

  @Test
  public void testFullCount() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count").
        then().
        statusCode(OK.code());
  }

  @Test
  public void testBaseBallFeaturesCount() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count?tags=baseball").
        then().
        statusCode(OK.code()).
        body("count", equalTo(10));
  }

  @Test
  public void testFootBallFeaturesCount() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count?tags=football").
        then().
        statusCode(OK.code()).
        body("count", equalTo(41));
  }

  @Test
  public void testCommaSeparatedTags() {
    given().
        urlEncodingEnabled(false).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when()
        .get(getSpacesPath() + "/x-psql-test/count?tags=football,stadium").
        then().
        statusCode(OK.code()).
        body("count", equalTo(252));
  }

  @Test
  public void testCommaSeparatedTagsEncoded() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when()
        .get(getSpacesPath() + "/x-psql-test/count?tags=football,stadium").
        then().
        statusCode(OK.code()).
        body("count", equalTo(0));
  }

  @Test
  public void testCombinedTags() {
    boolean bFlag = RestAssured.urlEncodingEnabled;
    RestAssured.urlEncodingEnabled = false;

    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count?tags=football+stadium").
        then().
        statusCode(OK.code()).
        body("count", equalTo(41));

    RestAssured.urlEncodingEnabled = bFlag;
  }

  @Test
  public void testMultipleParameterTags() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count?tags=football&tags=stadium").
        then().
        statusCode(OK.code()).
        body("count", equalTo(252));
  }

  @Test
  public void testFullCountWithEtag() {
    String etag =
        given().
            accept(APPLICATION_JSON).
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            when().
            get(getSpacesPath() + "/x-psql-test/count").
            then().
            statusCode(OK.code()).
            // TODO: Fix precise counting
            //body("count", equalTo(252)).
                header("etag", notNullValue()).
            extract().
            header("etag");

    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        header(IF_NONE_MATCH, etag).
        when().
        get(getSpacesPath() + "/x-psql-test/count").
        then().
        statusCode(NOT_MODIFIED.code());
  }

  @Test
  public void testEmptyParams() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        when().
        get(getSpacesPath() + "/x-psql-test/count?tags=").
        then().
        statusCode(OK.code());
  }

  @Test
  public void testFullCountWithAllAccess() {
    given().
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        when().
        get(getSpacesPath() + "/x-psql-test/count").
        then().
        statusCode(OK.code());
  }
}
