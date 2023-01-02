/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class StoreFeaturesApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    createSpace();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void putFeatures() {
    given().
        contentType(APPLICATION_GEO_JSON).
        accept("application/x-empty").
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/processedData.json")).
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(NO_CONTENT.code());
  }

  @Test
  public void putFeatureCollectionWithoutFeatureType() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}").
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());
  }

  @Test
  public void testFailureResponse() {
    FeatureCollection fc = new FeatureCollection().withFeatures(Arrays.asList(
        Feature.createEmptyFeature().withId("T1")));

    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(fc.serialize()).
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());

    FeatureCollection fcUpdate = new FeatureCollection().withFeatures(Arrays.asList(
        Feature.createEmptyFeature().withId("A1"),
        Feature.createEmptyFeature().withId("T1")));

    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(fcUpdate.serialize()).
        when().
        post(getSpacesPath() + "/x-psql-test/features?ne=retain&e=error&transactional=false").prettyPeek().
        then().
        statusCode(OK.code()).
        body("failed[0].id", equalTo("T1"));
  }

  @Test
  public void putFeaturesCheckCustomSizeLimit() {
    given().
            contentType(APPLICATION_GEO_JSON).
            accept("application/x-empty").
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            headers(new HashMap<String, String>(){{put("X-Upload-Content-Length-Limit","1");}}).
            body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}").
            when().
            put(getSpacesPath() + "/x-psql-test/features").
            then().
            statusCode(REQUEST_ENTITY_TOO_LARGE.code());
    given().
            contentType(APPLICATION_GEO_JSON).
            accept("application/x-empty").
            headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
            body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}").
            when().
            put(getSpacesPath() + "/x-psql-test/features").
            then().
            statusCode(NO_CONTENT.code());
  }

  @Test
  public void putFeatureWithAllowFeatureCreationWithUUID() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"properties\":{\"@ns:com:here:xyz\":{\"uuid\":\"12345-abcde\"}}}],\"type\":\"FeatureCollection\"}").
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(CONFLICT.code());

    given().
        contentType(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"allowFeatureCreationWithUUID\": true}").
        when().
        patch(getSpacesPath() + "/x-psql-test").
        then().
        statusCode(OK.code());

    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"features\":[{\"properties\":{\"@ns:com:here:xyz\":{\"uuid\":\"12345-abcde\"}}}],\"type\":\"FeatureCollection\"}").
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());
  }

  @Test
  public void testHeaderInputSizeReporting() {
    given().
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body("{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\"}]}").
        when().
        put(getSpacesPath() + "/x-psql-test/features").
        then().
        header("X-Decompressed-Input-Size", "63").
        header("X-Decompressed-Output-Size", "334").
        statusCode(OK.code());
  }
}
