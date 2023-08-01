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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.restassured.RestAssured.given;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class FeatureModificationIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void beforeClass() {
    RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig().encodeContentTypeAs("application/vnd.here.feature-modification-list",
        ContentType.JSON));
  }

  @Before
  public void before() {
    remove();
    createSpace();
  }

  @After
  public void after() {
    remove();
  }

  public String constructPayload(Feature feature, String ifNotExists, String ifExists, String conflictResolution) {
    return "{"
        + "    \"type\": \"FeatureModificationList\","
        + "    \"modifications\": ["
        + "        {"
        + "            \"type\": \"FeatureModification\","
        + "            \"onFeatureNotExists\": \""+ifNotExists+"\","
        + "            \"onFeatureExists\": \""+ifExists+"\","
        + "            \"onMergeConflict\": \""+conflictResolution+"\","
        + "            \"featureData\": " + new FeatureCollection().withFeatures(Collections.singletonList(feature)).serialize()
        + "        }"
        + "    ]"
        + "}";
  }

  public ValidatableResponse write(Feature feature, String ifNotExists, String ifExists, String conflictResolution) {
    return given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(feature, ifNotExists, ifExists, conflictResolution))
        .when().post(getSpacesPath() + "/"+ getSpaceId() +"/features").then();
  }

  @Test
  public void testInvalidPayload() {
    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(new Feature().withId("F1"), "invalid", "patch", "error"))
        .when()
        .post(getSpacesPath() + "/"+ getSpaceId() +"/features")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(new Feature().withId("F1"), "create", "invalid", "error"))
        .when()
        .post(getSpacesPath() + "/"+ getSpaceId() +"/features")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(new Feature().withId("F1"), "create", "patch", "invalid"))
        .when()
        .post(getSpacesPath() + "/"+ getSpaceId() +"/features")
        .then()
        .statusCode(BAD_REQUEST.code());
  }
}
