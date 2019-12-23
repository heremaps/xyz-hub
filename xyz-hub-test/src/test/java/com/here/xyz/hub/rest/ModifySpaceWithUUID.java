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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.jayway.restassured.response.ValidatableResponse;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ModifySpaceWithUUID extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    final ValidatableResponse response = given().
        contentType(APPLICATION_JSON).
        accept(APPLICATION_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/createSpaceWithUUID.json")).
        when().post("/spaces").then();

    response.statusCode(OK.code()).
        body("id", equalTo("x-psql-test")).
        body("title", equalTo("My Demo Space")).
        body("storage.id", equalTo("psql"));
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void checkUUIDExists() {
    FeatureCollection featureCollection = generateRandomFeatures(1, 8);
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(featureCollection.serialize()).
        when().
        post("/spaces/x-psql-test/features").
        then().
        statusCode(OK.code()).
        body("features.size()", equalTo(1)).
        body("features[0].properties.'@ns:com:here:xyz'.uuid", notNullValue());
  }
}
