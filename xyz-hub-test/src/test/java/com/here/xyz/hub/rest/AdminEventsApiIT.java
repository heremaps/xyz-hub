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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.response.ValidatableResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class AdminEventsApiIT extends TestSpaceWithFeature {

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

  private static ValidatableResponse postEvent(String event, AuthProfile profile) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(profile))
        .body(event)
        .when()
        .post("/admin/events")
        .then();
  }

  @Test
  public void testLoadFeaturesEvent() {
    postEvent(content("/xyz/hub/event/loadFeaturesEvent.json"), AuthProfile.ACCESS_OWNER_2)
        .statusCode(403);

    postEvent(content("/xyz/hub/event/loadFeaturesEvent.json"), AuthProfile.ACCESS_ALL)
        .statusCode(200)
        .body("type", equalTo("FeatureCollection"))
        .body("features[0].id", equalTo("Q3495887"));
  }
}
