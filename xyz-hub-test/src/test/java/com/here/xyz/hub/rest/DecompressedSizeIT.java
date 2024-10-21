/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DecompressedSizeIT extends TestSpaceWithFeature {

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
  public void testHeaderOutputSizeReporting() {
    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/x-psql-test/tile/quadkey/2100300120310022")
        .then()
        .header("X-Decompressed-Input-Size", "0")
        .header("X-Decompressed-Output-Size", "469");
  }

  @Test
  public void testHeaderInputSizeReporting() {
    String decompressedValue = given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(new JsonObject().put("type", "FeatureCollection").put("features", new JsonArray().add(new JsonObject().put("type", "Feature").put("id", "f1"))).toString())
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .prettyPeek()
        .then()
        .header("X-Decompressed-Input-Size", "70")
        .extract()
        .header("X-Decompressed-Output-Size");

    assertThat(decompressedValue, anyOf(equalTo("250"), equalTo("286")));
  }
}
