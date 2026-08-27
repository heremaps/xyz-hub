/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadFeatureCompositeSpaceWithVersionIT extends TestSpaceWithFeature {

  private static final String spaceId = getSpaceId();
  private static final String extSpaceId = getSpaceId() + "-ext";
  private static final String extOfExtSpaceId = extSpaceId + "-ext";

  private static void createSpaceWithVersionExtension(String spaceId, String baseSpaceId, long baseVersion, int versionsToKeep) {
    String reqBody = String.format("""
            {
              "id": "%s",
              "title": "x-psql-test-extension",
              "extends": {
                "spaceId": "%s",
                "version": "%s"
              }%s
            }
            """, spaceId, baseSpaceId, baseVersion, (versionsToKeep <= 0 ? "" : ",\"versionsToKeep\":" + versionsToKeep));

    createSpace(reqBody);
  }

  @BeforeEach
  public void setup() {
    tearDown();

    createSpaceWithId(spaceId, 100);
    addFeature(spaceId, newFeature("base-1"));
    addFeature(spaceId, newFeature("base-2"));

    createSpaceWithVersionExtension(extSpaceId, spaceId, 1, 100);
    addFeature(extSpaceId, newFeature("delta1-1"));
    addFeature(extSpaceId, newFeature("delta1-2"));

    createSpaceWithVersionExtension(extOfExtSpaceId, extSpaceId, 1, 100);
    addFeature(extOfExtSpaceId, newFeature("delta2-1"));
    addFeature(extOfExtSpaceId, newFeature("delta2-2"));

  }

  @AfterEach
  public void tearDown() {
    removeSpace(spaceId);
    removeSpace(extSpaceId);
    removeSpace(extOfExtSpaceId);
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void readFeaturesFromComposite(String endpoint) throws Exception {
    ///Read from composite space with base version
    loadFeatures(extSpaceId, DEFAULT, endpoint)
            .body("features", hasSize(3))
            .body("features.id", containsInAnyOrder("base-1", "delta1-1",  "delta1-2"));

    loadFeatures(extSpaceId, EXTENSION, endpoint)
            .body("features", hasSize(2))
            .body("features.id", containsInAnyOrder("delta1-1",  "delta1-2"));

    loadFeatures(extSpaceId, SUPER, endpoint)
            .body("features", hasSize(1))
            .body("features.id", containsInAnyOrder("base-1"));

    ///Read from composite-of-composite space with base version
    loadFeatures(extOfExtSpaceId, DEFAULT, endpoint)
            .body("features", hasSize(4))
            .body("features.id", containsInAnyOrder("base-1", "delta1-1", "delta2-1", "delta2-2"));

    loadFeatures(extOfExtSpaceId, EXTENSION, endpoint)
            .body("features", hasSize(2))
            .body("features.id", containsInAnyOrder("delta2-1",  "delta2-2"));

    loadFeatures(extOfExtSpaceId, SUPER, endpoint)
            .body("features", hasSize(2))
            .body("features.id", containsInAnyOrder("base-1", "delta1-1"));
  }

  private ValidatableResponse loadFeatures(String spaceId, SpaceContext context, String endpoint) {
    String uri = getSpacesPath() + "/" + spaceId + "/" + endpoint + (endpoint.equals("tile") ? "/quadkey/0" : "");
    Map<String, Object> queryParams = new HashMap<>();
    if (endpoint.equals("features"))
      queryParams.put("id", List.of("base-1", "delta1-1",  "delta1-2", "delta2-1", "delta2-2"));
    else if (endpoint.equals("bbox"))
      queryParams.putAll(Map.of("west", "-10", "south", "-10", "east", "10", "north", "10"));
    else if (endpoint.equals("spatial"))
      queryParams.putAll(Map.of("lat", "0", "lon", "0", "radius", "10"));

    queryParams.put("context", context);
    return given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .queryParams(queryParams)
            .when()
            .get(uri)
            .then()
            .statusCode(OK.code());
  }

}
