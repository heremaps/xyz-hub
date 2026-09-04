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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  private static Feature updatedFeature(String id, String value) {
    return newFeature(id).withProperties(new Properties().with("key1", value));
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
    removeSpace(spaceId + "-a");
    removeSpace(spaceId + "-b");
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void readFeaturesFromComposite(String endpoint) {
    ///Read from composite space with base version
    loadFeatures(extSpaceId, DEFAULT, endpoint)
        .body("features", hasSize(3))
        .body("features.id", containsInAnyOrder("base-1", "delta1-1", "delta1-2"));

    loadFeatures(extSpaceId, EXTENSION, endpoint)
        .body("features", hasSize(2))
        .body("features.id", containsInAnyOrder("delta1-1", "delta1-2"));

    loadFeatures(extSpaceId, SUPER, endpoint)
        .body("features", hasSize(1))
        .body("features.id", containsInAnyOrder("base-1"));

    ///Read from composite-of-composite space with base version
    loadFeatures(extOfExtSpaceId, DEFAULT, endpoint)
        .body("features", hasSize(4))
        .body("features.id", containsInAnyOrder("base-1", "delta1-1", "delta2-1", "delta2-2"));

    loadFeatures(extOfExtSpaceId, EXTENSION, endpoint)
        .body("features", hasSize(2))
        .body("features.id", containsInAnyOrder("delta2-1", "delta2-2"));

    loadFeatures(extOfExtSpaceId, SUPER, endpoint)
        .body("features", hasSize(2))
        .body("features.id", containsInAnyOrder("base-1", "delta1-1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void baseFeatureIsReadAtTheSpecifiedBaseVersion(String endpoint) {
    addFeature(spaceId, updatedFeature("base-1", "value-v3")); //base version 3

    String extV1 = spaceId + "-a";
    String extV3 = spaceId + "-b";
    createSpaceWithVersionExtension(extV1, spaceId, 1, 100);
    createSpaceWithVersionExtension(extV3, spaceId, 3, 100);

    loadFeatures(extV1, SUPER, endpoint)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(1));

    loadFeatures(extV3, SUPER, endpoint)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value-v3"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(3));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void baseFeatureUpdatedAfterExtensionVersionKeepsOldValue(String endpoint) {
    addFeature(spaceId, updatedFeature("base-1", "updated")); //base version 3

    loadFeatures(extSpaceId, DEFAULT, endpoint)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"));

    loadFeatures(extSpaceId, SUPER, endpoint)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void baseFeatureDeletedAfterExtensionVersionStaysVisible(String endpoint) {
    deleteFeature(spaceId, "base-1"); //base version 3 tombstone

    loadFeatures(extSpaceId, DEFAULT, endpoint)
        .body("features.id", hasItem("base-1"));

    loadFeatures(extSpaceId, SUPER, endpoint)
        .body("features.id", hasItem("base-1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void baseFeatureDeletedAtBoundVersionIsNotVisible(String endpoint) {
    deleteFeature(spaceId, "base-1");

    String extBefore = spaceId + "-a";   //bound to version 1 (before the delete)
    String extAtDelete = spaceId + "-b"; //bound to version 3 (the delete)
    createSpaceWithVersionExtension(extBefore, spaceId, 1, 100);
    createSpaceWithVersionExtension(extAtDelete, spaceId, 3, 100);

    loadFeatures(extBefore, SUPER, endpoint)
        .body("features.id", hasItem("base-1"));

    loadFeatures(extAtDelete, SUPER, endpoint)
        .body("features.id", not(hasItem("base-1")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void baseFeatureReportsItsOwnModificationVersionNotBoundVersion(String endpoint) {
    addFeature(spaceId, newFeature("base-3")); //base version 3 (base-1 stays at version 1)

    String extV3 = spaceId + "-a";
    createSpaceWithVersionExtension(extV3, spaceId, 3, 100);

    loadFeatures(extV3, SUPER, endpoint)
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(1))
        .body("features.find { it.id == 'base-3' }.properties.'@ns:com:here:xyz'.version", equalTo(3));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void intermediateChangeAfterExtensionVersionStaysVisible(String endpoint) {
    deleteFeature(extSpaceId, "delta1-1"); //intermediate version 3

    loadFeatures(extOfExtSpaceId, DEFAULT, endpoint)
        .body("features.id", hasItem("delta1-1"));
  }

  private ValidatableResponse loadFeatures(String spaceId, SpaceContext context, String endpoint) {
    String uri = getSpacesPath() + "/" + spaceId + "/" + endpoint + (endpoint.equals("tile") ? "/quadkey/0" : "");
    Map<String, Object> queryParams = new HashMap<>();
    switch (endpoint) {
      case "features" -> queryParams.put("id", List.of("base-1", "base-3", "delta1-1", "delta1-2", "delta2-1", "delta2-2"));
      case "bbox" -> queryParams.putAll(Map.of("west", "-10", "south", "-10", "east", "10", "north", "10"));
      case "spatial" -> queryParams.putAll(Map.of("lat", "0", "lon", "0", "radius", "10"));
    }

    queryParams.put("context", context);
    queryParams.put("skipCache", "true"); //bypass the hub response cache so version-bound reads always hit the DB

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
