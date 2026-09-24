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
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import io.restassured.response.ValidatableResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CompositeSpaceOnBaseVersionIT extends TestSpaceWithFeature {

  private static final String spaceId = getSpaceId();
  private static final String extSpaceId = spaceId + "-ext";
  private static final String extOfExtSpaceId = extSpaceId + "-ext";
  private final List<String> testSpecificSpaceIds = new ArrayList<>();

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
        """, spaceId, baseSpaceId, baseVersion == -1 ? null : baseVersion, (versionsToKeep <= 0 ? "" : ",\"versionsToKeep\":" + versionsToKeep));

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
    for (int i = testSpecificSpaceIds.size() - 1; i >= 0; i--)
      removeSpace(testSpecificSpaceIds.get(i));
    testSpecificSpaceIds.clear();

    removeSpace(spaceId);
    removeSpace(extSpaceId);
    removeSpace(extOfExtSpaceId);
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

  @Test
  public void baseFeatureIsReadAtTheSpecifiedBaseVersion() {
    String versionOneSpaceId = testSpecificSpaceId("-base-v1");
    String versionThreeSpaceId = testSpecificSpaceId("-base-v3");
    addFeature(spaceId, updatedFeature("base-1", "value-v3")); //base version 3

    createSpaceWithVersionExtension(versionOneSpaceId, spaceId, 1, 100);
    createSpaceWithVersionExtension(versionThreeSpaceId, spaceId, 3, 100);

    loadFeatures(versionOneSpaceId, SUPER)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(1));

    loadFeatures(versionThreeSpaceId, SUPER)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value-v3"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(3));
  }

  @Test
  public void logicalVersionZeroReadsConfiguredBaseVersion() {
    String versionThreeSpaceId = testSpecificSpaceId("-base-v3");
    addFeature(spaceId, updatedFeature("base-1", "value-v3")); //base version 3
    createSpaceWithVersionExtension(versionThreeSpaceId, spaceId, 3, 100);

    loadFeatures(versionThreeSpaceId, DEFAULT, "iterate", "0")
        .body("features", hasSize(2))
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value-v3"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void logicalVersionZeroReadsComposedNestedBaseVersion() {
    String nestedSpaceId = testSpecificSpaceId("-intermediate-v3");
    patchFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("intermediate", "override"))); //intermediate version 3
    createSpaceWithVersionExtension(nestedSpaceId, extSpaceId, 3, 100);

    loadFeatures(nestedSpaceId, DEFAULT, "iterate", "0")
        .body("features.findAll { it.id == 'base-1' }", hasSize(1))
        .body("features.find { it.id == 'base-1' }.properties.intermediate", equalTo("override"))
        .body("features.find { it.id == 'base-1' }.properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void baseFeatureUpdatedAfterExtensionVersionKeepsOldValue() {
    addFeature(spaceId, updatedFeature("base-1", "updated")); //base version 3

    loadFeatures(extSpaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"));

    loadFeatures(extSpaceId, SUPER)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"));

    loadFeatures(spaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("updated"));
  }

  @Test
  public void baseFeatureDeletedAfterExtensionVersionStaysVisible() {
    deleteFeature(spaceId, "base-1");

    loadFeatures(extSpaceId, DEFAULT)
        .body("features.id", hasItem("base-1"));

    loadFeatures(extSpaceId, SUPER)
        .body("features.id", hasItem("base-1"));
  }

  @Test
  public void baseFeatureDeletedAtBoundVersionIsNotVisible() {
    String deleteHistoryBaseSpaceId = testSpecificSpaceId("-delete-history");
    String beforeDeleteSpaceId = testSpecificSpaceId("-delete-history-before-delete");
    String atDeleteSpaceId = testSpecificSpaceId("-delete-history-at-delete");
    createSpaceWithId(deleteHistoryBaseSpaceId, 100);
    addFeature(deleteHistoryBaseSpaceId, newFeature("base-1"));
    addFeature(deleteHistoryBaseSpaceId, newFeature("base-2"));
    deleteFeature(deleteHistoryBaseSpaceId, "base-1");

    createSpaceWithVersionExtension(beforeDeleteSpaceId, deleteHistoryBaseSpaceId, 1, 100);
    createSpaceWithVersionExtension(atDeleteSpaceId, deleteHistoryBaseSpaceId, 3, 100);

    loadFeatures(beforeDeleteSpaceId, SUPER)
        .body("features.id", hasItem("base-1"));

    loadFeatures(atDeleteSpaceId, SUPER)
        .body("features.id", not(hasItem("base-1")));
  }

  @Test
  public void extendingASpecificVersionOfASpaceWithoutHistoryIsRejected() {
    String noHistoryBaseSpaceId = testSpecificSpaceId("-no-history-base");
    createSpaceWithId(noHistoryBaseSpaceId); //Without history, so no stable snapshot can be provided
    addFeature(noHistoryBaseSpaceId, newFeature("base-1"));

    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(String.format("""
            {"id": "%s", "title": "x-psql-test-extension", "extends": {"spaceId": "%s", "version": 1}}
            """, testSpecificSpaceId("-on-no-history-base"), noHistoryBaseSpaceId))
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void readingAVersionBeyondTheBoundBaseVersionIsRejected() {
    //extSpaceId is bound to version 1 of spaceId, so version 2 of the base is not readable through it
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .queryParams(Map.of("context", SUPER, "versionRef", "2"))
        .when()
        .get(getSpacesPath() + "/" + extSpaceId + "/iterate")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void intermediateChangeAfterExtensionVersionStaysVisible() {
    deleteFeature(extSpaceId, "delta1-1"); //intermediate version 3

    loadFeatures(extOfExtSpaceId, DEFAULT)
        .body("features.id", hasItem("delta1-1"));
  }

  @Test
  public void nestedPartialUpdateMergesOntoIntermediateStateAtBoundVersion() {
    //The intermediate overrides the inherited feature after the outer composite was bound to version 1
    patchFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("intermediate", "after-bound-version"))); //intermediate version 3

    //so the outer composite still reads it in the state it had at the bound version
    loadFeatures(extOfExtSpaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.intermediate", nullValue());

    //and a partial update through the outer composite has to be merged onto that very state.
    patchFeature(extOfExtSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("outer", "value")));

    loadFeatures(extOfExtSpaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.outer", equalTo("value"))
        .body("features.find { it.id == 'base-1' }.properties.intermediate", nullValue());

    //The intermediate itself stays untouched by the write which went into the outer extension
    loadFeatures(extSpaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.intermediate", equalTo("after-bound-version"))
        .body("features.find { it.id == 'base-1' }.properties.outer", nullValue());
  }

  @Test
  public void nestedPartialUpdateWorksAfterIntermediateDeletedTheInheritedFeature() {
    //The intermediate hides the inherited feature after the outer composite was bound to version 1
    deleteFeature(extSpaceId, "base-1"); //intermediate version 3

    loadFeatures(extSpaceId, DEFAULT)
        .body("features.id", not(hasItem("base-1")));

    //but the outer composite still sees it at the bound version
    loadFeatures(extOfExtSpaceId, DEFAULT)
        .body("features.id", hasItem("base-1"));

    //so a partial update through the outer composite has to succeed instead of answering 404.
    patchFeature(extOfExtSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("outer", "value")));

    loadFeatures(extOfExtSpaceId, DEFAULT)
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.outer", equalTo("value"));
  }

  @Test
  public void partialUpdateThroughCompositeMergesOntoBoundBaseVersion() {
    //The base moves after the composite was bound to version 1 ...
    addFeature(spaceId, updatedFeature("base-1", "value-v3")); //base version 3

    //the composite still reads the base at the bound version
    loadFeatures(extSpaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"));

    //and a partial update through the composite has to be merged onto that very state.
    patchFeature(extSpaceId, newFeature("base-1").withProperties(new Properties().with("key2", "delta")));

    loadFeatures(extSpaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.key2", equalTo("delta"));

    //The base itself stays untouched by the write which went into the extension
    loadFeatures(spaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value-v3"));
  }

  @Test
  public void baseVersionPartialUpdateMergesWithExtensionOverride() {
    patchFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("headDelta", "head")));

    patchFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties()
            .withXyzNamespace(new XyzNamespace().withVersion(0))
            .with("staleDelta", "stale")));

    loadFeatures(extSpaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.headDelta", equalTo("head"))
        .body("features.find { it.id == 'base-1' }.properties.staleDelta", equalTo("stale"));
  }

  @Test
  public void conflictDetectionUsesTheBoundBaseAsLogicalVersionZero() {
    addFeature(spaceId, updatedFeature("base-1", "value-after-bound"));

    postFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties()
            .withXyzNamespace(new XyzNamespace().withVersion(0))
            .with("delta", "value")), AuthProfile.ACCESS_ALL, true);

    loadFeatures(extSpaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.key1", equalTo("value1"))
        .body("features.find { it.id == 'base-1' }.properties.delta", equalTo("value"));
  }

  @Test
  public void conflictDetectionRejectsLogicalVersionZeroAfterDeltaOverride() {
    postFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties()
            .withXyzNamespace(new XyzNamespace().withVersion(0))
            .with("delta", "first")), AuthProfile.ACCESS_ALL, true);

    postFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties()
            .withXyzNamespace(new XyzNamespace().withVersion(0))
            .with("delta", "stale")), AuthProfile.ACCESS_ALL, true, CONFLICT);
  }

  @Test
  public void conflictDetectionUsesComposedNestedBaseAsLogicalVersionZero() {
    String nestedSpaceId = testSpecificSpaceId("-intermediate-v3");
    patchFeature(extSpaceId, newFeature("base-1")
        .withProperties(new Properties().with("intermediate", "override"))); //intermediate version 3
    createSpaceWithVersionExtension(nestedSpaceId, extSpaceId, 3, 100);

    postFeature(nestedSpaceId, newFeature("base-1")
        .withProperties(new Properties()
            .withXyzNamespace(new XyzNamespace().withVersion(0))
            .with("outer", "value")), AuthProfile.ACCESS_ALL, true);

    loadFeatures(nestedSpaceId, DEFAULT, "iterate")
        .body("features.find { it.id == 'base-1' }.properties.intermediate", equalTo("override"))
        .body("features.find { it.id == 'base-1' }.properties.outer", equalTo("value"));
  }

  private String testSpecificSpaceId(String suffix) {
    String id = spaceId + suffix;
    testSpecificSpaceIds.add(id);
    return id;
  }

  private static void patchFeature(String spaceId, Feature feature) {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(feature.serialize())
        .when()
        .patch(getSpacesPath() + "/" + spaceId + "/features/" + feature.getId())
        .then()
        .statusCode(OK.code());
  }

  private ValidatableResponse loadFeatures(String spaceId, SpaceContext context) {
    return loadFeatures(spaceId, context, "iterate");
  }

  private ValidatableResponse loadFeatures(String spaceId, SpaceContext context, String endpoint) {
    return loadFeatures(spaceId, context, endpoint, null);
  }

  private ValidatableResponse loadFeatures(String spaceId, SpaceContext context, String endpoint, String versionRef) {
    String uri = getSpacesPath() + "/" + spaceId + "/" + endpoint + (endpoint.equals("tile") ? "/quadkey/0" : "");
    Map<String, Object> queryParams = new HashMap<>();
    switch (endpoint) {
      case "features" -> queryParams.put("id", List.of("base-1", "base-3", "delta1-1", "delta1-2", "delta2-1", "delta2-2"));
      case "bbox" -> queryParams.putAll(Map.of("west", "-10", "south", "-10", "east", "10", "north", "10"));
      case "spatial" -> queryParams.putAll(Map.of("lat", "0", "lon", "0", "radius", "10"));
    }

    queryParams.put("context", context);
    if (versionRef != null)
      queryParams.put("versionRef", versionRef);

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
