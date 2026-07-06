/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class BranchFeatureApiIT extends TestSpaceBranch {

  @BeforeEach
  public void setup() {
    createSpaceWithVersionsToKeep(SPACE_ID, 10000);

    //Add one sample feature, to allow branching on the space
    addFeature(SPACE_ID, createSampleFeature("main0"));
  }

  @AfterEach
  public void cleanUp() {
    removeSpace(SPACE_ID);
    removeAllBranchesForSpace(SPACE_ID);
  }

  @Test
  public void writeFeaturesToBranch() throws Exception {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    //Checking if features from main exists without adding new features to branch
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of(), Set.of("main0"));

    // Add new features to branch
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of("f1", "f2"), Set.of("main0", "f1", "f2"));

    // Add new features to main
    addFeaturesToBranchAndVerify(null, Set.of("main1"), Set.of("main0", "main1"));

    // Check that new feature on main does not exist on branch
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of(), Set.of("main0", "f1", "f2"));

  }

  @Test
  public void writeFeaturesToBranchOfBranch() throws Exception {
    //Create branch1
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    //Checking if features from main exists without adding new features to branch
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of(), Set.of("main0"));

    //Add features to branch b1
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of("b1_1", "b1_2"), Set.of("main0", "b1_1", "b1_2"));

    //Create and add features to branch2
    createBranch(SPACE_ID, B2_B1, B1_MAIN )
            .body("id", equalTo(B2_B1));

    addFeaturesToBranchAndVerify(B2_B1, Set.of("b2_1", "b2_2"), Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2"));

    // Add Features to main
    addFeaturesToBranchAndVerify(null, Set.of("main1"), Set.of("main0", "main1"));

    // Read features from branch b1
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of(), Set.of("main0", "b1_1", "b1_2"));

    // Read features from branch b22
    addFeaturesToBranchAndVerify(B2_B1, Set.of(), Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2"));

  }


  // We should test the behavior on the branches when the definition of middle branch is changed (merged, rebased, deleted)
  // in context with multi-level branching
  @ParameterizedTest
  @ValueSource(strings = {"merge", "rebase", "delete"})
  public void multilevelBranchingWithChangedMiddleBranch(String operation) throws Exception {
    // Create and add features to branch1
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of("b1_1", "b1_2"), Set.of("main0", "b1_1", "b1_2"));

    // Create and add features to branch2
    createBranch(SPACE_ID, B2_B1, B1_MAIN)
            .body("id", equalTo(B2_B1));
    addFeaturesToBranchAndVerify(B2_B1, Set.of("b2_1", "b2_2"), Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2"));

    //Add features to main and update the middle branch config
    addFeaturesToBranchAndVerify(null, Set.of("main1"), Set.of("main0", "main1"));
    switch (operation) {
      case "rebase" -> rebaseBranch(SPACE_ID, B1_MAIN, "2");
      case "merge" -> mergeBranch(SPACE_ID, B1_MAIN, "main");
      case "delete" -> deleteBranch(SPACE_ID, B1_MAIN);
    }

    addFeaturesToBranchAndVerify(B2_B1, Set.of("b2_3"), Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2", "b2_3"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void readFeaturesFromBranch(String endpoint) throws Exception {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));
    addFeatureToBranch(SPACE_ID, B1_MAIN, createSampleFeature("f1"));

    String uri = getSpacesPath() + "/" + SPACE_ID + "/" + endpoint + (endpoint.equals("tile") ? "/quadkey/0" : "");
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("versionRef", B1_MAIN);

    if (endpoint.equals("features"))
      queryParams.put("id", List.of("main0", "f1"));
    else if (endpoint.equals("bbox"))
      queryParams.putAll(Map.of("west", "-10", "south", "-10", "east", "10", "north", "10"));
    else if (endpoint.equals("spatial"))
      queryParams.putAll(Map.of("lat", "0", "lon", "0", "radius", "10"));

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .queryParams(queryParams)
            .when()
            .get(uri)
            .then()
            .statusCode(OK.code())
            .body("features", hasSize(2))
            .body("features.id", containsInAnyOrder("main0", "f1"));
  }

  @Test
  public void readBranchFeaturesFromTag() throws Exception {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    // Add new features to branch
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of("f1", "f2"), Set.of("main0", "f1", "f2"));
    addFeaturesToBranchAndVerify(B1_MAIN, Set.of("f3"), Set.of("main0", "f1", "f2", "f3"));

    String tag = "tag_b1";
    createTag(SPACE_ID, tag, B1_MAIN + ":3");

    addFeaturesToBranchAndVerify(tag, Set.of(), Set.of("main0", "f1", "f2"));
  }

  private Set<String> extractFeatureIds(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null) return Set.of();
    return featureCollection.getFeatures()
            .stream()
            .map(feature -> feature.getId())
            .collect(Collectors.toSet());
  }

  private void addFeaturesToBranchAndVerify(String branchId, Set<String> newFeatures, Set<String> expectedFeatures) throws Exception {
    for(String featureId : newFeatures) {
      addFeatureToBranch(SPACE_ID, branchId, createSampleFeature(featureId));
    }

    FeatureCollection fc = readFeaturesFromBranch(SPACE_ID, branchId);
    Set<String> actualFeatures = extractFeatureIds(fc);
    assertEquals(expectedFeatures, actualFeatures);
  }

}
