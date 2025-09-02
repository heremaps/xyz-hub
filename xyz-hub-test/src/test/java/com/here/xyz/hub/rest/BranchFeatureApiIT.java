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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Disabled
public class BranchFeatureApiIT extends BranchApiIT {
  /*
  - Write features on
    - main
    - branch of main
    - branch of a branch of main

  - Read features for the above cases and validate

  - Check if all the read feature endpoints are working as expected
    - iterate
    - search
    - id
    - tile
    - spatial

  - Try to read/write features after deleting an intermediate branch
   */

  @Test
  public void writeFeaturesToBranch() throws Exception {
    String branchId = "branch_main";
    createBranch(SPACE_ID, branchId, null)
            .body("id", equalTo(branchId));

    FeatureCollection fc;
    Set<String> fIds;

    fc = readFeaturesFromBranch(SPACE_ID, branchId);
    fIds = extractFeatureIds(fc);
    assertEquals(1, fc.getFeatures().size());
    assertTrue(fIds.contains("main0"));

    // Add Features to branch
    addFeatureToBranch(SPACE_ID, branchId, createSampleFeature("f1"));
    addFeatureToBranch(SPACE_ID, branchId, createSampleFeature("f2"));
    fc = readFeaturesFromBranch(SPACE_ID, branchId);
    fIds = extractFeatureIds(fc);
    assertEquals(3, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "f1", "f2")));

    // Add Features to main
    addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1"));
    fc = readFeaturesFromBranch(SPACE_ID, branchId);
    fIds = extractFeatureIds(fc);
    assertEquals(3, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "f1", "f2")));
    assertFalse(fIds.contains("main1"));
  }

  @Test
  public void writeFeaturesToBranchOfBranch() throws Exception {
    String branch1 = "b1_main";
    String branch2 = "b2_b1";

    FeatureCollection fc;
    Set<String> fIds;

    // Create branch1
    createBranch(SPACE_ID, branch1, null)
            .body("id", equalTo(branch1));

    fc = readFeaturesFromBranch(SPACE_ID, branch1);
    fIds = extractFeatureIds(fc);
    assertEquals(1, fc.getFeatures().size());
    assertTrue(fIds.contains("main0"));

    // Add Features to branch1
    addFeatureToBranch(SPACE_ID, branch1, createSampleFeature("b1_1"));
    addFeatureToBranch(SPACE_ID, branch1, createSampleFeature("b1_2"));
    fc = readFeaturesFromBranch(SPACE_ID, branch1);
    fIds = extractFeatureIds(fc);
    assertEquals(3, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "b1_1", "b1_2")));

    // Create and add features to branch2
    createBranch(SPACE_ID, branch2, branch1)
            .body("id", equalTo(branch2));
    addFeatureToBranch(SPACE_ID, branch2, createSampleFeature("b2_1"));
    addFeatureToBranch(SPACE_ID, branch2, createSampleFeature("b2_2"));
    fc = readFeaturesFromBranch(SPACE_ID, branch2);
    fIds = extractFeatureIds(fc);
    assertEquals(5, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2")));


    // Add Features to main
    addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1"));

    // Read features from branch1
    fc = readFeaturesFromBranch(SPACE_ID, branch1);
    fIds = extractFeatureIds(fc);
    assertEquals(3, fc.getFeatures().size());
    assertFalse(fIds.contains("main1"), "should not contains new features from parent branches");

    // Read features from branch2
    fc = readFeaturesFromBranch(SPACE_ID, branch2);
    fIds = extractFeatureIds(fc);
    assertEquals(5, fc.getFeatures().size());
    assertFalse(fIds.contains("main1"), "should not contains new features from parent branches");

  }

  @Test
  public void operateOnBranchAfterDeletingIntermediateBranch() throws Exception {
    String branch1 = "b1_main";
    String branch2 = "b2_b1";

    FeatureCollection fc;
    Set<String> fIds;

    // Create and add features to branch1
    createBranch(SPACE_ID, branch1, null)
            .body("id", equalTo(branch1));
    addFeatureToBranch(SPACE_ID, branch1, createSampleFeature("b1_1"));
    addFeatureToBranch(SPACE_ID, branch1, createSampleFeature("b1_2"));
    fc = readFeaturesFromBranch(SPACE_ID, branch1);
    fIds = extractFeatureIds(fc);
    assertEquals(3, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "b1_1", "b1_2")));

    // Create and add features to branch2
    createBranch(SPACE_ID, branch2, branch1)
            .body("id", equalTo(branch2));
    addFeatureToBranch(SPACE_ID, branch2, createSampleFeature("b2_1"));
    addFeatureToBranch(SPACE_ID, branch2, createSampleFeature("b2_2"));
    fc = readFeaturesFromBranch(SPACE_ID, branch1);
    fIds = extractFeatureIds(fc);
    assertEquals(5, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2")));

    //Delete branch1, then add features to branch2
    deleteBranch(SPACE_ID, branch1);
    addFeatureToBranch(SPACE_ID, branch2, createSampleFeature("b2_3"));
    fc = readFeaturesFromBranch(SPACE_ID, branch2);
    fIds = extractFeatureIds(fc);
    assertEquals(6, fc.getFeatures().size());
    assertTrue(fIds.containsAll(Set.of("main0", "b1_1", "b1_2", "b2_1", "b2_2", "b2_3")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"features", "iterate", "search", "bbox", "tile", "spatial"})
  public void readFeaturesFromBranch(String endpoint) throws Exception {
    String branchId = "branch_main";
    createBranch(SPACE_ID, branchId, null)
            .body("id", equalTo(branchId));
    addFeatureToBranch(SPACE_ID, branchId, createSampleFeature("f1"));

    String uri = getSpacesPath() + "/" + SPACE_ID + "/" + endpoint + (endpoint.equals("tile") ? "/quadkey/0" : "");
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("versionRef", branchId);

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

  private Set<String> extractFeatureIds(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null) return Set.of();
    return featureCollection.getFeatures()
            .stream()
            .map(feature -> feature.getId())
            .collect(Collectors.toSet());
  }

}
