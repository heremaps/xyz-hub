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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@Disabled
public class BranchApiIT extends TestSpaceBranch {

  @BeforeEach
  public void setup() {
    createSpaceWithVersionsToKeep(SPACE_ID, 10000);

    //Add one sample feature, to allow branching on the space
    addFeature(SPACE_ID, createSampleFeature("main0"));
  }

  @AfterEach
  public void cleanUp() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void createSimpleBranchOnMainHead() {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"HEAD", "1", "main", "main:HEAD", "main:1"})
  public void createBranchWithDifferentBaseRef(String baseRef) {
    createBranch(SPACE_ID, B1_MAIN, baseRef)
            .body("id", equalTo(B1_MAIN));
  }

  @Test
  public void createBranchOnMainVersion() throws Exception {
    long version1 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1")));
    long version2 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main2")));
    long version3 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main3")));

    String branch1 = "branch_main_" + version1;
    String branch2 = "branch_main_" + version2;
    String branch3 = "branch_main_" + version3;

    createBranch(SPACE_ID, branch1, "main:" + version1)
            //TODO: Also check version ref in the response
            .body("id", equalTo(branch1));


    createBranch(SPACE_ID, branch2, "main:" + version2)
            .body("id", equalTo(branch2));

    createBranch(SPACE_ID, branch3, "main:" + version3)
            .body("id", equalTo(branch3));
  }

  @Test
  public void createBranchOnBranch() {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    createBranch(SPACE_ID, B2_B1, B1_MAIN)
            .body("id", equalTo(B2_B1));

    createBranch(SPACE_ID, B3_B2, B2_B1)
            .body("id", equalTo(B3_B2));
  }

  @Test
  public void rebaseBranchOnMain() throws Exception {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    createBranch(SPACE_ID, B2_B1, B1_MAIN)
            .body("id", equalTo(B2_B1));

    //Increase versions of main space
    long version1 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1")));
    long version2 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main2")));

    //Rebase branch b1 on main head
    rebaseBranch(SPACE_ID, B1_MAIN, "main:HEAD")
            .body("id", equalTo(B1_MAIN))
            .body("baseRef", equalTo("main:" + version2));

    //Rebase branch b2 on main version 1
    rebaseBranch(SPACE_ID, B2_B1, "main:" + version1)
            .body("id", equalTo(B2_B1))
            .body("baseRef", equalTo("main:" + version1));;
  }

  @Test
  public void deleteBranchOnMain() {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    deleteBranch(SPACE_ID, B1_MAIN);
    getBranch(SPACE_ID, B1_MAIN, NOT_FOUND.code());
  }

  @Test
  public void deleteSpaceShouldDeleteAllBranches() {
    createBranch(SPACE_ID, B1_MAIN, null)
            .body("id", equalTo(B1_MAIN));

    createBranch(SPACE_ID, B2_B1, B1_MAIN)
            .body("id", equalTo(B2_B1));

    assertEquals("should have branches before removing space", 2, getBranches(SPACE_ID).size());

    removeSpace(SPACE_ID);

    assertEquals("should not have any branch after removing space", 0, getBranches(SPACE_ID).size());

  }

  /**
   * Returns the version of the first feature from the FeatureCollection
   *
   * @param featureCollection expected to contain the features of a single transaction
   */
  protected Long extractVersion(FeatureCollection featureCollection) throws JsonProcessingException {
    if (featureCollection == null || featureCollection.getFeatures() == null || featureCollection.getFeatures().isEmpty())
      return null;

    return featureCollection.getFeatures().get(0).getProperties().getXyzNamespace().getVersion();
  }

}
