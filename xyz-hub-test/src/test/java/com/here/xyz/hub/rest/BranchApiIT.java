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

import static com.here.xyz.util.Random.randomAlpha;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class BranchApiIT extends TestSpaceBranch {

  protected String SPACE_ID = getClass().getSimpleName() + "_" + randomAlpha(5);

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
  public void createSimpleBranchOnMainHead() {
    createBranch(SPACE_ID, "branch_main_head", null)
            .body("id", equalTo("branch_main_head"));
  }

  @Test
  public void createBranchOnMainVersion() throws Exception {
    long version1 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1")));
    long version2 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main2")));
    long version3 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main3")));

    createBranch(SPACE_ID, "branch_main_" + version1, "" + version1)
            //TODO: Also check version ref in the response
            .body("id", equalTo("branch_main_" + version1));


    createBranch(SPACE_ID, "branch_main_" + version2, "main:" + version2)
            .body("id", equalTo("branch_main_" + version2));

    createBranch(SPACE_ID, "branch_main_" + version3, "main" + version3)
            .body("id", equalTo("branch_main_" + version3));
  }

  @Test
  public void createBranchOnBranch() {
    createBranch(SPACE_ID, "b1_main_head", null)
            .body("id", equalTo("b1_main_head"));

    createBranch(SPACE_ID, "b2_b1_head", "b1_main_head")
            .body("id", equalTo("b2_b1_head"));

    createBranch(SPACE_ID, "b3_b2_head", "b2_b1_head")
            .body("id", equalTo("b2_b2_head"));
  }

  @Test
  public void rebaseBranchOnMain() throws Exception {
    createBranch(SPACE_ID, "b1_main_head", null)
            .body("id", equalTo("b1_main_head"));

    createBranch(SPACE_ID, "b2_b1_head", "b1_main_head")
            .body("id", equalTo("b2_b1_head"));

    //Increase versions of main space
    long version1 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main1")));
    long version2 = extractVersion(addFeatureToBranch(SPACE_ID, null, createSampleFeature("main2")));

    //Rebase branch b1 on main head
    rebaseBranch(SPACE_ID, "b1_main_head", "main:HEAD");

    //Rebase branch b2 on main version 1
    rebaseBranch(SPACE_ID, "b2_b1_head", "main:" + version1);
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
