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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A branch is anchored to the version of the root space it was created from and reads the root's history through its
 * branch path instead of copying it, so deleting the changesets below that version removes what the branch reads.
 *
 * <p>The setup updates one feature repeatedly on purpose: the purge deletes row versions that were superseded
 * ({@code next_version <= minVersion}), so writing distinct features would leave nothing deletable and the test would
 * pass without ever creating the condition.
 */
public class BranchBaseVersionPurgeProtectionIT extends TestSpaceBranch {

  private static final String BRANCH = "b1main";
  private static final String INHERITED = "f1";
  private static final String BRANCH_ONLY = "branchOnly";

  @BeforeEach
  public void setup() {
    createSpaceWithVersionsToKeep(SPACE_ID, 10000);
  }

  @AfterEach
  public void cleanUp() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void deletingChangesetsBelowTheBranchBaseVersionIsRejected() throws Exception {
    long branchBaseVersion = updateInheritedFeature(5);
    createBranch(SPACE_ID, BRANCH, "main:" + branchBaseVersion);
    addFeatureToBranch(SPACE_ID, BRANCH, feature(BRANCH_ONLY, "b"));

    Map<String, Object> readableBefore = branchContent();

    //Would delete the row versions superseded before the base version + 2, which includes the state the branch reads
    deleteChangesetsBelow(branchBaseVersion + 2)
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", containsString("branch " + BRANCH));

    assertEquals(readableBefore, branchContent());
  }

  @Test
  public void deletingTheBranchUnblocksTheDeletionOfChangesets() throws Exception {
    long branchBaseVersion = updateInheritedFeature(5);
    createBranch(SPACE_ID, BRANCH, "main:" + branchBaseVersion);

    deleteChangesetsBelow(branchBaseVersion + 2).statusCode(BAD_REQUEST.code());

    deleteBranch(SPACE_ID, BRANCH);

    deleteChangesetsBelow(branchBaseVersion + 2).statusCode(NO_CONTENT.code());
  }

  @Test
  public void deletingChangesetsUpToTheBranchBaseVersionKeepsTheBranchReadable() throws Exception {
    long branchBaseVersion = updateInheritedFeature(5);
    createBranch(SPACE_ID, BRANCH, "main:" + branchBaseVersion);
    addFeatureToBranch(SPACE_ID, BRANCH, feature(BRANCH_ONLY, "b"));

    Map<String, Object> readableBefore = branchContent();

    //Everything superseded before the base version may go, the state the branch is rooted at survives
    deleteChangesetsBelow(branchBaseVersion).statusCode(NO_CONTENT.code());

    assertEquals(readableBefore, branchContent());
  }

  /** A branch keeps its base version alive past the rolling window, so it has to be reported as the oldest available one. */
  @Test
  public void aBranchBaseVersionIsReportedAsTheOldestAvailableVersion() throws Exception {
    long branchBaseVersion = updateInheritedFeature(5);
    createBranch(SPACE_ID, BRANCH, "main:" + branchBaseVersion);

    //Shrink the rolling window so that it would no longer include the branch base version
    patchSpace(SPACE_ID, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());

    given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + SPACE_ID + "/statistics")
        .then().statusCode(OK.code())
        .body("minVersion.value", equalTo((int) branchBaseVersion));
  }

  /**
   * Updates a single feature the given number of times, so each write supersedes the previous row version.
   *
   * @return a version in the middle of the resulting history, leaving versions both below and above it
   */
  private long updateInheritedFeature(int updates) throws Exception {
    long middleVersion = -1;
    for (int i = 1; i <= updates; i++) {
      FeatureCollection written = addFeatureToBranch(SPACE_ID, null, feature(INHERITED, "v" + i));
      if (i == updates / 2 + 1)
        middleVersion = written.getFeatures().get(0).getProperties().getXyzNamespace().getVersion();
    }
    return middleVersion;
  }

  private static Feature feature(String id, String value) {
    return new Feature()
        .withId(id)
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)))
        .withProperties(new Properties().with("val", value));
  }

  /** The feature IDs the branch returns, with their values, so a changed value is caught too. */
  private Map<String, Object> branchContent() throws Exception {
    return readFeaturesFromBranch(SPACE_ID, BRANCH).getFeatures().stream()
        .collect(Collectors.toMap(Feature::getId, f -> f.getProperties().get("val")));
  }

  private ValidatableResponse deleteChangesetsBelow(long version) {
    return given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID + "/changesets?version=lt=" + version)
        .then();
  }
}
