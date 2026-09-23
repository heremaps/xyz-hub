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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BranchBaseVersionPurgeProtectionIT extends TestSpaceBranch {

  private static final String BRANCH = "b1main";
  private long branchBase;

  @BeforeEach
  public void setup() throws Exception {
    createSpaceWithVersionsToKeep(SPACE_ID, 10000);
    //Update one feature repeatedly, so every write supersedes a row and the purge has something to delete
    for (int i = 1; i <= 5; i++) {
      FeatureCollection written = addFeatureToBranch(SPACE_ID, null, feature("v" + i));
      if (i == 3)
        branchBase = written.getFeatures().get(0).getProperties().getXyzNamespace().getVersion();
    }
    createBranch(SPACE_ID, BRANCH, "main:" + branchBase);
  }

  @AfterEach
  public void cleanUp() {
    removeSpace(SPACE_ID);
  }

  @Test
  public void deletingChangesetsBelowTheBranchBaseIsRejectedUntilTheBranchIsDeleted() throws Exception {
    //Try to delete changesets below the branch base, which should be rejected
    deleteChangesetsBelow(branchBase + 2)
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", containsString("branch " + BRANCH));
    assertEquals("v3", branchValue());

    //Delete the branch and try again, which should now succeed
    deleteBranch(SPACE_ID, BRANCH);
    //Now deleting changesets below the branch base should succeed
    deleteChangesetsBelow(branchBase + 2).statusCode(NO_CONTENT.code());
  }

  @Test
  public void deletingChangesetsUpToTheBranchBaseIsAllowed() throws Exception {
    //Try to delete changesets up to the branch base, which should be allowed
    deleteChangesetsBelow(branchBase).statusCode(NO_CONTENT.code());

    //Check that the branch value is still as expected
    assertEquals("v3", branchValue());
  }

  @Test
  public void theBranchBaseIsReportedAsTheOldestAvailableVersion() {
    //Set the versionsToKeep to 2, which should purge all changesets below the branch base
    patchSpace(SPACE_ID, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());
    //Check that the branch base is reported as the oldest available version
    given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + SPACE_ID + "/statistics")
        .then().statusCode(OK.code())
        .body("minVersion.value", equalTo((int) branchBase));
  }

  private static Feature feature(String value) {
    return new Feature()
        .withId("f1")
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)))
        .withProperties(new Properties().with("val", value));
  }

  private Object branchValue() throws Exception {
    return readFeaturesFromBranch(SPACE_ID, BRANCH).getFeatures().get(0).getProperties().get("val");
  }

  private ValidatableResponse deleteChangesetsBelow(long version) {
    return given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().delete(getSpacesPath() + "/" + SPACE_ID + "/changesets?version=lt=" + version)
        .then();
  }
}
