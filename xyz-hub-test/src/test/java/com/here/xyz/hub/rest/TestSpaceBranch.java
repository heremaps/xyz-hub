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
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;

import com.google.common.base.Strings;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Branch;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestSpaceBranch extends TestSpaceWithFeature {

  protected static final String B1_MAIN = "b1_main";
  protected static final String B2_B1 = "b2_b1";
  protected static final String B3_B2 = "b3_b2";

  //Space ID should not contain underscore for branching to work
  protected String SPACE_ID = getClass().getSimpleName() + "-" + randomAlpha(5);

  protected ValidatableResponse createBranch(String spaceId, String branchId, String ref) {

    //TODO: Remove when service defaults to HEAD if not provided
     ref = ref == null || ref.equals("HEAD") ? "HEAD" : !ref.contains(":") ? ref + ":HEAD" : ref;

    return given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(JsonObject.of("id", branchId, "baseRef", ref).encode())
            .when()
            .post(getSpaceBranchPath(spaceId, null))
            .then()
            .statusCode(OK.code());
  }

  protected List<String> getBranchIds(String spaceId) {
    return getBranches(spaceId).stream().map(Branch::getId).toList();
  }

  protected List<Branch> getBranches(String spaceId) {
    return given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .when()
            .get(getSpaceBranchPath(spaceId, null))
            .then()
            .statusCode(OK.code())
            .extract()
            .jsonPath()
            .getList("", Branch.class);
  }

  protected void deleteBranch(String spaceId, String branchId) {
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .when()
            .delete(getSpaceBranchPath(spaceId, branchId))
            .then()
            .statusCode(NO_CONTENT.code());
  }

  protected ValidatableResponse rebaseBranch(String spaceId, String branchId, String newRef) {
    return given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body("{\"id\":\""+branchId+"\",\"baseRef\":\""+newRef+"\"}")
            .when()
            .patch(getSpaceBranchPath(spaceId, branchId))
            .then()
            .statusCode(OK.code());
  }

  protected FeatureCollection addFeaturesToBranch(String spaceId, String branchId, FeatureCollection featureCollection) throws WebClientException {
    return (FeatureCollection) HubWebClient.getInstance(RestAssuredConfig.config().fullHubUri)
            .postFeatures(spaceId, featureCollection, branchId == null ? null : Map.of("versionRef", branchId));
  }

  protected FeatureCollection addFeatureToBranch(String spaceId, String branchId, Feature feature) throws WebClientException {
    return addFeaturesToBranch(spaceId, branchId, new FeatureCollection().withFeatures(List.of(feature)));
  }

  protected FeatureCollection readFeaturesFromBranch(String spaceId, String branchId) throws WebClientException {
    String versionRef = branchId == null ? "" : "?versionRef=" + branchId;
    return HubWebClient.getInstance(RestAssuredConfig.config().fullHubUri)
            .customReadFeaturesQuery(spaceId, "/iterate" + versionRef);
  }

  protected void removeAllBranchesForSpace(String spaceId) {
    List<String> allBranchIds = getBranches(spaceId).stream()
            .map(branch -> branch.getId())
            .collect(Collectors.toList());

    for(String branchId : allBranchIds) {
      deleteBranch(spaceId, branchId);
    }
  }

  protected String getSpaceBranchPath(String spaceId, String branchId) {
    return getSpacesPath() + "/" + spaceId + "/branches" + (Strings.isNullOrEmpty(branchId) ? "" : "/" + branchId);
  }

  protected Feature createSampleFeature(String id) {
    return new Feature()
            .withId(id)
            .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)))
            .withProperties(new Properties().with("name", id));
  }

}
