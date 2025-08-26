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

import com.google.common.base.Strings;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;

public class TestSpaceBranch extends TestSpaceWithFeature {

  protected ValidatableResponse createBranch(String spaceId, String branchId, String ref) {
    return given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body("{\"id\":\""+branchId+"\",\"baseRef\":\""+ref+"\"}")
            .when()
            .post(getSpaceBranchPath(spaceId, null))
            .then()
            .statusCode(OK.code());
  }

  protected JsonArray getBranches(String spaceId) {
    return given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .when()
            .get(getSpaceBranchPath(spaceId, null))
            .body().as(JsonArray.class);
  }

  protected void deleteBranch(String spaceId, String branchId) {
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .when()
            .delete(getSpaceBranchPath(spaceId, branchId))
            .then()
            .statusCode(OK.code());
  }

  protected void rebaseBranch(String spaceId, String branchId, String newRef) {
    given()
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
            .postFeatures(spaceId, featureCollection, Map.of("versionRef", branchId));
  }

  protected FeatureCollection addFeatureToBranch(String spaceId, String branchId, Feature feature) throws WebClientException {
    return addFeaturesToBranch(spaceId, branchId, new FeatureCollection().withFeatures(List.of(feature)));
  }

  protected FeatureCollection readFeaturesFromBranch(String spaceId, String branchId) throws WebClientException {
    return HubWebClient.getInstance(RestAssuredConfig.config().fullHubUri)
            .customReadFeaturesQuery(spaceId, "/iterate?versionRef=" + branchId);
  }

  protected void removeAllBranchesForSpace(String spaceId) {
    List<String> allBranchIds = getBranches(spaceId).stream()
            .map(branch -> ((JsonObject) branch).getString("id"))
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
