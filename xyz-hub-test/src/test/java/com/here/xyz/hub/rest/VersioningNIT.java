/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningNIT extends TestSpaceWithFeature {
  final String SPACE_WITH_VERSIONING = "spacev2k1000";
  final Feature DEFAULT_FEATURE_PAYLOAD =  new Feature().withId("f1")
      .withGeometry(new Point().withCoordinates(new PointCoordinates(0,0)))
      .withProperties(new Properties().with("key1", "value1"));
  final FeatureCollection DEFAULT_FC_PAYLOAD =  new FeatureCollection().withFeatures(Arrays.asList(DEFAULT_FEATURE_PAYLOAD));

  @Before
  public void setup() {
    remove();
    createSpaceWithVersionsToKeep(SPACE_WITH_VERSIONING, 1000);
  }
  public void createSpaceWithVersionsToKeep(String spaceId, int versionsToKeep) {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\""+spaceId+"\",\"title\":\"" + spaceId + "\",\"versionsToKeep\":"+versionsToKeep+"}")
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());
  }

  public void addDefaultFeature(){
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(DEFAULT_FC_PAYLOAD.toString())
        .when()
        .post(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features");
  }

  @After
  public void tearDown() {
    removeSpace(SPACE_WITH_VERSIONING);
  }

  @Test
  public void testCreatePOST() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(DEFAULT_FC_PAYLOAD.toString())
        .when()
        .post(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void testCreatePUT() {
    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(DEFAULT_FC_PAYLOAD.toString())
        .when()
        .put(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void testCreatePOSTwithVersion() throws JsonProcessingException {
    FeatureCollection fc = DEFAULT_FC_PAYLOAD.copy();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(100);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(fc.toString())
        .when()
        .post(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void testCreatePUTwithVersion() throws JsonProcessingException {
    FeatureCollection fc = DEFAULT_FC_PAYLOAD.copy();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(100);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(fc.toString())
        .when()
        .put(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));;
  }

  @Test
  public void testNamespacePropertiesCorrect() throws JsonProcessingException {
    FeatureCollection fc = DEFAULT_FC_PAYLOAD.copy();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(100);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(fc.toString())
        .when()
        .put(getSpacesPath() + "/"+ SPACE_WITH_VERSIONING +"/features")
        .then()
        .statusCode(OK.code())
        .body("features[0].properties.'@ns:com:here:xyz'.size()", equalTo(3))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0))
        .body("features[0].properties.'@ns:com:here:xyz'.createdAt", notNullValue())
        .body("features[0].properties.'@ns:com:here:xyz'.updatedAt", notNullValue());
  }

  @Test
  public void testPatchHead() throws JsonProcessingException {
    addDefaultFeature();
    FeatureCollection fc = DEFAULT_FC_PAYLOAD.copy();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(0);
  }

  public void testPatchNoVersion() throws JsonProcessingException {
  }

  public void testPatchOldVersion() throws JsonProcessingException {
  }

  public void testPatchOldVersionWithConflict() throws JsonProcessingException {
  }

  public void testMergeHead() throws JsonProcessingException {
  }

  public void testMergeNoVersion() throws JsonProcessingException {
  }

  public void testMergeOldVersion() throws JsonProcessingException {
  }

  public void testMergeOldVersionWithConflict() throws JsonProcessingException {
  }

  public void testReplaceHead() throws JsonProcessingException {
  }

  public void testReplaceNoVersion() throws JsonProcessingException {
  }

  public void testReplaceOldVersion() throws JsonProcessingException {
  }

  public void delete() throws JsonProcessingException {
  }

  public void deleteAndRecreate() throws JsonProcessingException {
  }
}
