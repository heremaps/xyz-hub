/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.jayway.restassured.response.ValidatableResponse;
import com.jayway.restassured.specification.RequestSpecification;

import java.util.*;

import groovy.json.JsonBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ModifySpaceWithUUID extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    final ValidatableResponse response = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSpaceWithUUID.json"))
        .when().post("/spaces").then();

    response.statusCode(OK.code())
        .body("id", equalTo("x-psql-test"))
        .body("title", equalTo("My Demo Space"))
        .body("storage.id", equalTo("psql"));
  }

  public RequestSpecification createRequest(FeatureCollection featureCollection) {
    return given()
        .accept(APPLICATION_GEO_JSON)
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(featureCollection.serialize())
        .when();
  }

  @After
  public void tearDown() {
    remove();
  }

  private FeatureCollection prepareUpdate() throws JsonProcessingException {
    String body = createRequest(generateRandomFeatures(1, 8))
        .post("/spaces/x-psql-test/features")
        .getBody().asString();
    FeatureCollection fc = XyzSerializable.deserialize(body);
    fc.getFeatures().get(0).getProperties().put("test", 1);
    return fc;
  }

  private FeatureCollection prepareUpdateWithUUID(String uuid ) throws JsonProcessingException {
    FeatureCollection fc = prepareUpdate();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setUuid(uuid);
    return fc;
  }

  @Test
  public void checkUUIDExists() {
    createRequest(generateRandomFeatures(1, 8))
        .post("/spaces/x-psql-test/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.uuid", notNullValue());
  }

  @Test
  public void replaceWithUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(null))
        .put("/spaces/x-psql-test/features")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }

  @Test
  public void replaceWithWrongUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(UUID.randomUUID().toString()))
        .put("/spaces/x-psql-test/features")
        .then().statusCode(CONFLICT.code());
  }

  @Test
  public void replaceWithoutUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(null))
        .post("/spaces/x-psql-test/features")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }


  @Test
  public void mergeWithUUID() throws JsonProcessingException {
    createRequest(prepareUpdate())
        .post("/spaces/x-psql-test/features?e=merge")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }

  @Test
  public void mergeWithWrongUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(UUID.randomUUID().toString()))
        .post("/spaces/x-psql-test/features?e=merge")
        .then().statusCode(CONFLICT.code());
  }

  @Test
  public void mergeWithoutUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(null))
        .post("/spaces/x-psql-test/features?e=merge")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }

  @Test
  public void patchWithUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(null))
        .post("/spaces/x-psql-test/features")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }

  @Test
  public void patchWithWrongUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(UUID.randomUUID().toString()))
        .post("/spaces/x-psql-test/features")
        .then().statusCode(CONFLICT.code());
  }

  @Test
  public void patchWithoutUUID() throws JsonProcessingException {
    createRequest(prepareUpdateWithUUID(null))
        .post("/spaces/x-psql-test/features")
        .then().statusCode(OK.code())
        .body("features[0].properties.test", equalTo(1));
  }

  @Test
  public void testImmutableUUID() throws JsonProcessingException {
    given().contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
            .body(content("/xyz/hub/updateSpaceWithDisabledUUID.json"))
            .when().patch("/spaces/x-psql-test").then()
            .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testMergeWithHistory() throws JsonProcessingException {
    /** Keep 5 versions of objects */
    patchSpaceWithMaxVersionCount(5,AuthProfile.ACCESS_ALL);
    String body = createRequest(generateRandomFeatures(1, 2))
            .post("/spaces/x-psql-test/features")
            .getBody().asString();
    FeatureCollection fc = XyzSerializable.deserialize(body);

    Map<Integer, List<Feature>> featureHistory = produceUpdates(fc,10);
    countFeatures(1);

    /** Take 5th version of Object for doing an update on */
    Feature oldVersion = featureHistory.get(5).get(0);

    XyzNamespace xyzNamespace = oldVersion.getProperties().getXyzNamespace();
    /** try to add a new property on the oldVersion */
    oldVersion.withProperties(new Properties().with("foo_new" , "bar" ).with("foo",5).withXyzNamespace(xyzNamespace));

    final ValidatableResponse response = createRequest(new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(oldVersion);}}))
            .when().post("/spaces/x-psql-test/features?e=merge")
            .then();

    /** Check the merged response */
    response.statusCode(OK.code());
  }

  @Test
  public void testPatchConflictWithHistory() throws JsonProcessingException {
    /** Keep 5 versions of objects */
    patchSpaceWithMaxVersionCount(5,AuthProfile.ACCESS_ALL);
    String body = createRequest(generateRandomFeatures(1, 2))
            .post("/spaces/x-psql-test/features")
            .getBody().asString();
    FeatureCollection fc = XyzSerializable.deserialize(body);

    Map<Integer, List<Feature>> featureHistory = produceUpdates(fc,10);
    countFeatures(1);

    /** Take 5th version of Object for doing an update on */
    Feature oldVersion = featureHistory.get(5).get(0);

    XyzNamespace xyzNamespace = oldVersion.getProperties().getXyzNamespace();
    /** try to modify property which has changed in head */
    oldVersion.withProperties(new Properties().with("foo" , "barConflict" ).withXyzNamespace(xyzNamespace));

    final ValidatableResponse response = createRequest(new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(oldVersion);}}))
            .when().post("/spaces/x-psql-test/features?e=patch")
            .then();

    /** Check the merged response */
    response.statusCode(CONFLICT.code());
  }

  @Test
  public void testPatchWithHistory() throws JsonProcessingException {
    /** Keep 5 versions of objects */
    patchSpaceWithMaxVersionCount(5,AuthProfile.ACCESS_ALL);
    String body = createRequest(generateRandomFeatures(1, 2))
            .post("/spaces/x-psql-test/features")
            .getBody().asString();
    FeatureCollection fc = XyzSerializable.deserialize(body);

    Map<Integer, List<Feature>> featureHistory = produceUpdates(fc,10);
    countFeatures(1);

    /** Take 5th version of Object for doing an update on */
    Feature headVersion = featureHistory.get(10).get(0);

    /** Take 5th version of Object for doing an update on */
    Feature oldVersion = featureHistory.get(5).get(0);

    XyzNamespace xyzNamespace = oldVersion.getProperties().getXyzNamespace();
    oldVersion.withProperties(new Properties().with("foo2" , "bar" ).withXyzNamespace(xyzNamespace));

    final ValidatableResponse response = createRequest(new FeatureCollection().withFeatures(new ArrayList<Feature>(){{add(oldVersion);}}))
            .when().post("/spaces/x-psql-test/features?e=patch")
            .then();

    /** Check the merged response */
    response.statusCode(OK.code())
            .body("updated",equalTo(Arrays.asList(oldVersion.getId())))
            .body("features[0].properties.@ns:com:here:xyz.muuid", equalTo(oldVersion.getProperties().getXyzNamespace().getUuid()))
            .body("features[0].properties.@ns:com:here:xyz.puuid", equalTo(headVersion.getProperties().getXyzNamespace().getUuid()))
            .body("features[0].properties.foo", equalTo(10))
            .body("features[0].properties.foo2", equalTo("bar"))
            .body("features[0].id",equalTo(oldVersion.getId()));

//    fc = XyzSerializable.deserialize(response.extract().asString());
  }

  private void patchSpaceWithMaxVersionCount(int maxVersionCount, AuthProfile profile){
    String body = "{\"storage\": {\"id\": \"psql\",\"params\": { \"maxVersionCount\" : "+maxVersionCount+" }}}";
    final ValidatableResponse response = given()
            .contentType(APPLICATION_JSON)
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(profile))
            .body(body)
            .when().patch("/spaces/x-psql-test").then();

    response.statusCode(OK.code())
            .body("storage.params.maxVersionCount", equalTo(maxVersionCount));
  }

  private Map<Integer,List<Feature>> produceUpdates(FeatureCollection fc , int updateCnt) throws JsonProcessingException {
    Map<Integer,List<Feature>> featureHistory = new HashMap<>();
    featureHistory.put(0,fc.getFeatures());

    /** Update Object 10 times */
    for (int i = 1; i <= updateCnt; i++) {
      fc.getFeatures().get(0).getProperties().put("foo" , i );
      String body = createRequest(fc)
              .post("/spaces/x-psql-test/features")
              .getBody().asString();
      fc = XyzSerializable.deserialize(body);
      featureHistory.put(i,fc.getFeatures());
    }
    return featureHistory;
  }
}
