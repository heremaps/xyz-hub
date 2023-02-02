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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.config.EncoderConfig;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.ValidatableResponse;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningNIT extends TestSpaceWithFeature {

  String SPACE_ID = "spacev2k1000";
  final static Feature TEST_FEATURE =  new Feature().withId("f1")
      .withGeometry(new Point().withCoordinates(new PointCoordinates(0,0)))
      .withProperties(new Properties().with("key1", "value1"));

  @BeforeClass
  public static void beforeClass() {
    RestAssured.config = RestAssured.config().encoderConfig(EncoderConfig.encoderConfig().encodeContentTypeAs("application/vnd.here.feature-modification-list",
        ContentType.JSON));
  }

  @Before
  public void before() {
    removeSpace(SPACE_ID);
    createSpaceWithVersionsToKeep("spacev2k1000", 1000);
  }

  @After
  public void after() {
    removeSpace(SPACE_ID);
  }

  public String constructPayload(Feature feature, String ifNotExists, String ifExists) {
    return "{"
        + "    \"type\": \"FeatureModificationList\","
        + "    \"modifications\": ["
        + "        {"
        + "            \"type\": \"FeatureModification\","
        + "            \"onFeatureNotExists\": \""+ifNotExists+"\","
        + "            \"onFeatureExists\": \""+ifExists+"\","
        + "            \"featureData\": " + new FeatureCollection().withFeatures(Collections.singletonList(feature)).serialize()
        + "        }"
        + "    ]"
        + "}";
  }

  public ValidatableResponse write(Feature feature, String ifNotExists, String ifExists){
    return given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(feature, ifNotExists, ifExists))
        .when().post(getSpacesPath() + "/"+ SPACE_ID +"/features").then();
  }

  public void addDefaultFeature(){
    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(TEST_FEATURE, "create", "patch"))
        .when()
        .post(getSpacesPath() + "/"+ SPACE_ID +"/features");
  }

  public void createSpaceWithVersionsToKeep(String spaceId, int versionsToKeep) {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\""+spaceId+"\",\"title\":\"" + spaceId + "\",\"versionsToKeep\":"+versionsToKeep+",\"enableUUID\":\"true\"}")
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void testCreateNoVersion() {
    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(TEST_FEATURE, "create", "patch"))
        .when()
        .post(getSpacesPath() + "/"+ SPACE_ID +"/features")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void testCreateWithNotExistingVersion() {
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(999));

    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0));
  }

  @Test
  public void testNamespacePropertiesCorrect() {
    write(TEST_FEATURE, "create", "patch")
        .statusCode(OK.code())
        .body("features[0].properties.'@ns:com:here:xyz'.size()", equalTo(6))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(0))
        .body("features[0].properties.'@ns:com:here:xyz'.createdAt", notNullValue())
        .body("features[0].properties.'@ns:com:here:xyz'.updatedAt", notNullValue());
  }

  @Test
  public void testPatchHead() {
    addDefaultFeature();
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(0));
    feature.getProperties().put("key2", "value2");

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(feature, "create", "patch"))
        .when().post(getSpacesPath() + "/"+ SPACE_ID +"/features").then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testPatchNoVersion() {
    addDefaultFeature();
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(constructPayload(feature, "create", "patch"))
        .when().post(getSpacesPath() + "/"+ SPACE_ID +"/features").then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testPatchOldVersion() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    // update again with "base" version 0 and a non-conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(0));
    feature2.getProperties().put("key3", "value3");
    write(feature2, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.key3", equalTo("value3"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testPatchOldVersionWithConflict() {
    addDefaultFeature();
    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    // update again with "base" version 0 and a conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(0));
    feature2.getProperties().put("key2", "value3");
    write(feature2, "create", "patch")
        .statusCode(CONFLICT.code());
  }

  @Test
  public void testMergeHead() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    feature.getProperties().put("version", 0);

    write(feature, "create", "merge")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testMergeNoVersion() {
    addDefaultFeature();
    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");

    write(feature, "create", "merge")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testMergeOldVersion() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    // update again with "base" version 0 and a non-conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(0));
    feature2.getProperties().put("key3", "value3");
    write(feature2, "create", "merge")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.key3", equalTo("value3"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testMergeOldVersionWithConflict() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    // update again with "base" version 0 and a non-conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(0));
    feature2.getProperties().put("key2", "value3");
    write(feature2, "create", "merge")
        .statusCode(CONFLICT.code());
  }

  @Test
  public void testReplaceHead() {
    addDefaultFeature();

    // replace
    Feature feature = new Feature().withId("f1").withProperties(new Properties().with("key2", "value2").withXyzNamespace(new XyzNamespace().withVersion(0)));
    write(feature, "create", "replace")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", nullValue())
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testReplaceNoVersion() {
    addDefaultFeature();

    // replace
    Feature feature = new Feature().withId("f1").withProperties(new Properties().with("key2", "value2"));
    write(feature, "create", "replace")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", nullValue())
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testReplaceOldVersion() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    // replace
    Feature feature2 = new Feature().withId("f1").withProperties(new Properties().withXyzNamespace(new XyzNamespace().withVersion(0)));
    write(feature2, "create", "replace")
        .statusCode(CONFLICT.code());
  }

  @Test
  public void delete() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+ SPACE_ID +"/features/f1")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void deleteAndRecreate() {
    addDefaultFeature();

    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+ SPACE_ID +"/features/f1")
        .then()
        .statusCode(NO_CONTENT.code());

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    write(feature, "create", "patch")
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }
}
