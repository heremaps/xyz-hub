/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_ALL;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import io.restassured.http.ContentType;
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
    removeSpace(getSpaceId());
    createSpaceWithVersionsToKeep(getSpaceId(), 1000);
  }

  @After
  public void after() {
    removeSpace(getSpaceId());
  }

  public void addDefaultFeature() {
    writeFeature(TEST_FEATURE, "create", "patch", false);
  }

  @Test
  public void testCreateWithoutBaseVersion() {
    writeFeature(TEST_FEATURE, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testCreateFeatureWithNotExistingVersion() {
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(999));

    writeFeature(feature, "create", "patch", true)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1));
  }

  @Test
  public void testNamespacePropertiesCorrect() {
    writeFeature(TEST_FEATURE, "create", "patch", false)
        .statusCode(OK.code())
        .body("features[0].properties.'@ns:com:here:xyz'.size()", equalTo(4))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(1))
        .body("features[0].properties.'@ns:com:here:xyz'.createdAt", notNullValue())
        .body("features[0].properties.'@ns:com:here:xyz'.updatedAt", notNullValue())
        .body("features[0].properties.'@ns:com:here:xyz'.author", equalTo(ACCESS_ALL.payload.aid));
  }

  @Test
  public void testPatchHead() {
    addDefaultFeature();
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1));
    feature.getProperties().put("key2", "value2");

    writeFeature(feature, "create", "patch", true)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testPatchWithoutBaseVersion() {
    addDefaultFeature();
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");

    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testPatchWithStaleBaseVersion() {
    addDefaultFeature();

    //Update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    //Update again with base version 1 and a non-conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1));
    feature2.getProperties().put("key3", "value3");
    writeFeature(feature2, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.key3", equalTo("value3"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(3));
  }

  @Test
  public void testPatchStaleBaseVersionWithAttributeConflict() {
    addDefaultFeature();
    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    //Update again with base version 0 and a conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1));
    feature2.getProperties().put("key2", "value3");
    writeFeature(feature2, "create", "patch", true)
        .statusCode(CONFLICT.code());
  }

  @Test
  public void testMergeHead() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    feature.getProperties().setXyzNamespace(new XyzNamespace().withVersion(1));

    writeFeature(feature, "create", "merge", true)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testMergeWithoutBaseVersion() {
    addDefaultFeature();
    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");

    writeFeature(feature, "create", "merge", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testMergeWithStaleBaseVersion() {
    addDefaultFeature();

    //Update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    //Update again with base version 1 and a non-conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1));
    feature2.getProperties().put("key3", "value3");
    writeFeature(feature2, "create", "merge", true)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.key3", equalTo("value3"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(3));
  }

  @Test
  public void testMergeWithStaleBaseVersionAndAttributeConflict() {
    addDefaultFeature();

    //Update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    //Update again with base version 1 and a conflicting change
    Feature feature2 = TEST_FEATURE.copy();
    feature2.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1));
    feature2.getProperties().put("key2", "value3");
    writeFeature(feature2, "create", "merge", true)
        .statusCode(CONFLICT.code());
  }

  @Test
  public void testReplaceHead() {
    addDefaultFeature();

    // replace
    Feature feature = new Feature().withId("f1").withProperties(new Properties().with("key2", "value2").withXyzNamespace(new XyzNamespace().withVersion(1)));
    writeFeature(feature, "create", "replace", true)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", nullValue())
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testReplaceWithoutBaseVersion() {
    addDefaultFeature();

    // replace
    Feature feature = new Feature().withId("f1").withProperties(new Properties().with("key2", "value2"));
    writeFeature(feature, "create", "replace", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", nullValue())
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));
  }

  @Test
  public void testReplaceWithStaleBaseVersion() {
    addDefaultFeature();

    // update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    // replace
    Feature feature2 = new Feature().withId("f1").withProperties(new Properties().withXyzNamespace(new XyzNamespace().withVersion(1)));
    writeFeature(feature2, "create", "replace", true)
        .statusCode(CONFLICT.code());
  }

  @Test
  public void delete() {
    addDefaultFeature();

    //Update
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(2));

    //Delete
    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+ getSpaceId() +"/features/f1")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void deleteAndRecreate() {
    addDefaultFeature();

    //Delete
    given()
        .contentType(APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+ getSpaceId() +"/features/f1")
        .then()
        .statusCode(NO_CONTENT.code());

    //Create
    Feature feature = TEST_FEATURE.copy();
    feature.getProperties().put("key2", "value2");
    writeFeature(feature, "create", "patch", false)
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.key1", equalTo("value1"))
        .body("features[0].properties.key2", equalTo("value2"))
        .body("features[0].properties.'@ns:com:here:xyz'.version", equalTo(3));
  }
}
