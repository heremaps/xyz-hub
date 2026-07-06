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

import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_ALL;
import static com.here.xyz.models.hub.FeatureModificationList.IfExists.MERGE;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("RestTests")
public class StoreFeaturesApiIT extends TestSpaceWithFeature {

  @BeforeEach
  public void setup() {
    remove(DEFAULT_SPACE_ID);
  }

  @AfterEach
  public void tearDown() {
    remove(DEFAULT_SPACE_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void putFeatures(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .accept("application/x-empty")
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(content("/xyz/hub/processedData.json"))
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void putFeatureCollectionWithoutFeatureType(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}")
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .then()
        .statusCode(OK.code());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Disabled("transactional=false is not supported anymore")
  public void testFailureResponse(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    FeatureCollection fc = new FeatureCollection().withFeatures(Arrays.asList(Feature.createEmptyFeature().withId("T1")));

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(fc.serialize())
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .then()
        .statusCode(OK.code());

    FeatureCollection fcUpdate = new FeatureCollection().withFeatures(Arrays.asList(Feature.createEmptyFeature().withId("A1"),
        Feature.createEmptyFeature().withId("T1")));

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body(fcUpdate.serialize())
        .when()
        .post(getSpacesPath() + "/x-psql-test/features?ne=retain&e=error&transactional=false")
        .then()
        .statusCode(OK.code())
        .body("failed[0].id", equalTo("T1"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void putFeaturesCheckCustomSizeLimit(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    given()
        .contentType(APPLICATION_GEO_JSON)
        .accept("application/x-empty")
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .headers(new HashMap<String, String>() {{
          put("X-Upload-Content-Length-Limit", "1");
        }})
        .body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}")
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .then()
        .statusCode(REQUEST_ENTITY_TOO_LARGE.code());

    given()
        .contentType(APPLICATION_GEO_JSON)
        .accept("application/x-empty")
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .body("{\"features\":[{\"geometry\":{\"coordinates\":[-2.960777,53.430777],\"type\":\"Point\"}}],\"type\":\"FeatureCollection\"}")
        .when()
        .put(getSpacesPath() + "/x-psql-test/features")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void overwriteFeatureGeometry(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    LineStringCoordinates coords = lineStringCoords();
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new Point().withCoordinates(new PointCoordinates(1,1))), ACCESS_ALL);
    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates", equalTo(List.of(1f, 1f, 0f)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void patchFeatureGeometry(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    LineStringCoordinates coords = lineStringCoords();
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);

    coords.get(0).set(0, 7.23);
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);
    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates[0]", equalTo(List.of(7.23f, 50.01f, 0f)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void patchFeatureGeometryAddPoint(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    LineStringCoordinates coords = lineStringCoords();
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);

    coords.add(new Position(7.23, 50.01));
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);
    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates[2]", equalTo(List.of(7.23f, 50.01f, 0f)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void patchFeatureGeometryRemovePoint(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    LineStringCoordinates coords = lineStringCoords();
    coords.add(new Position(7.23, 50.01));
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);
    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates.size()", equalTo(3));

    coords.remove(coords.size() - 1);
    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL);
    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates[1]", equalTo(List.of(7.19f, 50.01f, 0f)))
        .body("geometry.coordinates.size()", equalTo(2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void merge(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    postFeature(DEFAULT_SPACE_ID, newFeature()
        .withProperties(new Properties().with("myProp1", "someValue"))
        .withGeometry(new LineString().withCoordinates(lineStringCoords())), ACCESS_ALL);

    //Edit first attribute
    postFeature(DEFAULT_SPACE_ID, newFeature()
        .withProperties(new Properties()
            .with("myProp1", "someNewValue")
            .withXyzNamespace(new XyzNamespace().withVersion(1))), ACCESS_ALL, true);

    //Add 2nd attribute while keeping the original value of the first attribute
    postFeature(DEFAULT_SPACE_ID, newFeature()
        .withProperties(new Properties()
            .with("myProp1", "someValue")
            .with("myProp2", "otherValue")
            .withXyzNamespace(new XyzNamespace().withVersion(1))), ACCESS_ALL, true, MERGE, historyActive ? OK : CONFLICT);

    //NOTE: Merging is only possible when history is activated on a space
    if (historyActive) {
      //First attribute value should not have been overwritten by 2nd POST
      getFeature(DEFAULT_SPACE_ID, newFeature().getId())
          .body("id", equalTo(newFeature().getId()))
          .body("properties.myProp1", equalTo("someNewValue"))
          .body("properties.myProp2", equalTo("otherValue"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void mergeFeatureGeometry(boolean historyActive) {
    createSpace(DEFAULT_SPACE_ID, historyActive);

    postFeature(DEFAULT_SPACE_ID, newFeature().withGeometry(new LineString().withCoordinates(lineStringCoords())), ACCESS_ALL);

    //Edit first point
    LineStringCoordinates coords = lineStringCoords();
    coords.get(0).set(0, 7.10);
    postFeature(DEFAULT_SPACE_ID, newFeature()
        .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withVersion(1)))
        .withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL, true);

    //Add a point at the end
    coords = lineStringCoords();
    coords.add(new Position(7.23, 50.01));
    postFeature(DEFAULT_SPACE_ID, newFeature()
        .withProperties(new Properties().withXyzNamespace(new XyzNamespace().withVersion(1)))
        .withGeometry(new LineString().withCoordinates(coords)), ACCESS_ALL, true, MERGE, CONFLICT);

    getFeature(DEFAULT_SPACE_ID, newFeature().getId())
        .body("id", equalTo(newFeature().getId()))
        .body("geometry.coordinates[0]", equalTo(List.of(7.10f, 50.01f, 0f)))
        .body("geometry.coordinates[2]", equalTo(null));
  }

  private static LineStringCoordinates lineStringCoords() {
    LineStringCoordinates coords = new LineStringCoordinates();
    coords.add(new Position(7.16, 50.01));
    coords.add(new Position(7.19, 50.01));
    return coords;
  }
}
