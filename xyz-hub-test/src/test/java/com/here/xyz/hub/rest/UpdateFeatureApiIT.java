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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class UpdateFeatureApiIT extends TestSpaceWithFeature {

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    createSpace();
    addFeatures();
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void postFeatureWithNumberId() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/featureWithNumberId.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code()).
        body("features[0].id", equalTo("1234"));
  }

  @Test
  public void postFeatureWithWrongType() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/wrongType.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(BAD_REQUEST.code());
  }

  @Test
  public void updateFeatureById_put() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        put(getSpacesPath() + "/x-psql-test/features/Q2838923?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code()).
        body("id", equalTo("Q2838923")).
        body("properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("properties.occupant", equalTo("National University of San Marcos Updated")).
        body("properties.sport", equalTo("association baseball")).
        body("properties.capacity", equalTo(67470)).
        body("properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }


  @Test
  public void updateNonExistingFeatureById_put() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        put(getSpacesPath() + "/x-psql-test/features/Q2838924?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code()).
        body("id", equalTo("Q2838924")).
        body("properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("properties.occupant", equalTo("National University of San Marcos Updated")).
        body("properties.sport", equalTo("association baseball")).
        body("properties.capacity", equalTo(67470)).
        body("properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void updateNonExistingFeatureByIdWithPrefix_put() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        put(getSpacesPath() + "/x-psql-test/features/Q2838924?addTags=baseball&removeTags=soccer&prefixId=foo:").
        then().
        statusCode(OK.code()).
        body("id", equalTo("foo:Q2838924")).
        body("properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("properties.occupant", equalTo("National University of San Marcos Updated")).
        body("properties.sport", equalTo("association baseball")).
        body("properties.capacity", equalTo(67470)).
        body("properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void updateNonExistingSpace_put() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        put(getSpacesPath() + "/x-psql-dummy/features/Q2838925").
        then().
        statusCode(NOT_FOUND.code());
  }

  @Test
  public void updateFeatureById_post() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeatureById.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code()).
        body("features[0].id", equalTo("Q271454")).
        body("features[0].properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("features[0].properties.occupant", equalTo("National University of San Marcos Updated")).
        body("features[0].properties.sport", equalTo("association baseball")).
        body("features[0].properties.capacity", equalTo(67470)).
        body("features[0].properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void updateFeatureByIdWithPrefix_post() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/updateFeatureById.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball&removeTags=soccer&prefixId=foo:").
        then().
        statusCode(OK.code()).
        body("features[0].id", equalTo("foo:Q271454")).
        body("features[0].properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("features[0].properties.occupant", equalTo("National University of San Marcos Updated")).
        body("features[0].properties.sport", equalTo("association baseball")).
        body("features[0].properties.capacity", equalTo(67470)).
        body("features[0].properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void updateNonExistingSpace_post() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/createFeatureById.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code()).
        body("features[0].id", equalTo("Q271455")).
        body("features[0].properties.name", equalTo("Estadio Universidad San Marcos Updated")).
        body("features[0].properties.occupant", equalTo("National University of San Marcos Updated")).
        body("features[0].properties.sport", equalTo("association baseball")).
        body("features[0].properties.capacity", equalTo(67470)).
        body("features[0].properties.'@ns:com:here:xyz'.tags", hasItems("stadium", "baseball"));
  }

  @Test
  public void updateFeatureById_put_WithAccessAll() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/updateFeature.json")).
        when().
        patch(getSpacesPath() + "/x-psql-test/features/Q2838923?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code());
  }

  @Test
  public void updateFeatureById_patch_WithAllAccess() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/patchFeature.json")).
        when().
        patch(getSpacesPath() + "/x-psql-test/features/Q2838923?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code());
  }

  @Test
  public void updateFeatureById_post_WithAllAccess() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(content("/xyz/hub/updateFeatureById.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball&removeTags=soccer").
        then().
        statusCode(OK.code());
  }

  @Test
  public void updateFeaturesWithEmptyFeatureCollection() {
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(content("/xyz/hub/emptyFeatureCollection.json")).
        when().
        post(getSpacesPath() + "/x-psql-test/features?addTags=baseball").
        then().
        statusCode(OK.code()).
        body("features.size()", equalTo(0));
  }

  @Test
  public void updateTheGeometryTypeWithPost() {
    Feature point = Feature.createEmptyFeature()
        .withId("A001")
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 1)));
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(point.serialize()).
        when().
        post(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());

    LineStringCoordinates coordinates = new LineStringCoordinates();
    coordinates.add(new Position(0, 0));
    coordinates.add(new Position(0, 1));

    Feature line = Feature.createEmptyFeature()
        .withId("B001")
        .withGeometry(new LineString().withCoordinates(coordinates));

    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(line.serialize()).
        when().
        post(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());
  }

  @Test
  public void updateTheGeometryTypeWithPut() {
    Feature point = Feature.createEmptyFeature()
        .withId("C001")
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 1)));
    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(point.serialize()).
        when().
        put(getSpacesPath() + "/x-psql-test/features/C001").
        then().
        statusCode(OK.code());

    LineStringCoordinates coordinates = new LineStringCoordinates();
    coordinates.add(new Position(0, 0));
    coordinates.add(new Position(0, 1));

    Feature line = Feature.createEmptyFeature()
        .withId("C001")
        .withGeometry(new LineString().withCoordinates(coordinates));

    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_ALL)).
        body(line.serialize()).
        when().
        put(getSpacesPath() + "/x-psql-test/features/C001").
        then().
        statusCode(OK.code()).
        body("geometry.type", equalTo("LineString"));
  }

  @Test
  public void validateFeature() {
    String featureId = "Q2838923";
    validateFeatureProperties(given()
        .urlEncodingEnabled(false)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get(getSpacesPath() + "/x-psql-test/features?id=" + featureId)
        .then()
        .statusCode(OK.code()), featureId);
  }

  @Test
  public void postFeatureWithVersionWithoutConflictDetection() throws Exception {
    FeatureCollection featureCollection = XyzSerializable.deserialize(content("/xyz/hub/updateFeatureById.json"));
    featureCollection.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(5);

    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(featureCollection.serialize()).
        when().
        post(getSpacesPath() + "/x-psql-test/features").
        then().
        statusCode(OK.code());
  }

  @Test
  public void patchFeatureWithVersionWithoutConflictDetection() throws Exception {
    Feature feature = XyzSerializable.deserialize(content("/xyz/hub/updateFeature.json"));
    feature.getProperties().getXyzNamespace().setVersion(5);

    given().
        accept(APPLICATION_GEO_JSON).
        contentType(APPLICATION_GEO_JSON).
        headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN)).
        body(feature.serialize()).
        when().
        patch(getSpacesPath() + "/x-psql-test/features/Q2838923").
        then().
        statusCode(OK.code());
  }
}
