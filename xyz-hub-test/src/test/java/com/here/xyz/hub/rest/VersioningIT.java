/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
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
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningIT extends TestSpaceWithFeature {

  final String SPACE_ID = "x-psql-test";
  final String FEATURE_ID = "Q3495887";

  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    remove();
    createSpaceWithRevisionsToKeep(5);
    addFeatures();
    updateFeature();
  }

  public void updateFeature() {
    // update a feature
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID).withProperties(new Properties().with("name", "updated name")));
  }

  public void createSpaceWithRevisionsToKeep(int revisionsToKeep) {
    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"id\":\""+SPACE_ID+"\",\"title\":\"x-psql-test\",\"revisionsToKeep\":"+revisionsToKeep+"}")
        .when()
        .post(getCreateSpacePath())
        .then()
        .statusCode(OK.code());
  }

  @After
  public void tearDown() {
    remove();
  }

  @Test
  public void getFeatureInvalidRevisionOperator() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=xx=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=gt=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev>1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=gte=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev>=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev!=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=cs=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev@>1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteOldVersion() {
    // delete versions lower than the returned one
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+SPACE_ID+"/revisions?rev=lt=2")
        .then()
        .statusCode(OK.code());

    // get feature by id and version, expect 404
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getFeatureExpectVersionIncrease() {
    // get a feature, expect version increased
    getFeature(SPACE_ID, FEATURE_ID, OK.code(), "properties.@ns:com:here:xyz.rev", "2");
  }

  @Test
  public void getFeatureEqualsToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=eq=1")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("Stade Tata Raphaël"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=2")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+FEATURE_ID+"?rev=222")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getFeaturesEqualsToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&id=Q929126&rev=eq=1")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&id=Q929126&rev=2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&id=Q929126&rev=eq=222")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));
  }

  @Test
  public void getFeaturesUpToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&id=Q929126&rev=lt=2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&id=Q929126&rev<3")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(3));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+FEATURE_ID+"&rev=lt=3")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].id", equalTo(FEATURE_ID))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].id", equalTo(FEATURE_ID))
        .body("features[1].properties.name", equalTo("updated name"));
  }

  @Test
  public void getFeatureByAuthor() {
    // TODO
  }

  @Test
  public void getFeaturesByAuthor() {
    // TODO
  }

  @Test
  public void getFeaturesEqualsToRevisionAndAuthor() {
    // TODO
  }

  @Test
  public void getFeaturesUpToRevisionAndAuthor() {
    // TODO
  }

  @Test
  public void searchFeaturesByPropertyAndAuthor() {
    // TODO
  }
}
