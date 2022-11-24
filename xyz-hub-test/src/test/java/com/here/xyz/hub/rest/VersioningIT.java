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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
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
  final String FEATURE_ID_1 = "Q3495887";
  final String FEATURE_ID_2 = "Q929126";
  final String FEATURE_ID_3 = "Q1370732";
  final String USER_1= "XYZ-01234567-89ab-cdef-0123-456789aUSER1";
  final String USER_2= "XYZ-01234567-89ab-cdef-0123-456789aUSER2";

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
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "updated name")));
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
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=xx=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=gt=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision>1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=gte=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision>=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision!=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=cs=1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision@>1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteOldVersion() {
    // delete versions lower than the returned one
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/"+SPACE_ID+"/revisions?revision=lt=2")
        .then()
        .statusCode(OK.code());

    // get feature by id and version, expect 404
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getFeatureExpectVersionIncrease() {
    // get a feature, expect version increased
    getFeature(SPACE_ID, FEATURE_ID_1, OK.code(), "properties.@ns:com:here:xyz.revision", "2");
  }

  @Test
  public void getFeatureEqualsToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=eq=1")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("Stade Tata Raphaël"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=2")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?revision=222")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getFeaturesEqualsToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&revision=eq=1")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&revision=2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&revision=eq=222")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));
  }

  @Test
  public void getFeaturesUpToRevision() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&revision=lt=2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&revision<3")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(3));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&revision=lt=3")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].id", equalTo(FEATURE_ID_1))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].id", equalTo(FEATURE_ID_1))
        .body("features[1].properties.name", equalTo("updated name"));
  }

  @Test
  public void getFeatureByAuthor() {
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(FEATURE_ID_1))
        .body("properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_1 +"?author="+USER_2)
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_2 +"?author="+USER_1)
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features/"+ FEATURE_ID_2 +"?author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(FEATURE_ID_2))
        .body("properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesByAuthor() {
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_1))
        .body("features[0].properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_2 +"&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesEqualsToRevisionAndAuthor() {
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=1&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(3))
        .body("features[0].id", equalTo(FEATURE_ID_1))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[1].id", equalTo(FEATURE_ID_2))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"))
        .body("features[2].id", equalTo(FEATURE_ID_3))
        .body("features[2].properties.name", equalTo("Estádio Olímpico do Pará"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=2&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].id", equalTo(FEATURE_ID_1))
        .body("features[0].properties.name", equalTo("updated name"))
        .body("features[1].id", equalTo(FEATURE_ID_2))
        .body("features[1].properties.name", equalTo("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=1&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=555&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=2&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision=3&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_3))
        .body("features[0].properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesUpToRevisionAndAuthor() {
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision<999&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(5));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision<999&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.name", equalTo("second feature"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision<3&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(4));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&revision<3&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));
  }

  @Test
  public void searchFeaturesByPropertyAndAuthor() {
    postFeature(SPACE_ID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("capacity", 58505)), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/search?f.revision=1&f.author="+USER_1+"&p.capacity=58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/search?f.revision=2&f.author="+USER_1+"&p.capacity=58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/search?f.revision=2&f.author="+USER_2+"&p.capacity>58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/search?f.revision=lt=10&f.author="+USER_2+"&p.capacity=58505")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+SPACE_ID+"/search?f.id="+FEATURE_ID_2+"&f.revision=lt=10&f.author="+USER_1+"&f.author="+USER_2+"&p.capacity<=58505")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58500))
        .body("features[0].properties.@ns:com:here:xyz.revision", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505))
        .body("features[0].properties.@ns:com:here:xyz.revision", equalTo(2));
  }
}
