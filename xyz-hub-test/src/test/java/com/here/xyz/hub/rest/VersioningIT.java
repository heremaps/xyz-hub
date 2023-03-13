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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(RestTests.class)
public class VersioningIT extends TestSpaceWithFeature {
  final String SPACE_ID_1_NO_UUID = "space1nouuid";
  final String SPACE_ID_2_UUID = "space2uuid";
  final String FEATURE_ID_1 = "Q3495887";
  final String FEATURE_ID_2 = "Q929126";
  final String FEATURE_ID_3 = "Q1370732";
  final String USER_1= "XYZ-01234567-89ab-cdef-0123-456789aUSER1";
  final String USER_2= "XYZ-01234567-89ab-cdef-0123-456789aUSER2";

  @Before
  public void setup() {
    remove();
    createSpaceWithVersionsToKeep(SPACE_ID_1_NO_UUID, 5, false);
    createSpaceWithVersionsToKeep(SPACE_ID_2_UUID, 5, true);
    addFeatures(SPACE_ID_1_NO_UUID);
    addFeatures(SPACE_ID_2_UUID);
    updateFeature(SPACE_ID_1_NO_UUID);
    updateFeature(SPACE_ID_2_UUID);
  }

  public void updateFeature(String spaceId) {
    // update a feature
    postFeature(spaceId, new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "updated name")),
        AuthProfile.ACCESS_OWNER_1_ADMIN);
  }

  @After
  public void tearDown() {
    removeSpace(SPACE_ID_1_NO_UUID);
    removeSpace(SPACE_ID_2_UUID);
  }

  @Test
  public void testTransactional() {
    Feature f1 = new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    Feature f2 = new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "non-conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Arrays.asList(f1, f2));

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(fc.toString())
        .when()
        .post(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features?transactional=true")
        .then()
        .statusCode(CONFLICT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"))
        .body("properties.@ns:com:here:xyz.version", equalTo(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features/"+ FEATURE_ID_2)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo(""))
        .body("properties.@ns:com:here:xyz.version", equalTo(0));
  }

  @Test
  public void testNonTransactional() {
    Feature f1 = new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    Feature f2 = new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "non-conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Arrays.asList(f1, f2));

    given()
        .contentType(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(fc.toString())
        .when()
        .post(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features?transactional=false")
        .then()
        .statusCode(OK.code())
        .extract().body().asString();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"))
        .body("properties.@ns:com:here:xyz.version", equalTo(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features/"+ FEATURE_ID_2)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("non-conflicting change"))
        .body("properties.@ns:com:here:xyz.version", equalTo(2));
  }

  //@Test
  //TODO fix flickering or remove it
  public void testConflictDetectionDisabled() {
    Feature f1 = new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Collections.singletonList(f1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_GEO_JSON)
        .when()
        .body(fc.toString())
        .post(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("conflicting change"))
        .body("properties.@ns:com:here:xyz.version", equalTo(2));
  }

  @Test
  public void testConflictDetectionEnabled() {
    Feature f1 = new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "conflicting change").withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Collections.singletonList(f1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_GEO_JSON)
        .when()
        .body(fc.toString())
        .post(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features")
        .then()
        .statusCode(CONFLICT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_2_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"))
        .body("properties.@ns:com:here:xyz.version", equalTo(1));
  }

  @Test
  public void testWriteWithVersionInNamespace() {
    Feature f1 = new Feature().withId("F1").withProperties(new Properties().with("name", "abc").withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Collections.singletonList(f1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_GEO_JSON)
        .when()
        .body(fc.toString())
        .post(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/F1?version=0")
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/F1")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("abc"))
        .body("properties.@ns:com:here:xyz.version", equalTo(2));
  }

  @Test
  public void testWriteWithoutVersionInNamespace() {
    Feature f1 = new Feature().withId("F1").withProperties(new Properties().with("name", "abc"));
    FeatureCollection fc = new FeatureCollection().withFeatures(Collections.singletonList(f1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_GEO_JSON)
        .when()
        .body(fc.toString())
        .post(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/F1?version=0")
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/F1")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("abc"))
        .body("properties.@ns:com:here:xyz.version", equalTo(2));
  }

  @Test
  public void testMerge() {
    Feature f1 = new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("quantity", 123).withXyzNamespace(new XyzNamespace().withVersion(0)));
    FeatureCollection fc = new FeatureCollection().withFeatures(Collections.singletonList(f1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .body(fc.toString())
        .post(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"))
        .body("properties.quantity", equalTo(123))
        .body("properties.@ns:com:here:xyz.version", equalTo(2));
  }

  @Test
  public void testGetFeaturesByIdOrderByVersionStar() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?version=*")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].properties.name", equalTo("Stade Tata Raphaël"))
        .body("features[0].properties.@ns:com:here:xyz.version", equalTo(0))
        .body("features[1].properties.name", equalTo("updated name"))
        .body("features[1].properties.@ns:com:here:xyz.version", equalTo(1));
  }

  @Test
  public void getFeatureExpectVersionIncrease() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1)
        .then()
        .statusCode(OK.code())
        .body("properties.@ns:com:here:xyz.version", equalTo(1));
  }

  @Test
  public void getFeatureEqualsToVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?version=0")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("Stade Tata Raphaël"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?version=1")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("updated name"));

//    given()
//        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
//        .when()
//        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?version=222")
//        .then()
//        .statusCode(NOT_FOUND.code()); FIXME should return not found or head?
  }

  @Test
  public void getFeaturesEqualsToVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&version=0")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features.properties.name", hasItem("Stade Tata Raphaël"))
        .body("features.properties.occupant", hasItem("Guangzhou Evergrande Taobao Football Club"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&version=1")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features.properties.name", hasItem("updated name"));

//    given()
//        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
//        .when()
//        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&version=222")
//        .then()
//        .statusCode(OK.code())
//        .body("features.size()", equalTo(0)); FIXME should return not found or head?
  }

  @Test
  public void getFeatureByAuthor() {
    postFeature(SPACE_ID_1_NO_UUID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(FEATURE_ID_1))
        .body("properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_1 +"?author="+USER_2)
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_2 +"?author="+USER_1)
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features/"+ FEATURE_ID_2 +"?author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(FEATURE_ID_2))
        .body("properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesByAuthor() {
    postFeature(SPACE_ID_1_NO_UUID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_1))
        .body("features[0].properties.name", equalTo("updated name"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_2 +"&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesEqualsToVersionAndAuthor() {
    postFeature(SPACE_ID_1_NO_UUID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("name", "second feature")), AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=0&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(3))
        .body("features.id", hasItems(FEATURE_ID_1, FEATURE_ID_2, FEATURE_ID_3))
        .body("features.properties.name", hasItems("Stade Tata Raphaël", "", "Estádio Olímpico do Pará"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=2&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features.id", hasItems(FEATURE_ID_1, FEATURE_ID_3))
        .body("features.properties.name", hasItems("updated name", "Estádio Olímpico do Pará"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=0&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=555&author="+USER_1)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=1&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/"+ SPACE_ID_1_NO_UUID +"/features?id="+ FEATURE_ID_1 +"&id="+FEATURE_ID_2+"&id="+FEATURE_ID_3+"&version=2&author="+USER_2)
        .then()
        .statusCode(OK.code())
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.name", equalTo("second feature"));
  }

  @Test
  public void getFeaturesVersionStarAndAuthor() {
    // TODO
  }

  @Test
  @Ignore // TODO remove ignore
  public void searchFeaturesByPropertyAndAuthor() {
    postFeature(SPACE_ID_1_NO_UUID, new Feature().withId(FEATURE_ID_2).withProperties(new Properties().with("capacity", 58505)),
        AuthProfile.ACCESS_OWNER_2_ALL);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID_1_NO_UUID + "/search?f.version=1&f.author=" + USER_1 + "&p.capacity=58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID_1_NO_UUID + "/search?f.version=2&f.author=" + USER_1 + "&p.capacity=58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID_1_NO_UUID + "/search?f.version=2&f.author=" + USER_2 + "&p.capacity>58500")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID_1_NO_UUID + "/search?f.version=lt=10&f.author=" + USER_2 + "&p.capacity=58505")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get(getSpacesPath() + "/" + SPACE_ID_1_NO_UUID + "/search?f.id=" + FEATURE_ID_2 + "&f.version=lt=10&f.author=" + USER_1 + "&f.author="
            + USER_2 + "&p.capacity<=58505")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58500))
        .body("features[0].properties.@ns:com:here:xyz.version", equalTo(1))
        .body("features[0].id", equalTo(FEATURE_ID_2))
        .body("features[0].properties.capacity", equalTo(58505))
        .body("features[0].properties.@ns:com:here:xyz.version", equalTo(2));
  }

  @Test
  public void deleteFeatureAndReinsertTest() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete(getSpacesPath() + "/" + SPACE_ID_2_UUID + "/features/" + FEATURE_ID_1)
        .then()
        .statusCode(NO_CONTENT.code());

    postFeature(SPACE_ID_2_UUID, new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "updated name 2")), AuthProfile.ACCESS_OWNER_1_ADMIN);

    postFeature(SPACE_ID_2_UUID, new Feature().withId(FEATURE_ID_1).withProperties(new Properties().with("name", "updated name 3")), AuthProfile.ACCESS_OWNER_1_ADMIN);
  }
}
