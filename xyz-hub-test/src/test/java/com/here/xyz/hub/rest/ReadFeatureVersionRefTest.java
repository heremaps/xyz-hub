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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadFeatureVersionRefTest extends TestSpaceWithFeature {

  private void createTag() {
    given()
        .contentType(APPLICATION_JSON)
        .body("{\"id\":\"tag1\"}")
        .when()
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .statusCode(200);
  }
  @Before
  public void setup() {
    removeSpace(getSpaceId());
    createSpaceWithVersionsToKeep(getSpaceId(), 2);
    addFeature(getSpaceId(), new Feature().withId("F1").withProperties(new Properties().with("a", 1)).withGeometry(new Point().withCoordinates(new PointCoordinates(1, 1))));
    createTag();
    addFeature(getSpaceId(), new Feature().withId("F1").withProperties(new Properties().with("b", 2)).withGeometry(new Point().withCoordinates(new PointCoordinates(2, 2))));
  }

  @After
  public void tearDown() {
    removeSpace(getSpaceId());
  }

  @Test
  public void getFeatureByIdVersionRefNotFound() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features/F1?versionRef=HEAD1")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features/F1?versionRef=head")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features/F1?versionRef=notExistingTag")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void getFeaturesByIdsAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features?id=F1&versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features?id=F1&versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void getFeatureByIdAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features/F1?versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("properties.a", equalTo(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/features/F1?versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("properties.a", equalTo(1))
        .body("properties.b", equalTo(2));
  }

  @Test
  public void getFeaturesByBboxAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/bbox?west=-10&north=10&east=10&south=-10&versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/bbox?west=-10&north=10&east=10&south=-10&versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void getFeaturesByTileAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tile/quadkey/1?versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tile/quadkey/1?versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void getFeaturesBySpatialAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/spatial?lat=0&lon=0&radius=1000000&versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/spatial?lat=0&lon=0&radius=1000000&versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void postFeaturesBySpatialAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"type\":\"Point\",\"coordinates\":[0,0]}")
        .post("/spaces/" + getSpaceId() + "/spatial?radius=1000000&versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body("{\"type\":\"Point\",\"coordinates\":[0,0]}")
        .post("/spaces/" + getSpaceId() + "/spatial?radius=1000000&versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void getFeaturesBySearchAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/search?versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/search?versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }

  @Test
  public void getFeaturesByIterateAndVersionRef() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/iterate?versionRef=tag1")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/iterate?versionRef=HEAD")
        .then()
        .statusCode(OK.code())
        .body("features.properties.a", hasItem(1))
        .body("features.properties.b", hasItem(2));
  }
}
