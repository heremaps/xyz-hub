/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class ModifyFeatureCompositeSpaceIT extends TestCompositeSpace {

  private static Feature newFeature() {
    return new Feature()
        .withId(RandomStringUtils.randomAlphanumeric(3));
  }

  private void postFeature(String spaceId, Feature feature) {
    given()
      .contentType(APPLICATION_GEO_JSON)
      .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
      .body(feature.serialize())
      .when()
      .post("/spaces/" + spaceId + "/features")
      .then()
      .statusCode(OK.code())
      .body("features[0].id", equalTo(feature.getId()));
  }

  @Test
  public void getFromDelta() {
    Feature feature = newFeature();
    postFeature("x-psql-test", feature);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId() + "?context=extension")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void updateOnDelta() {
    Feature feature = newFeature();
    postFeature("x-psql-test", feature.withProperties(new Properties().with("name", "abc")));
    postFeature("x-psql-test-ext", feature.withProperties(new Properties().with("name", "xyz")));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()))
        .body("properties.name", equalTo("abc"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()))
        .body("properties.name", equalTo("xyz"));
  }

  @Test
  public void getOnlyOnDelta() {
    Feature f1 = newFeature();
    Feature f2 = newFeature();
    // FIXME in order to get the extending space to be created, a read or write operation must be executed, otherwise a 504 is returned
    postFeature("x-psql-test", f1);
    postFeature("x-psql-test-ext", f2);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test/features/" + f2.getId())
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + f2.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(f2.getId()));
  }

  @Test
  public void deleteFromDelta() {
    Feature feature = newFeature();
    postFeature("x-psql-test", feature);
    postFeature("x-psql-test-ext", feature.withProperties(new Properties().with("name", "xyz")));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .delete("/spaces/x-psql-test-ext/features/" + feature.getId())
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test/features/" + feature.getId())
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId())
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId() + "?context=extension")
        .then()
        .statusCode(OK.code())
        .body("properties.name", nullValue())
        .body("properties.@ns:com:here:xyz.deleted", equalTo(true));

    // FIXME aren't supposed the delete with context=extension to execute a real deletion (e.g. removing the deleted flagged feature?)
//    given()
//        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
//        .when()
//        .delete("/spaces/x-psql-test-ext/features/" + feature.getId() + "?context=extension")
//        .then()
//        .statusCode(NO_CONTENT.code());
//
//    given()
//        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
//        .when()
//        .get("/spaces/x-psql-test-ext/features/" + feature.getId() + "?context=extension")
//        .then()
//        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getOnDeltaOfDelta() {
    Feature feature = newFeature();
    postFeature("x-psql-test", feature.withProperties(new Properties().with("name", "a").with("level", "base")));
    postFeature("x-psql-test-ext", feature.withProperties(new Properties().with("name", "b").with("size", "m")));
    postFeature("x-psql-test-ext-ext", feature.withProperties(new Properties().with("name", "c").with("height", "2m")));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()))
        .body("properties.name", equalTo("a"))
        .body("properties.level", equalTo("base"))
        .body("properties.size", nullValue())
        .body("properties.height", nullValue());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()))
        .body("properties.name", equalTo("b"))
        .body("properties.level", equalTo("base"))
        .body("properties.size", equalTo("m"))
        .body("properties.height", nullValue());


    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext-ext/features/" + feature.getId())
        .then()
        .statusCode(OK.code())
        .body("id", equalTo(feature.getId()))
        .body("properties.name", equalTo("c"))
        .body("properties.level", equalTo("base"))
        .body("properties.size", equalTo("m"))
        .body("properties.height", equalTo("2m"));
  }

  @Test
  public void bboxOnDelta() {
    Feature f1 = newFeature();
    Feature f2 = newFeature();
    postFeature("x-psql-test", f1.withGeometry(new Point().withCoordinates(new PointCoordinates(1,1))));
    postFeature("x-psql-test", f2.withGeometry(new Point().withCoordinates(new PointCoordinates(-30,-30))));
    postFeature("x-psql-test-ext", f1.withGeometry(new Point().withCoordinates(new PointCoordinates(-1,-1))));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/bbox?west=0&south=-1&east=1&north=0")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/bbox?west=-35&south=-35&east=35&north=35")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/bbox?west=-35&south=-35&east=35&north=35&context=extension")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(f1.getId()));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/bbox?west=-10&south=-10&east=10&north=10")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(f1.getId()));
  }

  @Test
  public void tileOnDelta() {
    Feature f1 = newFeature();
    Feature f2 = newFeature();
    postFeature("x-psql-test", f1.withGeometry(new Point().withCoordinates(new PointCoordinates(1,1))));
    postFeature("x-psql-test", f2.withGeometry(new Point().withCoordinates(new PointCoordinates(-30,-30))));
    postFeature("x-psql-test-ext", f1.withGeometry(new Point().withCoordinates(new PointCoordinates(-1,-1))));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/tile/quadkey/0")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/tile/quadkey/2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(2));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-ext/tile/quadkey/2111")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(1))
        .body("features[0].id", equalTo(f1.getId()));
  }
}
