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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SingleChangesetApiIT extends TestSpaceWithFeature {

  private static String cleanUpSpaceId = "space1";
  protected static final String AUTHOR_1 = "XYZ-01234567-89ab-cdef-0123-456789aUSER1";
  protected static final String AUTHOR_2 = "XYZ-01234567-89ab-cdef-0123-456789aUSER2";

  @BeforeClass
  public static void setupClass() {
    removeAll();
  }

  private static void removeAll() {
    removeSpace(cleanUpSpaceId);
  }

  @Before
  public void setup() {
    createSpaceWithCustomStorage(cleanUpSpaceId, "psql", null, 10);
  }

  @After
  public void teardown() {
    removeAll();
  }

  private void postFeatureCollection(AuthProfile authProfile, FeatureCollection featureCollection) {
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(authProfile))
            .body(featureCollection.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());
  }

  private void deleteFeature(AuthProfile authProfile, String featureId) {
    given()
        .headers(getAuthHeaders(authProfile))
        .delete("/spaces/" + cleanUpSpaceId + "/features?id=" + featureId)
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void testAuthorOneChangeset() {
    FeatureCollection featureCollection = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A1")),
            new Feature().withId("B").withProperties(new Properties().with("name", "B1"))
        )
    );

    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection);

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=1")
            .then()
            .statusCode(OK.code())
            .body("startVersion", equalTo(1))
            .body("endVersion", equalTo(1))

            .body("versions.1.inserted.features.size()", equalTo(2))
            .body("versions.1.updated.features.size()", equalTo(0))
            .body("versions.1.deleted.features.size()", equalTo(0))
            .body("versions.1.author", equalTo(AUTHOR_1))
            .body("versions.1.createdAt", notNullValue())

            .body("nextPageToken", nullValue());
  }

  @Test
  public void testAuthorTwoChangesets() {
    FeatureCollection featureCollection1 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A1")),
            new Feature().withId("B").withProperties(new Properties().with("name", "B1"))
        )
    );

    FeatureCollection featureCollection2 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("B").withProperties(new Properties().with("name", "B2")),
            new Feature().withId("C").withProperties(new Properties().with("name", "C1"))
        )
    );

    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection1);
    postFeatureCollection(AuthProfile.ACCESS_OWNER_2_ALL, featureCollection2);

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=2")
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(2))

        .body("versions.1.inserted.features.size()", equalTo(2))
        .body("versions.1.updated.features.size()", equalTo(0))
        .body("versions.1.deleted.features.size()", equalTo(0))
        .body("versions.1.author", equalTo(AUTHOR_1))
        .body("versions.1.createdAt", notNullValue())

        .body("versions.2.inserted.features.size()", equalTo(1))
        .body("versions.2.updated.features.size()", equalTo(1))
        .body("versions.2.deleted.features.size()", equalTo(0))
        .body("versions.2.author", equalTo(AUTHOR_2))
        .body("versions.2.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void testAuthorThreeChangesets() {
    FeatureCollection featureCollection1 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A1")),
            new Feature().withId("B").withProperties(new Properties().with("name", "B1"))
        )
    );

    FeatureCollection featureCollection2 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("B").withProperties(new Properties().with("name", "B2")),
            new Feature().withId("C").withProperties(new Properties().with("name", "C1"))
        )
    );

    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection1);
    postFeatureCollection(AuthProfile.ACCESS_OWNER_2_ALL, featureCollection2);
    deleteFeature(AuthProfile.ACCESS_OWNER_1_ADMIN, "A");

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=999")
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(3))

        .body("versions.1.inserted.features.size()", equalTo(2))
        .body("versions.1.updated.features.size()", equalTo(0))
        .body("versions.1.deleted.features.size()", equalTo(0))
        .body("versions.1.author", equalTo(AUTHOR_1))
        .body("versions.1.createdAt", notNullValue())

        .body("versions.2.inserted.features.size()", equalTo(1))
        .body("versions.2.updated.features.size()", equalTo(1))
        .body("versions.2.deleted.features.size()", equalTo(0))
        .body("versions.2.author", equalTo(AUTHOR_2))
        .body("versions.2.createdAt", notNullValue())

        .body("versions.3.inserted.features.size()", equalTo(0))
        .body("versions.3.updated.features.size()", equalTo(0))
        .body("versions.3.deleted.features.size()", equalTo(1))
        .body("versions.3.author", equalTo(AUTHOR_1))
        .body("versions.3.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }
}
