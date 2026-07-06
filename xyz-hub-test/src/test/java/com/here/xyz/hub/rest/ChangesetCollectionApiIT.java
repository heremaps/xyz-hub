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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ChangesetCollectionApiIT extends TestSpaceWithFeature {

  protected static final String AUTHOR_1 = "XYZ-01234567-89ab-cdef-0123-456789aUSER1";
  private static String cleanUpSpaceId = "space1";

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

  private void addChangeSets() {
    //Changeset 0 is the empty space

    //Changeset 1
    FeatureCollection featureCollection1 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A1")),
            new Feature().withId("B").withProperties(new Properties().with("name", "B1"))
        )
    );

    //Changeset 2
    FeatureCollection featureCollection2 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A2")),
            new Feature().withId("C").withProperties(new Properties().with("name", "C1"))
        )
    );

    //Changeset 3
    FeatureCollection featureCollection3 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("B").withProperties(new Properties().with("name", "B2")),
            new Feature().withId("C").withProperties(new Properties().with("name", "C2"))
        )
    );

    //Changeset 4
    FeatureCollection featureCollection4 = new FeatureCollection().withFeatures(
        Arrays.asList(
            new Feature().withId("A").withProperties(new Properties().with("name", "A3")),
            new Feature().withId("C").withProperties(new Properties().with("name", "C3"))
        )
    );
    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection1);
    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection2);
    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection3);
    postFeatureCollection(AuthProfile.ACCESS_OWNER_1_ADMIN, featureCollection4);
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

  @Test
  public void validateAllCollection() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets")
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(4))

        .body("versions.1.inserted.features.size()", equalTo(2))
        .body("versions.1.updated.features.size()", equalTo(0))
        .body("versions.1.deleted.features.size()", equalTo(0))
        .body("versions.1.author", equalTo(AUTHOR_1))
        .body("versions.1.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCollectionWithVersionParams() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=0&endVersion=4")
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(4))

        .body("versions.1.inserted.features.size()", equalTo(2))
        .body("versions.1.updated.features.size()", equalTo(0))
        .body("versions.1.deleted.features.size()", equalTo(0))
        .body("versions.1.author", equalTo(AUTHOR_1))
        .body("versions.1.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCollectionWithStartVersionParam() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=2")
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(2))
        .body("endVersion", equalTo(4))

        .body("versions.2.inserted.features.size()", equalTo(1))
        .body("versions.2.updated.features.size()", equalTo(1))
        .body("versions.2.deleted.features.size()", equalTo(0))
        .body("versions.2.author", equalTo(AUTHOR_1))
        .body("versions.2.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCollectionWithEndVersionParam() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?endVersion=2")
        .then()
        .log().all()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(2))

        .body("versions.2.inserted.features.size()", equalTo(1))
        .body("versions.2.updated.features.size()", equalTo(1))
        .body("versions.2.deleted.features.size()", equalTo(0))
        .body("versions.2.author", equalTo(AUTHOR_1))
        .body("versions.2.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCollectionWhenNoChangetsetPresent() {
    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void validateCollectionFilterByAuthor() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?author=" + AUTHOR_1)
        .then()
        .statusCode(OK.code())
        .body("startVersion", equalTo(1))
        .body("endVersion", equalTo(4))

        .body("versions.1.inserted.features.size()", equalTo(2))
        .body("versions.1.updated.features.size()", equalTo(0))
        .body("versions.1.deleted.features.size()", equalTo(0))
        .body("versions.1.author", equalTo(AUTHOR_1))
        .body("versions.1.createdAt", notNullValue())

        .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCollectionFilterByInvalidAuthor() {
    addChangeSets();

    given()
        .get("/spaces/" + cleanUpSpaceId + "/changesets?author=INVALID_USER")
        .then()
        .statusCode(NOT_FOUND.code());
  }
}
