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

public class ChangesetApiIT extends TestSpaceWithFeature {

  private static String cleanUpSpaceId = "space1";
  protected static final String AUTHOR_1 = "XYZ-01234567-89ab-cdef-0123-456789aUSER1";

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
    addChangesets();
  }

  @After
  public void teardown() {
    removeAll();
  }

  private void addChangesets() {
    //Version 0
    FeatureCollection changeset0 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("A").withProperties(new Properties().with("name", "A1")),
                    new Feature().withId("B").withProperties(new Properties().with("name", "B1"))
            )
    );

    //Version 1
    FeatureCollection changeset1 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("A").withProperties(new Properties().with("name", "A2")),
                    new Feature().withId("C").withProperties(new Properties().with("name", "C1"))
            )
    );

    //Version 2
    FeatureCollection changeset2 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("D").withProperties(new Properties().with("name", "D1")),
                    new Feature().withId("E").withProperties(new Properties().with("name", "E1"))
            )
    );

    //Version 3
    FeatureCollection changeset3 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("B").withProperties(new Properties().with("name", "B2"))
            )
    );

    //Version 4
    FeatureCollection changeset4 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("A").withProperties(new Properties().with("name", "A3"))
            )
    );

    //Version 5
    FeatureCollection changeset5 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("D").withProperties(new Properties().with("name", "D2"))
            )
    );

    //Version 6
    FeatureCollection changeset6 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("B").withProperties(new Properties().with("name", "B3"))
            )
    );

    //Version 7
    FeatureCollection changeset7 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("E").withProperties(new Properties().with("name", "E2"))
            )
    );

    //Version 8
    FeatureCollection changeset8 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("F").withProperties(new Properties().with("name", "F1"))
            )
    );

    //Version 9 is a deletion (see below)

    //Version 10
    FeatureCollection changeset10 = new FeatureCollection().withFeatures(
            Arrays.asList(
                    new Feature().withId("F").withProperties(new Properties().with("name", "F2"))
            )
    );

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset0.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset1.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset2.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset3.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset4.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset5.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset6.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset7.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());
    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset8.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());

    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .delete("/spaces/" + cleanUpSpaceId + "/features/A")
            .then()
            .statusCode(NO_CONTENT.code());

    given()
            .contentType(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .body(changeset10.toString())
            .post("/spaces/" + cleanUpSpaceId + "/features")
            .then()
            .statusCode(OK.code());
  }

  private void addChangesetsMultipleTimes(int times) {
    for (int i=0; i<times; i++) {
      int rnd1 = (int) (Math.random()*10);
      int rnd2 = (int) (Math.random()*10);
      FeatureCollection changeset = new FeatureCollection().withFeatures(
          new ArrayList<Feature>() {{
            new Feature().withId("F" + rnd1).withProperties(new Properties().with("name", rnd1 + "" + rnd2));
            new Feature().withId("F" + rnd2).withProperties(new Properties().with("name", rnd2 + "" + rnd1));
          }}
      );

      given()
          .contentType(APPLICATION_JSON)
          .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
          .body(changeset.toString())
          .post("/spaces/" + cleanUpSpaceId + "/features")
          .then()
          .statusCode(OK.code());
    }
  }

  @Test
  public void deleteChangesets() throws InterruptedException {given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features/B?version=1")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features/B")
        .then()
        .statusCode(OK.code())
        .body("properties.name", equalTo("B3"));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<2")
        .then()
        .statusCode(NO_CONTENT.code());

    Thread.sleep(3_000);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features/B?version=1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void deleteChangesetsMaxValue() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<4")
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features?id=A,B,C,D&version=4")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features/A?version=1")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void deleteChangesetsNegativeValue() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<-1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void deleteChangesetsLargerThanHeadValue() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<12")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<4")
        .then()
        .statusCode(NO_CONTENT.code());
  }

  @Test
  public void deleteChangesetsMultipleTimes() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version=lt=3")
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version=lt=3")
        .then()
        .statusCode(BAD_REQUEST.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features?id=F1,F2&version=2")
        .then()
        .statusCode(OK.code())
        .body("features.size()", equalTo(0));
  }

  @Test
  public void getChangesetWithGeometryNull() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=1")
        .then()
        .statusCode(OK.code())
        .body("versions.1.inserted.features[0]", hasKey("geometry"))
        .body("versions.1.inserted.features[0].geometry", nullValue());
  }

  @Test
  public void deleteAndGetMultipleChangesets() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<4")
        .then()
        .statusCode(NO_CONTENT.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=0&endVersion=999")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void pageThroughChangesetCollectionVersion0To1() {
    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=2&limit=2")
            .then()
            .statusCode(OK.code())
            .body("startVersion", equalTo(1))
            .body("endVersion", equalTo(1))
            .body("versions.1.inserted.features.size()", equalTo(2))
            .body("2", nullValue())
            .body("nextPageToken", notNullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=2&limit=1&pageToken=1_B")
            .then()
            .statusCode(OK.code())
            .body("startVersion", equalTo(2))
            .body("endVersion", equalTo(2))
            .body("versions.1", nullValue())
            .body("versions.2.updated.features.size()", equalTo(1))
            .body("nextPageToken", notNullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=0&endVersion=1&limit=1&pageToken=1_A")
            .then()
            .statusCode(OK.code())
            .body("startVersion", equalTo(1))
            .body("endVersion", equalTo(1))
            .body("versions.1.inserted.features.size()", equalTo(1))
            .body("versions.1.updated.features.size()", equalTo(0))
            .body("nextPageToken", nullValue());

  }

  @Test
  public void pageThroughChangesetVersion0To1() {
    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/1?limit=1")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(1))
            .body("inserted.features.size()", equalTo(1))
            .body("nextPageToken", notNullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/2?limit=2")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(2))
            .body("inserted.features.size()", equalTo(1))
            .body("updated.features.size()", equalTo(1))
            .body("nextPageToken", nullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/3?limit=1")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(3))
            .body("inserted.features.size()", equalTo(1))
            .body("nextPageToken", notNullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/3?pageToken=3_D&limit=1")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(3))
            .body("inserted.features.size()", equalTo(1))
            .body("nextPageToken", nullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/4?limit=10")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(4))
            .body("updated.features.size()", equalTo(1))
            .body("nextPageToken", nullValue());
  }

  @Test
  public void validateChangesetVersions() {
    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/1")
        .prettyPeek()
        .then()
            .statusCode(OK.code())
            .body("version", equalTo(1))
            .body("inserted.features.size()", equalTo(2))
            .body("updated.features.size()", equalTo(0))
            .body("deleted.features.size()", equalTo(0))
            .body("author", equalTo(AUTHOR_1))
            .body("createdAt", notNullValue())
            .body("nextPageToken", nullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/2")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(2))
            .body("inserted.features.size()", equalTo(1))
            .body("updated.features.size()", equalTo(1))
            .body("deleted.features.size()", equalTo(0))
            .body("nextPageToken", nullValue());

    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets/10")
            .then()
            .statusCode(OK.code())
            .body("version", equalTo(10))
            .body("inserted.features.size()", equalTo(0))
            .body("updated.features.size()", equalTo(0))
            .body("deleted.features.size()", equalTo(1))
            .body("nextPageToken", nullValue());
  }

  @Test
  public void validateCompleteChangesetCollection() {
    given()
            .get("/spaces/" + cleanUpSpaceId + "/changesets?startVersion=1&endVersion=12")
            .then()
            .statusCode(OK.code())
            .body("startVersion", equalTo(1))
            .body("endVersion", equalTo(11))

            .body("versions.1.inserted.features.size()", equalTo(2))
            .body("versions.1.updated.features.size()", equalTo(0))
            .body("versions.1.deleted.features.size()", equalTo(0))
            .body("versions.1.author", equalTo(AUTHOR_1))
            .body("versions.1.createdAt", notNullValue())
            .body("versions.1.version", equalTo(1))

            .body("versions.2.inserted.features.size()", equalTo(1))
            .body("versions.2.updated.features.size()", equalTo(1))
            .body("versions.2.deleted.features.size()", equalTo(0))
            .body("versions.2.version", equalTo(2))

            .body("versions.3.inserted.features.size()", equalTo(2))
            .body("versions.3.updated.features.size()", equalTo(0))
            .body("versions.3.deleted.features.size()", equalTo(0))
            .body("versions.3.version", equalTo(3))

            .body("versions.4.inserted.features.size()", equalTo(0))
            .body("versions.4.updated.features.size()", equalTo(1))
            .body("versions.4.deleted.features.size()", equalTo(0))
            .body("versions.4.version", equalTo(4))

            .body("versions.5.inserted.features.size()", equalTo(0))
            .body("versions.5.updated.features.size()", equalTo(1))
            .body("versions.5.deleted.features.size()", equalTo(0))
            .body("versions.5.version", equalTo(5))

            .body("versions.6.inserted.features.size()", equalTo(0))
            .body("versions.6.updated.features.size()", equalTo(1))
            .body("versions.6.deleted.features.size()", equalTo(0))
            .body("versions.6.version", equalTo(6))

            .body("versions.7.inserted.features.size()", equalTo(0))
            .body("versions.7.updated.features.size()", equalTo(1))
            .body("versions.7.deleted.features.size()", equalTo(0))
            .body("versions.7.version", equalTo(7))

            .body("versions.8.inserted.features.size()", equalTo(0))
            .body("versions.8.updated.features.size()", equalTo(1))
            .body("versions.8.deleted.features.size()", equalTo(0))
            .body("versions.8.version", equalTo(8))

            .body("versions.9.inserted.features.size()", equalTo(1))
            .body("versions.9.updated.features.size()", equalTo(0))
            .body("versions.9.deleted.features.size()", equalTo(0))
            .body("versions.9.version", equalTo(9))

            .body("versions.10.inserted.features.size()", equalTo(0))
            .body("versions.10.updated.features.size()", equalTo(0))
            .body("versions.10.deleted.features.size()", equalTo(1))
            .body("versions.10.version", equalTo(10))

            .body("versions.11.inserted.features.size()", equalTo(0))
            .body("versions.11.updated.features.size()", equalTo(1))
            .body("versions.11.deleted.features.size()", equalTo(0))
            .body("versions.11.version", equalTo(11))

            .body("nextPageToken", nullValue());
  }
}
