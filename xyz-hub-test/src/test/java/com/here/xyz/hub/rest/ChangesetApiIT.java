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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.jayway.restassured.RestAssured.given;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.equalTo;

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
        .get("/spaces/" + cleanUpSpaceId + "/features/B?version=0")
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
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<1")
        .then()
        .statusCode(NO_CONTENT.code());

    Thread.sleep(3_000);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + cleanUpSpaceId + "/features/B?version=0")
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
        .delete("/spaces/" + cleanUpSpaceId + "/changesets?version<11")
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
}
