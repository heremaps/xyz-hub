/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReadSpaceApiIT extends TestSpaceWithFeature {

  private static final String MY_DEMO_SPACE = "My Demo Space";
  private static final String SHARED = "shared";
  private static final String OTHER = "other";
  private static Set<String> cleanUpIds = new HashSet<>();


  @BeforeClass
  public static void setupClass() {
    remove();
  }

  @Before
  public void setup() {
    createSpace();
  }

  @After
  public void tearDown() {
    remove();
    cleanUpIds.forEach(TestSpaceWithFeature::removeSpace);
  }

  @Test
  public void readSpaceAdmin() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test"))
        .body("title", equalTo("My Demo Space"))
        .body("storage.id", equalTo("psql"));
  }

  @Test
  public void readSpaceNoAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  //FIXME: Remove that test (and the added workaround response type application/geo+json in the openapi_src.yaml) once CMEKB-2572 has been fixed (see: CMEKB-2637)
  @Test
  public void readSpaceWithGeoJsonWorkaround() {
    given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test"));
  }

  @Test
  public void readSpaceNoAdmin() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("x-psql-test"))
        .body("title", equalTo("My Demo Space"))
        .body("$", not(hasKey("storage")))
        .body("$", not(hasKey("tileMinLevel")))
        .body("$", not(hasKey("tileMaxLevel")))
        .body("$", not(hasKey("insertBBox")));
  }

  @Test
  public void readSpacesNoAdmin() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].id", equalTo("x-psql-test"))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].$", not(hasKey("rights")))
        .body("[0].$", not(hasKey("storage")))
        .body("[0].$", not(hasKey("tileMinLevel")))
        .body("[0].$", not(hasKey("tileMaxLevel")))
        .body("[0].$", not(hasKey("insertBBox")));
  }

  @Test
  public void readSpacesNoAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
        .when()
        .get("/spaces")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void readSpacesOthersNoAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void readSpacesOwner2NoAccess() throws Exception {
    cleanUpIds.add(given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .body(content("/xyz/hub/createSpaceWithoutId.json"))
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code())
        .extract()
        .body()
        .path("id"));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
        .when()
        .get("/spaces?owner=XYZ-01234567-89ab-cdef-0123-456789aUSER2")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void readSpacesStarNoAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.NO_ACCESS))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void readSpacesWithRightsNoAdmin() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces?includeRights=true")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].id", equalTo("x-psql-test"))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].rights.size()", is(5))
        .body("[0].$", not(hasKey("storage")))
        .body("[0].$", not(hasKey("tileMinLevel")))
        .body("[0].$", not(hasKey("tileMaxLevel")))
        .body("[0].$", not(hasKey("insertBBox")));
  }

  @Test
  public void readSpacesCheckForCreatedAtUpdatedAt() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].id", equalTo("x-psql-test"))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].createdAt", instanceOf(Number.class))
        .body("[0].updatedAt", instanceOf(Number.class));
  }

  @Test
  public void readSpacesWithStorageId() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?owner=*&includeConnectors=true")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].storage.id", equalTo("psql"));
  }

  @Test
  public void readSpacesWithStorageIdNoAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces?owner=*&includeConnectors=true")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].storage", nullValue());
  }

  @Test
  public void readSpacesWithStorageIdWithAccessDontInclude() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("size()", is(1))
        .body("[0].title", equalTo("My Demo Space"))
        .body("[0].storage", nullValue());
  }

  @Test
  public void readSpaceNonExisting() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/spaces/x-psql-test-dummy")
        .then()
        .statusCode(NOT_FOUND.code());
  }

  @Test
  public void readSpaceWithOtherOwner() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void readSpaceWithAllAccess() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void readSharedSpaceWithOtherOwner() {
    //Publish space from OWNER_1
    publishSpace("x-psql-test");

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void readSpaceWithAccessConnectors() {
    addListener("x-psql-test");

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_PSQL))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("$", hasKey("listeners"))
        .body("listeners.listener-test", notNullValue())
        .body("listeners.listener-test.size()", equalTo(1));
  }

  @Test
  public void readSpaceWithNoAccessConnectors() {
    addListener("x-psql-test");

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_ONLY))
        .when()
        .get("/spaces/x-psql-test")
        .then()
        .statusCode(OK.code())
        .body("$", not(hasKey("listeners")));
  }

  @Test
  public void readSpacesWithOwnerStar() {
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, SHARED, true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, OTHER, false));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(3))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_MANAGE_ALL_SPACES_ONLY))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(3))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_MANAGE_ALL_SPACES))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(3))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2_WITH_FEATURES_ADMIN_ALL_SPACES))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(3))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_3))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(SHARED));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_ALL_FEATURES))
        .when()
        .get("/spaces?owner=*")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(3))
        .body("title", hasItems(MY_DEMO_SPACE, SHARED, OTHER));
  }

  @Test
  public void readSpacesWithOwnerMe() {
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "other", false));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_MANAGE_ALL_SPACES_ONLY))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_MANAGE_ALL_SPACES))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2_WITH_FEATURES_ADMIN_ALL_SPACES))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_3))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(0));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_ALL_FEATURES))
        .when()
        .get("/spaces?owner=me")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));
  }

  @Test
  public void readSpacesWithOwnerOthers() {
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "shared", true));
    cleanUpIds.add(createSpace(AuthProfile.ACCESS_OWNER_2, "other", false));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_NO_ADMIN))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(SHARED));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(0));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_MANAGE_ALL_SPACES_ONLY))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_WITH_FEATURES_MANAGE_ALL_SPACES))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_2_WITH_FEATURES_ADMIN_ALL_SPACES))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(MY_DEMO_SPACE));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_3))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(1))
        .body("title", hasItems(SHARED));

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_READ_ALL_FEATURES))
        .when()
        .get("/spaces?owner=others")
        .then()
        .statusCode(OK.code())
        .body("$", hasSize(2))
        .body("title", hasItems(SHARED, OTHER));
  }
}
