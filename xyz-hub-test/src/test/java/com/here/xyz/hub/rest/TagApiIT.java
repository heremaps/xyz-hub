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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Tag;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TagApiIT extends TestSpaceWithFeature {
  private static final String SECOND_SPACE = "secondSpace";
  private static final List<String> createdSpaces = new ArrayList<>();

  @BeforeClass
  public static void setupClass() {
      removeSpace(getSpaceId());
      createdSpaces.forEach(TagApiIT::removeSpace);
  }

  @Before
  public void setup() {
      createSpaceWithVersionsToKeep(getSpaceId(), 2);
  }

  @After
  public void teardown() {
      removeSpace(getSpaceId());
      removeSpace(SECOND_SPACE);
      createdSpaces.forEach(TagApiIT::removeSpace);
      _deleteSubscription();
  }

  private ValidatableResponse _createTag() {
      return _createTagForId(getSpaceId(), "XYZ_1", false);
  }

  private ValidatableResponse _deleteSubscription() {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId() + "/subscriptions/test-subscription-1")
        .then();
  }

  private ValidatableResponse _createTagForId(String spaceId, String tagId, boolean system) {
      return given()
          .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
          .contentType(ContentType.JSON)
          .body(new Tag().withId(tagId).withSystem(system))
          .post("/spaces/" + spaceId + "/tags")
          .then().statusCode(OK.code());
  }

  private void _addOneFeature() {
    addFeature(getSpaceId(), new Feature().withProperties(new Properties().with("name", "abc")));
  }

  @Test
  public void testDeleteSpace() {
      _createTag();
      removeSpace(getSpaceId());
      createSpace();

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(NOT_FOUND.code());
  }

  @Test
  public void createTag() {
      _createTag()
              .statusCode(OK.code())
              .body("id", equalTo("XYZ_1"))
              .body("version", equalTo(0));

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(OK.code());
  }

  @Test
  public void createTagStartingNumericNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("1_NUMBER"))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then().statusCode(BAD_REQUEST.code());
  }

  @Test
  public void createTagWithIdStarNegative() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("*"))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then().statusCode(BAD_REQUEST.code());
  }

    @Test
  public void deleteTag() {
      _createTag();

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(OK.code());

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .delete("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(NOT_FOUND.code());
  }

  @Test
  public void getTagVersion() {
      _createTag();

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(OK.code())
              .body("version", equalTo(0));
  }

  @Test
  public void updateTagVersion() {
      _createTag();

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .contentType(ContentType.JSON)
              .body(new Tag().withVersion(999).serialize())
              .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(OK.code());

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
              .then()
              .statusCode(OK.code())
              .body("version", equalTo(999));
  }

  @Test
  public void testSubscriptions() {
      given()
              .accept(APPLICATION_JSON)
              .contentType(APPLICATION_JSON)
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .body(content("/xyz/hub/createSubscription.json"))
              .post("/spaces/" + getSpaceId() + "/subscriptions");

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
              .then()
              .statusCode(OK.code());

      given()
              .accept(APPLICATION_JSON)
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .delete("/spaces/" + getSpaceId() + "/subscriptions/test-subscription-1")
              .then()
              .statusCode(OK.code());
  }

  @Test
  public void testRemoveSubscriptions() {
      given()
              .accept(APPLICATION_JSON)
              .contentType(APPLICATION_JSON)
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .body(content("/xyz/hub/createSubscription.json"))
              .post("/spaces/" + getSpaceId() + "/subscriptions");

      given()
              .accept(APPLICATION_JSON)
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .delete("/spaces/" + getSpaceId() + "/subscriptions/test-subscription-1")
              .then()
              .statusCode(OK.code());

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
              .then()
              .statusCode(NOT_FOUND.code());
  }

  @Test
  public void testListSpacesFilterByTagId() {
    createSpaceWithVersionsToKeep(SECOND_SPACE, 2);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces?tag=XYZ_1")
        .then()
        .body("size()", is(0));

    _createTag();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces?tag=XYZ_1")
        .then()
        .body("size()", is(1))
        .body( "[0].tags.XYZ_1.id", equalTo("XYZ_1"))
        .body( "[0].tags.XYZ_1.version", equalTo(0));
  }

  @Ignore("Disabled. Takes too long")
  @Test
  public void testBigListSpacesFilterByTagId() {
      for (int i = 0; i < 250; i++)
       createdSpaces.add(createSpaceWithRandomId());

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces?tag=XYZ_1")
              .then()
              .body("size()", is(0));

      createdSpaces.stream().forEach(spaceId -> _createTagForId(spaceId, "XYZ_1", false));

      given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .get("/spaces?tag=XYZ_1")
              .then()
              .body("size()", is(createdSpaces.size()));
  }

  @Test
  public void testUpdateTagWithVersionNotSet() {
    _createTag();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testCreateTagWithoutVersion() {
    _addOneFeature();
    _addOneFeature();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1"))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(2));
  }

  @Test
  public void testCreateTagWithVersion() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(555))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(555));
  }

  @Test
  public void testCreateTagWithVersionMinusTen() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(-10))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testCreateTagWithVersionMinusOneOnEmptySpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(-1))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(-1));
  }

  @Test
  public void testCreateTagWithVersionMinusOneOnSpaceWithData() {
    _addOneFeature();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(-1))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(-1));
  }

  @Test
  public void testCreateTagWithVersionZeroOnEmptySpace() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(0))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(0));
  }

  @Test
  public void testCreateTagWithVersionZeroOnSpaceWithData() {
    _addOneFeature();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withId("XYZ_1").withVersion(0))
        .post("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("version", equalTo(0));
  }

  @Test
  public void testUpdateTagWithVersionMinusTen() {
    _createTag();

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(ContentType.JSON)
        .body(new Tag().withVersion(-10))
        .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(BAD_REQUEST.code());
  }

  @Test
  public void testListSpacesFilterByContentUpdatedAtAndTagIdAndRegion() {
    createSpaceWithVersionsToKeep(SECOND_SPACE, 2);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces?contentUpdatedAt=gt=1&tag=XYZ_1&region=invalid_region")
        .then()
        .body("size()", is(0))
        .statusCode(OK.code());
  }

  @Test
  public void testGetSystemTag() {
    _createTagForId(getSpaceId(), "XYZ_2", true);
    _createTagForId(getSpaceId(), "XYZ_1", false);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("XYZ_1"))
        .body("version", equalTo(0))
        .body("$", not(hasKey("system")));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/XYZ_2")
        .then()
        .statusCode(OK.code())
        .body("id", equalTo("XYZ_2"))
        .body("version", equalTo(0))
        .body("$", hasKey("system"))
        .body("system", equalTo(true));
  }

  @Test
  public void testListEmptyTags() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("size()", is(0))
        .statusCode(OK.code());
  }

  @Test
  public void testListTags() {
    _createTagForId(getSpaceId(), "XYZ_2", true);
    _createTagForId(getSpaceId(), "XYZ_1", false);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("size()", is(1))
        .body("id", hasItems("XYZ_1"))
        .statusCode(OK.code());
  }

  @Test
  public void testListSystemTags() {
    _createTagForId(getSpaceId(), "XYZ_2", true);
    _createTagForId(getSpaceId(), "XYZ_1", true);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags")
        .then()
        .body("size()", is(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags?includeSystemTags=false")
        .then()
        .body("size()", is(0));

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags?includeSystemTags=true")
        .then()
        .body("size()", is(2))
        .body("id", hasItems("XYZ_1", "XYZ_2"))
        .statusCode(OK.code());
  }

  @Test
  public void testRecreateSpaceWithSubscription() {
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
        .then()
        .statusCode(NOT_FOUND.code());

    given()
        .contentType(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(content("/xyz/hub/createSubscription.json"))
        .post("/spaces/" + getSpaceId() + "/subscriptions");

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
        .then()
        .statusCode(OK.code());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .delete("/spaces/" + getSpaceId());

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
        .then()
        .statusCode(NOT_FOUND.code());

    createSpaceWithVersionsToKeep(getSpaceId(), 2);

    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .get("/spaces/" + getSpaceId() + "/tags/xyz_ntf")
        .then()
        .statusCode(OK.code());
  }

  @Test
  public void createTagAndCheckForAuthorAndCreatedAtAndDescription() {
    _createTag()
            .statusCode(OK.code())
            .body("id", equalTo("XYZ_1"))
            .body("version", equalTo(0));

    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .get("/spaces/" + getSpaceId() + "/tags/XYZ_1")
            .then()
            .statusCode(OK.code())
            .body("$", hasKey("author"))
            .body("$", hasKey("createdAt"))
            .body("$", hasKey("description"));
  }

  @Test
  public void testUpdateTagWithDescription() {
    _createTag();

    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .contentType(ContentType.JSON)
            .body(new Tag().withId("XYZ_1").withVersion(1).withDescription("description"))
            .patch("/spaces/" + getSpaceId() + "/tags/XYZ_1")
            .then()
            .statusCode(OK.code())
            .body("description", equalTo("description"));
  }

  @Test
  public void testListTagsSortedDesc() throws InterruptedException {
    _createTagForId(getSpaceId(), "XYZ_1", false);
    Thread.sleep(10);
    _createTagForId(getSpaceId(), "XYZ_2", false);

    given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .get("/spaces/" + getSpaceId() + "/tags")
            .then()
            .statusCode(OK.code())
            .body("size()", is(2))
            .body("id[0]", equalTo("XYZ_2"))
            .body("id[1]", equalTo("XYZ_1"));
  }
}
