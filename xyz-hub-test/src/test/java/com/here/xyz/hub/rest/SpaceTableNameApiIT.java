/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_ALL;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * The physical table name of a space ({@code storage.params.tableName}) is assigned by the Hub on creation and can not be
 * set or changed by a request.
 */
public class SpaceTableNameApiIT extends TestSpaceWithFeature {

  private static final String CONNECTOR = "psql";
  private static final String GENERATED_NAME = "[0-9a-f]{32}";

  private static final String RUN_ID = RandomStringUtils.randomAlphanumeric(6).toLowerCase(Locale.ROOT);
  private static final List<String> createdSpaces = new ArrayList<>();

  @AfterClass
  public static void tearDown() {
    //Extensions were created after their bases, so they are removed first
    for (int i = createdSpaces.size() - 1; i >= 0; i--)
      removeSpace(createdSpaces.get(i));
  }

  @Test
  public void createIgnoresTheClientTableName() {
    String clientTableName = "client_" + RUN_ID;
    String spaceId = spaceId("create");
    Map<String, Object> responseParams = post(space(spaceId, CONNECTOR, new JsonObject().put("tableName", clientTableName)))
        .extract()
        .path("storage.params");

    String tableName = tableName(spaceId);
    assertGenerated(tableName);
    assertNotEquals(clientTableName, tableName);
    assertEquals(tableName, responseParams.get("tableName"));
    assertEquals(Boolean.TRUE, storageParams(spaceId).get("lazyTable"));
  }

  @Test
  public void patchCannotChangeTheTableName() {
    String spaceId = createSpace("patch", CONNECTOR);
    String tableName = tableName(spaceId);

    String responseTableName = patch(spaceId, storage(CONNECTOR, new JsonObject().put("tableName", "other_" + RUN_ID)))
        .extract()
        .path("storage.params.tableName");

    assertEquals(tableName, responseTableName);
    assertEquals(tableName, tableName(spaceId));
  }

  @Test
  public void patchCannotRemoveTheTableName() {
    String spaceId = createSpace("remove", CONNECTOR);
    String tableName = tableName(spaceId);

    patch(spaceId, storage(CONNECTOR, new JsonObject().putNull("tableName")));
    assertEquals(tableName, tableName(spaceId));

    patch(spaceId, new JsonObject().put("storage", new JsonObject().put("id", CONNECTOR).putNull("params")));
    assertEquals(tableName, tableName(spaceId));
  }

  @Test
  public void patchCannotChangeTheLazyFlagOrTheTableName() {
    String spaceId = createSpace("patchlazy", CONNECTOR);
    String tableName = tableName(spaceId);
    assertEquals(Boolean.TRUE, storageParams(spaceId).get("lazyTable"));

    patch(spaceId, storage(CONNECTOR, new JsonObject().put("tableName", "other_" + RUN_ID).put("lazyTable", false)));

    Map<String, Object> params = storageParams(spaceId);
    assertEquals(tableName, params.get("tableName"));
    assertEquals(Boolean.TRUE, params.get("lazyTable"));
  }

  @Test
  public void patchWithReadOnlyAndATableNameKeepsTheTableName() {
    String spaceId = createSpace("readonly", CONNECTOR);
    String tableName = tableName(spaceId);

    patch(spaceId, storage(CONNECTOR, new JsonObject().put("tableName", "other_" + RUN_ID)).put("readOnly", true));

    assertEquals(tableName, tableName(spaceId));
  }

  @Test
  public void patchOfAnExtensionIgnoresTheTableName() {
    String baseId = createSpace("base", CONNECTOR);
    String extensionId = spaceId("ext");
    post(new JsonObject()
        .put("id", extensionId)
        .put("title", "Table name test ext")
        .put("extends", new JsonObject().put("spaceId", baseId)));
    String tableName = tableName(extensionId);
    assertGenerated(tableName);
    assertNotEquals(tableName(baseId), tableName);

    patch(extensionId, storage(CONNECTOR, new JsonObject().put("tableName", "other_" + RUN_ID)));

    assertEquals(tableName, tableName(extensionId));
  }

  private static String spaceId(String name) {
    return "x-tn-" + name + "-" + RUN_ID + TEST_SUFFIX;
  }

  private static String createSpace(String name, String storageId) {
    String spaceId = spaceId(name);
    post(space(spaceId, storageId, null));
    return spaceId;
  }

  private static JsonObject space(String spaceId, String storageId, JsonObject storageParams) {
    return storage(storageId, storageParams)
        .put("id", spaceId)
        .put("title", "Table name test " + spaceId);
  }

  private static JsonObject storage(String storageId, JsonObject storageParams) {
    JsonObject storage = new JsonObject().put("id", storageId);
    if (storageParams != null)
      storage.put("params", storageParams);
    return new JsonObject().put("storage", storage);
  }

  private static ValidatableResponse post(JsonObject space) {
    createdSpaces.add(space.getString("id"));
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .body(space.encode())
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());
  }

  private static ValidatableResponse patch(String spaceId, JsonObject body) {
    return given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .body(body.encode())
        .when()
        .patch("/spaces/" + spaceId)
        .then()
        .statusCode(OK.code());
  }

  private static Map<String, Object> storageParams(String spaceId) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .get("/spaces/" + spaceId)
        .then()
        .statusCode(OK.code())
        .extract()
        .path("storage.params");
  }

  private static String tableName(String spaceId) {
    Map<String, Object> params = storageParams(spaceId);
    return params == null ? null : (String) params.get("tableName");
  }

  private static void assertGenerated(String tableName) {
    assertNotNull(tableName);
    assertTrue("Not a generated table name: " + tableName, tableName.matches(GENERATED_NAME));
  }
}
