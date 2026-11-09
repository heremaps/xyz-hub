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

import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_ADMIN_LAZY_TABLE;
import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_ALL;
import static com.here.xyz.hub.auth.TestAuthenticator.AuthProfile.ACCESS_OWNER_1_ADMIN;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.here.xyz.util.db.pg.LockHelper;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Spaces of a connector with lazyTableCreation: the Hub marks a new space with storage.params.lazyTable, the connector creates
 * its table on the first write, and the Hub removes the flag after that write.
 */
public class LazySpaceApiIT extends TestSpaceWithFeature {

  private static final String LAZY_CONNECTOR = "psql-lazy";
  //Another database than the one of psql-lazy
  private static final String LAZY_DB2_CONNECTOR = "psql-db2-lazy";
  private static final String EAGER_CONNECTOR = "psql";

  private static final String DB_URL = System.getenv().getOrDefault("STORAGE_DB_URL", "jdbc:postgresql://localhost/postgres");
  private static final String DB2_URL = System.getenv().getOrDefault("STORAGE_DB2_URL", "jdbc:postgresql://localhost/postgres_db2");
  private static final String DB_USER = System.getenv().getOrDefault("STORAGE_DB_USER", "postgres");
  private static final String DB_PASSWORD = System.getenv().getOrDefault("STORAGE_DB_PASSWORD", "password");

  //The Hub removes the flag asynchronously, after the response of the first write
  private static final Duration CLEAR_TIMEOUT = Duration.ofSeconds(10);
  //Longer than the throttle of the contentUpdatedAt store of the Hub (Space.CONTENT_UPDATED_AT_INTERVAL_MILLIS)
  private static final long CONTENT_UPDATED_AT_WAIT_MILLIS = 31_000;

  private static final String RUN_ID = RandomStringUtils.randomAlphanumeric(6).toLowerCase(Locale.ROOT);
  private static final List<String> createdSpaces = new ArrayList<>();
  //Tables left on the first database by a space that was switched to another connector without moving its table
  private static final List<String> orphanedTables = new ArrayList<>();

  private static boolean eagerConnectorIsLazy;

  //Created in setup(), so that the wait of the contentUpdatedAt cases overlaps with the other tests
  private static String contentUpdatedAtSpaceId;
  private static long contentUpdatedAtSpaceCreated;
  private static long contentUpdatedAtBeforeWrite;
  private static String failedWriteSpaceId;
  private static long failedWriteSpaceCreated;
  private static long failedWriteContentUpdatedAtBefore;

  @BeforeClass
  public static void setup() {
    Boolean lazyTableCreation = given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .get("/connectors/" + EAGER_CONNECTOR)
        .then()
        .statusCode(OK.code())
        .extract()
        .path("params.lazyTableCreation");
    eagerConnectorIsLazy = Boolean.TRUE.equals(lazyTableCreation);

    contentUpdatedAtSpaceId = createLazySpace("cua");
    contentUpdatedAtSpaceCreated = System.currentTimeMillis();
    contentUpdatedAtBeforeWrite = contentUpdatedAt(contentUpdatedAtSpaceId);
    failedWriteSpaceId = createLazySpace("failed");
    failedWriteSpaceCreated = System.currentTimeMillis();
    failedWriteContentUpdatedAtBefore = contentUpdatedAt(failedWriteSpaceId);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    //Extensions were created after their bases, so they are removed first
    for (int i = createdSpaces.size() - 1; i >= 0; i--)
      removeSpace(createdSpaces.get(i));
    for (String table : orphanedTables)
      dropTableSet(DB_URL, table);
  }

  @Test
  public void createIsConfigOnly() throws SQLException {
    String spaceId = createLazySpace("create");
    String table = tableName(spaceId);

    assertNotNull(table);
    assertTrue(isFlagged(spaceId));
    assertEquals(0, relationCount(DB_URL, table));
  }

  @Test
  public void readsAnswerLikeAnEagerEmptySpace() throws SQLException {
    String eagerSpaceId = createSpaceOn("eager", EAGER_CONNECTOR, null);
    String lazySpaceId = createLazySpace("reads");

    for (String path : List.of("/search", "/search?p.key1=value1", "/iterate", "/bbox?west=-10&south=-10&east=10&north=10",
        "/tile/quadkey/120", "/spatial?lat=0&lon=0&radius=1000", "/features?id=a&id=b", "/statistics")) {
      Response eager = get(eagerSpaceId, path);
      Response lazy = get(lazySpaceId, path);
      assertEquals(path, eager.statusCode(), lazy.statusCode());
      assertEquals(path, OK.code(), lazy.statusCode());
      assertEquals(path, comparable(eager), comparable(lazy));
    }
    assertEquals(get(eagerSpaceId, "/features/a").statusCode(), get(lazySpaceId, "/features/a").statusCode());

    assertTrue(isFlagged(lazySpaceId));
    assertEquals("A read creates no table", 0, relationCount(DB_URL, tableName(lazySpaceId)));
  }

  @Test
  public void firstWriteMaterializes() throws SQLException {
    String spaceId = createLazySpace("materialize");
    String table = tableName(spaceId);
    assertEquals(0, relationCount(DB_URL, table));

    write(spaceId, "F1", "").statusCode(OK.code());

    assertTrue(hasRelation(DB_URL, table));
    get(spaceId, "/features/F1").then().statusCode(OK.code()).body("id", equalTo("F1"));
    countFeatures(spaceId, 1);
  }

  @Test
  public void firstWriteClearsFlag() {
    String spaceId = createLazySpace("clear");
    String table = tableName(spaceId);

    write(spaceId, "F1", "").statusCode(OK.code());
    awaitFlagCleared(spaceId);

    assertEquals("The clear keeps the table name", table, tableName(spaceId));
    write(spaceId, "F2", "").statusCode(OK.code());
    get(spaceId, "/features?id=F1&id=F2").then().statusCode(OK.code()).body("features.size()", equalTo(2));
    assertFalse(isFlagged(spaceId));
  }

  @Test
  public void writeResponseHasNoMaterializedField() {
    String spaceId = createLazySpace("response");

    write(spaceId, "F1", "")
        .statusCode(OK.code())
        .body("features[0].id", equalTo("F1"))
        .body("$", not(hasKey("materialized")));
  }

  @Test
  public void heldLockTimesOutAndTheRetrySucceeds() throws SQLException {
    String spaceId = createLazySpace("lock");
    String table = tableName(spaceId);

    try (Connection lockConnection = connect(DB_URL)) {
      //The session lock that a space creation or deletion holds on the table
      LockHelper.advisoryLock(table, lockConnection);
      try {
        long start = System.currentTimeMillis();
        write(spaceId, "F1", "")
            .statusCode(GATEWAY_TIMEOUT.code())
            .body("code", equalTo("E318540"));
        long duration = System.currentTimeMillis() - start;
        //The materialization budget of the test connector is 3 s
        assertTrue("The write must time out after the budget, took " + duration + " ms", duration >= 2_500 && duration < 15_000);
      }
      finally {
        LockHelper.advisoryUnlock(table, lockConnection);
      }
    }
    assertEquals(0, relationCount(DB_URL, table));
    assertTrue(isFlagged(spaceId));

    write(spaceId, "F1", "").statusCode(OK.code());
    assertTrue(hasRelation(DB_URL, table));
    awaitFlagCleared(spaceId);
  }

  @Test
  public void firstWriteToLazyExtensionCreatesOnDemandIndexesOfTheBase() throws SQLException {
    String baseId = createSpaceOn("idxbase", LAZY_CONNECTOR,
        new JsonObject().put("searchableProperties", new JsonObject().put("foo", true)));
    String extensionId = createExtension("idxext", baseId);
    String baseTable = tableName(baseId);
    String extensionTable = tableName(extensionId);
    assertTrue(isFlagged(extensionId));

    write(extensionId, "F1", "").statusCode(OK.code());

    assertTrue(hasRelation(DB_URL, extensionTable));
    assertTrue("The extension's table has the index of the base's searchable property foo",
        indexCount(DB_URL, extensionTable, "foo") > 0);
    assertEquals("The never written base has no table", 0, relationCount(DB_URL, baseTable));
    awaitFlagCleared(extensionId);
    assertTrue(isFlagged(baseId));
  }

  @Test
  public void superWriteIsRejectedAndClearsNothing() throws SQLException {
    String baseId = createLazySpace("superbase");
    String extensionId = createExtension("superext", baseId);
    assertTrue(isFlagged(extensionId));

    //The feature API does not accept writes with context SUPER, so neither level is written
    write(extensionId, "F1", "?context=SUPER")
        .statusCode(FORBIDDEN.code())
        .body("code", equalTo("E318431"));

    assertTrue(isFlagged(baseId));
    assertTrue(isFlagged(extensionId));
    assertEquals(0, relationCount(DB_URL, tableName(baseId)));
    assertEquals(0, relationCount(DB_URL, tableName(extensionId)));
  }

  @Test
  public void lostTableAfterClearIs404() throws SQLException {
    String spaceId = createLazySpace("lost");
    String table = tableName(spaceId);
    write(spaceId, "F1", "").statusCode(OK.code());
    awaitFlagCleared(spaceId);

    dropTableSet(DB_URL, table);
    assertEquals(0, relationCount(DB_URL, table));

    get(spaceId, "/search").then()
        .statusCode(NOT_FOUND.code())
        .body("errorMessage", equalTo("Space not found in backend"));
    write(spaceId, "F2", "")
        .statusCode(NOT_FOUND.code())
        .body("errorMessage", equalTo("Space not found in backend"));
    assertEquals("Nothing creates the table again", 0, relationCount(DB_URL, table));
  }

  @Test
  public void switchNeverWrittenLazySpace() throws SQLException {
    String spaceId = createLazySpace("switchempty");
    String table = tableName(spaceId);

    patchSpace(spaceId, new JsonObject().put("storage", new JsonObject().put("id", LAZY_DB2_CONNECTOR)));
    assertEquals(table, tableName(spaceId));
    assertTrue(isFlagged(spaceId));

    get(spaceId, "/search").then().statusCode(OK.code()).body("features.size()", equalTo(0));
    write(spaceId, "F1", "").statusCode(OK.code());

    assertTrue("The first write creates the table on the new database", hasRelation(DB2_URL, table));
    assertEquals(0, relationCount(DB_URL, table));
    get(spaceId, "/features/F1").then().statusCode(OK.code());
    awaitFlagCleared(spaceId);
  }

  @Test
  public void switchWrittenLazySpaceWithoutMove() throws SQLException {
    String spaceId = createLazySpace("switchwritten");
    String table = tableName(spaceId);
    write(spaceId, "F1", "").statusCode(OK.code());
    awaitFlagCleared(spaceId);
    orphanedTables.add(table);

    patchSpace(spaceId, new JsonObject().put("storage", new JsonObject().put("id", LAZY_DB2_CONNECTOR)));

    //A space without the flag is a normal space, its table was not moved
    get(spaceId, "/search").then().statusCode(NOT_FOUND.code());
    assertEquals(0, relationCount(DB2_URL, table));
  }

  @Test
  public void extensionMovedAwayFromLazyBase() throws SQLException {
    String baseId = createLazySpace("movedbase");
    write(baseId, "F1", "").statusCode(OK.code());
    String extensionId = createExtension("movedext", baseId);
    assertTrue(isFlagged(extensionId));

    given()
        .headers(getAuthHeaders(ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .body(new JsonObject().put("storage", new JsonObject().put("id", LAZY_DB2_CONNECTOR)).encode())
        .when()
        .patch("/spaces/" + extensionId + "?forceStorage=true")
        .then()
        .statusCode(OK.code());

    //The base is on another storage now, so its level is not read as empty
    get(extensionId, "/search").then().statusCode(NOT_FOUND.code());
    assertEquals(0, relationCount(DB2_URL, tableName(baseId)));
  }

  @Test
  public void clientLazyTableOnEagerConnectorCreatesTable() throws SQLException {
    assumeFalse("Needs a connector without lazyTableCreation", eagerConnectorIsLazy);
    String spaceId = createSpaceOn("client", EAGER_CONNECTOR, null, new JsonObject().put("lazyTable", true));

    assertFalse("The Hub never takes the flag from the client", isFlagged(spaceId));
    assertTrue(hasRelation(DB_URL, tableName(spaceId)));
  }

  @Test
  public void adminClearLazyTable() {
    String spaceId = createLazySpace("admin");
    String table = tableName(spaceId);

    clearLazyTable(spaceId, null, "wrong-" + LAZY_CONNECTOR, table).statusCode(CONFLICT.code());
    assertTrue(isFlagged(spaceId));
    //The legacy form, for a space without a table name, does not match a space that has one
    clearLazyTable(spaceId, null, LAZY_CONNECTOR, null).statusCode(CONFLICT.code());
    assertTrue(isFlagged(spaceId));
    clearLazyTable(spaceId, null, null, table).statusCode(BAD_REQUEST.code());
    //The route needs the admin capability lazyTable
    clearLazyTable(spaceId, ACCESS_OWNER_1_ADMIN, LAZY_CONNECTOR, table).statusCode(FORBIDDEN.code());
    assertTrue(isFlagged(spaceId));

    clearLazyTable(spaceId, ACCESS_ADMIN_LAZY_TABLE, LAZY_CONNECTOR, table).statusCode(NO_CONTENT.code());
    assertFalse(isFlagged(spaceId));
    assertEquals(table, tableName(spaceId));
    clearLazyTable(spaceId, ACCESS_ADMIN_LAZY_TABLE, LAZY_CONNECTOR, table).statusCode(NO_CONTENT.code());
    assertFalse(isFlagged(spaceId));

    //Without a token the test Hub's anonymous access applies, as for the calls of the job steps
    String secondSpaceId = createLazySpace("admin2");
    clearLazyTable(secondSpaceId, null, LAZY_CONNECTOR, tableName(secondSpaceId)).statusCode(NO_CONTENT.code());
    assertFalse(isFlagged(secondSpaceId));
  }

  @Test
  public void contentUpdatedAtStoreKeepsFlagCleared() throws InterruptedException {
    String spaceId = contentUpdatedAtSpaceId;
    sleepUntil(contentUpdatedAtSpaceCreated + CONTENT_UPDATED_AT_WAIT_MILLIS);

    write(spaceId, "F1", "").statusCode(OK.code());

    awaitSpace(() -> !isFlagged(spaceId) && contentUpdatedAt(spaceId) > contentUpdatedAtBeforeWrite);
    Thread.sleep(3_000);
    assertFalse("The deferred contentUpdatedAt store must not bring the flag back", isFlagged(spaceId));
  }

  @Test
  public void failedWriteStoresContentUpdatedAt() throws SQLException, InterruptedException {
    String spaceId = failedWriteSpaceId;
    String table = tableName(spaceId);
    sleepUntil(failedWriteSpaceCreated + CONTENT_UPDATED_AT_WAIT_MILLIS);

    //The write reaches the connector, which answers with an error, because the materialization cannot get the lock
    try (Connection lockConnection = connect(DB_URL)) {
      LockHelper.advisoryLock(table, lockConnection);
      try {
        write(spaceId, "F1", "").statusCode(GATEWAY_TIMEOUT.code());
      }
      finally {
        LockHelper.advisoryUnlock(table, lockConnection);
      }
    }

    awaitSpace(() -> contentUpdatedAt(spaceId) > failedWriteContentUpdatedAtBefore);
    assertTrue("A failed write keeps the flag", isFlagged(spaceId));
  }

  private static String createLazySpace(String name) {
    String spaceId = createSpaceOn(name, LAZY_CONNECTOR, null);
    assertTrue("A new space of " + LAZY_CONNECTOR + " has the flag", isFlagged(spaceId));
    return spaceId;
  }

  private static String createSpaceOn(String name, String storageId, JsonObject properties) {
    return createSpaceOn(name, storageId, properties, null);
  }

  private static String createSpaceOn(String name, String storageId, JsonObject properties, JsonObject storageParams) {
    String spaceId = "x-lazy-" + name + "-" + RUN_ID + TEST_SUFFIX;
    JsonObject storage = new JsonObject().put("id", storageId);
    if (storageParams != null)
      storage.put("params", storageParams);
    JsonObject space = new JsonObject()
        .put("id", spaceId)
        .put("title", "Lazy space test " + name)
        .put("storage", storage);
    if (properties != null)
      space.mergeIn(properties);
    return post(space);
  }

  private static String createExtension(String name, String baseId) {
    String spaceId = "x-lazy-" + name + "-" + RUN_ID + TEST_SUFFIX;
    return post(new JsonObject()
        .put("id", spaceId)
        .put("title", "Lazy space test " + name)
        .put("extends", new JsonObject().put("spaceId", baseId)));
  }

  private static String post(JsonObject space) {
    String spaceId = space.getString("id");
    createdSpaces.add(spaceId);
    given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .body(space.encode())
        .when()
        .post("/spaces")
        .then()
        .statusCode(OK.code());
    return spaceId;
  }

  private static ValidatableResponse write(String spaceId, String featureId, String query) {
    return given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .body(wrapFeature(newFeature(featureId)).serialize())
        .when()
        .post("/spaces/" + spaceId + "/features" + query)
        .then();
  }

  private static Response get(String spaceId, String path) {
    return given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .get("/spaces/" + spaceId + path);
  }

  /**
   * @return The response body without the values that differ between two spaces of the same content
   */
  private static JsonObject comparable(Response response) {
    JsonObject body = new JsonObject(response.asString());
    body.remove("contentUpdatedAt");
    return body;
  }

  private static ValidatableResponse clearLazyTable(String spaceId, AuthProfile authProfile, String connector, String table) {
    RequestSpecification request = given();
    if (authProfile != null)
      request.headers(getAuthHeaders(authProfile));
    if (connector != null)
      request.queryParam("connector", connector);
    if (table != null)
      request.queryParam("table", table);
    return request
        .when()
        .delete("/admin/spaces/" + spaceId + "/lazyTable")
        .then();
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

  private static boolean isFlagged(String spaceId) {
    Map<String, Object> params = storageParams(spaceId);
    return params != null && Boolean.TRUE.equals(params.get("lazyTable"));
  }

  private static String tableName(String spaceId) {
    Map<String, Object> params = storageParams(spaceId);
    return params == null ? null : (String) params.get("tableName");
  }

  private static long contentUpdatedAt(String spaceId) {
    Number contentUpdatedAt = given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(ACCESS_ALL))
        .when()
        .get("/spaces/" + spaceId)
        .then()
        .statusCode(OK.code())
        .extract()
        .path("contentUpdatedAt");
    return contentUpdatedAt.longValue();
  }

  private static void awaitFlagCleared(String spaceId) {
    awaitSpace(() -> !isFlagged(spaceId));
  }

  private static void awaitSpace(Callable<Boolean> condition) {
    await().atMost(CLEAR_TIMEOUT).pollInterval(Duration.ofMillis(200)).until(condition);
  }

  private static void sleepUntil(long timestamp) throws InterruptedException {
    long remaining = timestamp - System.currentTimeMillis();
    if (remaining > 0)
      Thread.sleep(remaining);
  }

  private static Connection connect(String url) throws SQLException {
    return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
  }

  private static boolean hasRelation(String url, String name) throws SQLException {
    return count(url, "SELECT count(*) FROM pg_class WHERE relname = ?", name) > 0;
  }

  /**
   * @return The number of relations whose name starts with the table name: the table, its partitions, sequences and indexes
   */
  private static int relationCount(String url, String table) throws SQLException {
    return count(url, "SELECT count(*) FROM pg_class WHERE starts_with(relname, ?)", table);
  }

  private static int indexCount(String url, String table, String property) throws SQLException {
    return count(url, "SELECT count(*) FROM pg_indexes WHERE tablename = ? AND indexdef LIKE ?", table, "%" + property + "%");
  }

  private static int count(String url, String query, String... values) throws SQLException {
    try (Connection connection = connect(url); PreparedStatement stmt = connection.prepareStatement(query)) {
      for (int i = 0; i < values.length; i++)
        stmt.setString(i + 1, values[i]);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static void dropTableSet(String url, String table) throws SQLException {
    String quoted = "\"" + table.replace("\"", "\"\"") + "\"";
    String quotedSequence = "\"" + (table + "_version_seq").replace("\"", "\"\"") + "\"";
    try (Connection connection = connect(url); Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + quoted + " CASCADE");
      stmt.execute("DROP SEQUENCE IF EXISTS " + quotedSequence);
    }
  }
}
