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
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageReportingApiIT extends TestSpaceWithFeature {
  private static final String LAZY_CONNECTOR = "psql-lazy";
  //A connector without lazyTableCreation in every CI slice
  private static final String EAGER_DB2_CONNECTOR = "psql_db2_hashed";
  private static final String DB2_URL = System.getenv().getOrDefault("STORAGE_DB2_URL", "jdbc:postgresql://localhost/postgres_db2");
  private static final String DB_USER = System.getenv().getOrDefault("STORAGE_DB_USER", "postgres");
  private static final String DB_PASSWORD = System.getenv().getOrDefault("STORAGE_DB_PASSWORD", "password");
  private static Set<String> cleanUpIds = new HashSet<>();
  /**
   * The content bytes reported for a new space on the connector psql. A connector with lazyTableCreation creates the table of a
   * space on its first write, so an empty space has no table and reports 0 bytes.
   */
  private static int emptySpaceContentBytes;

  private static void cleanUp() {
    remove();
    cleanUpIds.forEach(TestWithSpaceCleanup::removeSpace);
  }

  @BeforeClass
  public static void setup() {
    cleanUp();
    Boolean lazyTableCreation = given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/connectors/psql")
        .then()
        .statusCode(OK.code())
        .extract()
        .path("params.lazyTableCreation");
    emptySpaceContentBytes = Boolean.TRUE.equals(lazyTableCreation) ? 0 : 8192;
  }

  @After
  public void tearDown() { cleanUp(); }

  @Test
  public void testEmptyResponsePositive() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L));
  }

  @Test
  public void testMissingPermission() {
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_OWNER_1_ADMIN))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(FORBIDDEN.code());
  }

  @Test
  public void testWithOneEmptySpacePositive() {
    createSpace();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes." + DEFAULT_SPACE_ID + ".contentBytes.value", equalTo(emptySpaceContentBytes));
  }

  @Test
  public void testWithOneFilledSpacePositive() {
    createSpace();
    addFeatures();
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("type", equalTo("StorageStatistics"))
        .body("createdAt", greaterThan(0L))
        .body("byteSizes." + DEFAULT_SPACE_ID + ".contentBytes.value", greaterThan(3000));
  }

  @Test
  public void testWithMultipleStorageIds() {
    cleanUpIds.add(createSpaceWithCustomStorage((DEFAULT_SPACE_ID + "-1"), "psql", null));
    cleanUpIds.add(createSpaceWithCustomStorage(SECOND_SPACE_ID, "psql_db2_hashed", null));
    addFeatures(SECOND_SPACE_ID);
    given()
            .accept(APPLICATION_JSON)
            .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
            .when()
            .get("/admin/statistics/spaces/storage")
            .then()
            .statusCode(OK.code())
            .body("type", equalTo("StorageStatistics"))
            .body("createdAt", greaterThan(0L))
            .body("byteSizes." + (DEFAULT_SPACE_ID + "-1") + ".contentBytes.value", equalTo(emptySpaceContentBytes))
            .body("byteSizes." + (DEFAULT_SPACE_ID + "-1") + ".storageId", equalTo("psql"))
            .body("byteSizes." + SECOND_SPACE_ID + ".contentBytes.value", greaterThan(8192))
            .body("byteSizes." + SECOND_SPACE_ID + ".storageId", equalTo("psql_db2_hashed"));
  }

  @Test
  public void testNeverWrittenLazySpaceHasItsStorageId() {
    String spaceId = createSpaceWithCustomStorage(DEFAULT_SPACE_ID + "-lazy", LAZY_CONNECTOR, null);
    cleanUpIds.add(spaceId);
    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("byteSizes." + spaceId + ".contentBytes.value", equalTo(0))
        .body("byteSizes." + spaceId + ".storageId", equalTo(LAZY_CONNECTOR));
  }

  @Test
  public void testEagerSpaceWithoutTableHasNoStorageId() throws SQLException {
    //The connector reports no storage ID for a space without a table, and the Hub adds one only for lazy spaces
    String spaceId = createSpaceWithCustomStorage(DEFAULT_SPACE_ID + "-no-table", EAGER_DB2_CONNECTOR, null);
    cleanUpIds.add(spaceId);
    String tableName = given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/" + spaceId)
        .then()
        .statusCode(OK.code())
        .body("storage.params.lazyTable", nullValue())
        .extract()
        .path("storage.params.tableName");
    assertNotNull(tableName);
    dropTableSet(DB2_URL, tableName);

    given()
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ADMIN_STATISTICS))
        .when()
        .get("/admin/statistics/spaces/storage")
        .then()
        .statusCode(OK.code())
        .body("byteSizes." + spaceId + ".contentBytes.value", equalTo(0))
        .body("byteSizes." + spaceId + ".storageId", nullValue());
  }

  private static void dropTableSet(String url, String table) throws SQLException {
    String quoted = "\"" + table.replace("\"", "\"\"") + "\"";
    String quotedSequence = "\"" + (table + "_version_seq").replace("\"", "\"\"") + "\"";
    try (Connection connection = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
        Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + quoted + " CASCADE");
      stmt.execute("DROP SEQUENCE IF EXISTS " + quotedSequence);
    }
  }
}
