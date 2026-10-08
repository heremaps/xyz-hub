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
package com.here.xyz.jobs.steps.impl.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.util.test.StepTestBase;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExpressImportSqlIT extends StepTestBase {

  private String schema;
  private Connection connection;
  private Statement statement;

  /**
   * Installs the express import SQL into a schema of its own for every test.
   * <p>
   * The schema is per test on purpose: {@code execute_express_import_batch} is resolved through the search path, so a private schema
   * isolates the functions from the rest of the database and lets every fixture reuse the same simple table names. Installing the
   * script is the only real cost of that.
   * </p>
   * <p>
   * The PostGIS extension is database wide and the statement is idempotent, so after the first test it is only a catalog lookup.
   * </p>
   */
  @BeforeEach
  void installExpressImportSql() throws Exception {
    schema = "express_import_sql_" + Long.toUnsignedString(System.nanoTime());
    connection = getTestDatabaseConnection();
    statement = connection.createStatement();
    statement.execute("CREATE EXTENSION IF NOT EXISTS postgis");
    statement.execute("CREATE SCHEMA " + schema);
    statement.execute("SET search_path TO " + schema + ", public");
    installDependencies(statement);
    try (InputStream stream = getClass().getResourceAsStream("/jobs/transport.sql")) {
      statement.execute(new String(stream.readAllBytes(), UTF_8));
    }
  }

  @AfterEach
  void dropSchema() throws Exception {
    try {
      statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    }
    finally {
      statement.close();
      connection.close();
    }
  }

  @Test
  void importsOversizedAndDuplicateFeaturesWithoutPlv8() throws Exception {

    statement.execute("""
        CREATE TABLE source_features(jsondata TEXT, i BIGSERIAL PRIMARY KEY);
        CREATE TABLE target_features(
            id TEXT NOT NULL,
            version BIGINT NOT NULL,
            next_version BIGINT NOT NULL DEFAULT 9223372036854775807,
            operation CHAR NOT NULL,
            author TEXT,
            jsondata JSONB,
            geo geometry(GeometryZ, 4326),
            UNIQUE(id, next_version),
            PRIMARY KEY(id, version, next_version)
        );
        INSERT INTO target_features(id, version, operation, author, jsondata)
        VALUES ('existing', 1, 'I', 'old',
            '{"type":"Feature","id":"existing","properties":{"@ns:com:here:xyz":{"createdAt":1,"updatedAt":1}}}');
        INSERT INTO source_features(jsondata) VALUES
            ('{"type":"Feature","id":"existing","properties":{"name":"updated"}}'),
            ('{"type":"Feature","id":"new","properties":{"name":"first"}}'),
            ('{"type":"Feature","id":"new","properties":{"name":"last"}}');
        """);

    for (boolean historyEnabled : new boolean[] {false, true}) {
      SQLException exception = assertThrows(SQLException.class, () -> statement.executeQuery("""
          SELECT * FROM execute_express_import_batch(
              'source_features', 'target_features', NULL, 'super', 1, 1, 'owner', 2, %s)
          """.formatted(historyEnabled)));
      assertEquals("XYZ40", exception.getSQLState());
      assertTrue(exception.getMessage().contains("Unsupported space context: SUPER"));
    }
    try (ResultSet result = statement.executeQuery("SELECT count(*) FROM target_features WHERE version = 2")) {
      assertTrue(result.next());
      assertEquals(0, result.getInt(1));
    }

    try (ResultSet result = statement.executeQuery("""
        SELECT * FROM execute_express_import_batch(
            'source_features', 'target_features', NULL, 'DEFAULT', 1, 0.000001, 'owner', 2, false)
        """)) {
      assertTrue(result.next());
      assertEquals(1, result.getInt("pulled_count"));
      assertTrue(result.getLong("pulled_bytes") > 1);
    }

    statement.execute("""
        SELECT * FROM execute_express_import_batch(
            'source_features', 'target_features', NULL, 'DEFAULT', 2, 1, 'owner', 2, false)
        """);
    try (ResultSet result = statement.executeQuery("""
        SELECT count(*) AS row_count,
               max(jsondata#>>'{properties,name}') FILTER (WHERE id = 'new') AS new_name,
               min(operation) FILTER (WHERE id = 'new') AS new_operation
          FROM target_features
        """)) {
      assertTrue(result.next());
      assertEquals(2, result.getInt("row_count"));
      assertEquals("last", result.getString("new_name"));
      assertEquals("I", result.getString("new_operation"));
    }
  }

  @Test
  void preservesCompositeHistoryDeleteOperations() throws Exception {

    statement.execute("""
        CREATE TABLE source_features(jsondata TEXT, i BIGSERIAL PRIMARY KEY);
        CREATE TABLE super_features(
            id TEXT NOT NULL, version BIGINT NOT NULL,
            next_version BIGINT NOT NULL DEFAULT 9223372036854775807,
            operation CHAR NOT NULL, author TEXT, jsondata JSONB,
            geo geometry(GeometryZ, 4326),
            UNIQUE(id, next_version), PRIMARY KEY(id, version, next_version)
        ) PARTITION BY RANGE(next_version);
        CREATE TABLE super_features_default PARTITION OF super_features DEFAULT;
        CREATE TABLE extension_features(LIKE super_features INCLUDING ALL)
            PARTITION BY RANGE(next_version);
        CREATE TABLE extension_features_default PARTITION OF extension_features DEFAULT;

        INSERT INTO super_features(id, version, operation, author, jsondata) VALUES
            ('super-only', 1, 'I', 'old',
                '{"type":"Feature","id":"super-only","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            ('both', 1, 'I', 'old',
                '{"type":"Feature","id":"both","properties":{"@ns:com:here:xyz":{"createdAt":1}}}');
        INSERT INTO extension_features(id, version, operation, author, jsondata) VALUES
            ('both', 1, 'I', 'old',
                '{"type":"Feature","id":"both","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            ('extension-only', 1, 'I', 'old',
                '{"type":"Feature","id":"extension-only","properties":{"@ns:com:here:xyz":{"createdAt":1}}}');
        INSERT INTO source_features(jsondata) VALUES
            ('{"type":"Feature","id":"super-only","properties":{"@ns:com:here:xyz":{"deleted":true}}}'),
            ('{"type":"Feature","id":"both","properties":{"@ns:com:here:xyz":{"deleted":true}}}'),
            ('{"type":"Feature","id":"extension-only","properties":{"@ns:com:here:xyz":{"deleted":true}}}');

        SELECT * FROM execute_express_import_batch(
            'source_features', 'extension_features', 'super_features',
            'DEFAULT', 1, 1, 'owner', 2, true);
        """);

    try (ResultSet result = statement.executeQuery("""
        SELECT
            max(operation) FILTER (WHERE id = 'super-only' AND version = 2) AS super_only_operation,
            max(operation) FILTER (WHERE id = 'both' AND version = 2) AS both_operation,
            max(operation) FILTER (WHERE id = 'extension-only' AND version = 2) AS extension_only_operation
          FROM extension_features
        """)) {
      assertTrue(result.next());
      assertEquals("H", result.getString("super_only_operation"));
      assertEquals("J", result.getString("both_operation"));
      assertEquals("D", result.getString("extension_only_operation"));
    }
  }

  private static Stream<Arguments> boundBaseVersionCases() {
    return Stream.of(
        //Bound to base version 1: the feature added at version 3 is not visible, the one removed at version 3 still is
        Arguments.of(1L, "J", "D", "J"),
        //Unbound, i.e. the base is probed at its HEAD: exactly the opposite for those two
        Arguments.of(null, "J", "J", "D")
    );
  }

  /**
   * A base space bound to a specific version has to be probed at that version rather than at its HEAD. Deleting through the
   * extension classifies as J while the feature is visible in the base and as D once it is not, so the two cases differ exactly
   * for the features whose visibility depends on the bound version.
   */
  @ParameterizedTest
  @MethodSource("boundBaseVersionCases")
  void compositeDeletesHonourABoundBaseVersion(Long superBoundVersion, String expectedVisibleAtBound,
                                               String expectedAddedAfterBound, String expectedRemovedAfterBound) throws Exception {
    statement.execute("""
        CREATE TABLE source_features(jsondata TEXT, i BIGSERIAL PRIMARY KEY);
        CREATE TABLE super_features(
            id TEXT NOT NULL, version BIGINT NOT NULL,
            next_version BIGINT NOT NULL DEFAULT 9223372036854775807,
            operation CHAR NOT NULL, author TEXT, jsondata JSONB,
            geo geometry(GeometryZ, 4326),
            UNIQUE(id, next_version), PRIMARY KEY(id, version, next_version)
        ) PARTITION BY RANGE(next_version);
        CREATE TABLE super_features_default PARTITION OF super_features DEFAULT;
        CREATE TABLE extension_features(LIKE super_features INCLUDING ALL)
            PARTITION BY RANGE(next_version);
        CREATE TABLE extension_features_default PARTITION OF extension_features DEFAULT;

        INSERT INTO super_features(id, version, next_version, operation, author, jsondata) VALUES
            --Live at version 1 and still at HEAD
            ('visible-at-bound', 1, 9223372036854775807, 'I', 'old',
                '{"type":"Feature","id":"visible-at-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            --Only written at version 3, so invisible at version 1
            ('added-after-bound', 3, 9223372036854775807, 'I', 'old',
                '{"type":"Feature","id":"added-after-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            --Live at version 1, deleted at version 3, so invisible at HEAD
            ('removed-after-bound', 1, 3, 'I', 'old',
                '{"type":"Feature","id":"removed-after-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            ('removed-after-bound', 3, 9223372036854775807, 'D', 'old',
                '{"type":"Feature","id":"removed-after-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}');

        --All three are live in the extension, so the classification turns on whether the base still has them
        INSERT INTO extension_features(id, version, operation, author, jsondata) VALUES
            ('visible-at-bound', 1, 'I', 'old',
                '{"type":"Feature","id":"visible-at-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            ('added-after-bound', 1, 'I', 'old',
                '{"type":"Feature","id":"added-after-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}'),
            ('removed-after-bound', 1, 'I', 'old',
                '{"type":"Feature","id":"removed-after-bound","properties":{"@ns:com:here:xyz":{"createdAt":1}}}');

        INSERT INTO source_features(jsondata) VALUES
            ('{"type":"Feature","id":"visible-at-bound","properties":{"@ns:com:here:xyz":{"deleted":true}}}'),
            ('{"type":"Feature","id":"added-after-bound","properties":{"@ns:com:here:xyz":{"deleted":true}}}'),
            ('{"type":"Feature","id":"removed-after-bound","properties":{"@ns:com:here:xyz":{"deleted":true}}}');
        """);

    statement.execute("""
        SELECT * FROM execute_express_import_batch(
            'source_features', 'extension_features', 'super_features',
            'DEFAULT', 1, 1, 'owner', 4, true, %s);
        """.formatted(superBoundVersion == null ? "NULL" : superBoundVersion.toString()));

    try (ResultSet result = statement.executeQuery("""
        SELECT
            max(operation) FILTER (WHERE id = 'visible-at-bound' AND version = 4) AS visible_at_bound,
            max(operation) FILTER (WHERE id = 'added-after-bound' AND version = 4) AS added_after_bound,
            max(operation) FILTER (WHERE id = 'removed-after-bound' AND version = 4) AS removed_after_bound
          FROM extension_features
        """)) {
      assertTrue(result.next());
      String boundDescription = "superBoundVersion=" + superBoundVersion;
      assertEquals(expectedVisibleAtBound, result.getString("visible_at_bound"), boundDescription);
      assertEquals(expectedAddedAfterBound, result.getString("added_after_bound"), boundDescription);
      assertEquals(expectedRemovedAfterBound, result.getString("removed_after_bound"), boundDescription);
    }
  }

  private static void installDependencies(Statement statement) throws Exception {
    statement.execute("""
        CREATE FUNCTION max_bigint() RETURNS BIGINT
            LANGUAGE sql IMMUTABLE AS 'SELECT 9223372036854775807::BIGINT';
        CREATE FUNCTION xyz_random_string(length INT) RETURNS TEXT
            LANGUAGE sql VOLATILE AS 'SELECT substr(md5(random()::TEXT), 1, length)';
        CREATE FUNCTION xyz_reduce_precision(geo geometry, enable_logging BOOLEAN DEFAULT true) RETURNS geometry
            LANGUAGE sql IMMUTABLE AS 'SELECT geo';
        CREATE FUNCTION xyz_create_history_partition(
            schema_name TEXT, table_name TEXT, partition_no BIGINT, partition_size BIGINT) RETURNS VOID
            LANGUAGE plpgsql AS 'BEGIN RETURN; END';
        """);
  }
}
