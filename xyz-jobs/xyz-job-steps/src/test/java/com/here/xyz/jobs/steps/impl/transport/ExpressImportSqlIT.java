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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class ExpressImportSqlIT {

  @Test
  void importsOversizedAndDuplicateFeaturesWithoutPlv8() throws Exception {
    String schema = "express_import_" + Long.toUnsignedString(System.nanoTime());
    try (Connection connection = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/postgres", "postgres", "password");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE EXTENSION IF NOT EXISTS postgis");
      statement.execute("CREATE SCHEMA " + schema);
      statement.execute("SET search_path TO " + schema + ", public");
      installDependencies(statement);
      try (InputStream stream = getClass().getResourceAsStream("/jobs/transport.sql")) {
        statement.execute(new String(stream.readAllBytes(), UTF_8));
      }

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
    finally {
      try (Connection connection = DriverManager.getConnection(
          "jdbc:postgresql://localhost:5432/postgres", "postgres", "password");
          Statement statement = connection.createStatement()) {
        statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void preservesCompositeHistoryDeleteOperations() throws Exception {
    String schema = "express_composite_" + Long.toUnsignedString(System.nanoTime());
    try (Connection connection = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/postgres", "postgres", "password");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE EXTENSION IF NOT EXISTS postgis");
      statement.execute("CREATE SCHEMA " + schema);
      statement.execute("SET search_path TO " + schema + ", public");
      installDependencies(statement);
      try (InputStream stream = getClass().getResourceAsStream("/jobs/transport.sql")) {
        statement.execute(new String(stream.readAllBytes(), UTF_8));
      }

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
    finally {
      try (Connection connection = DriverManager.getConnection(
          "jdbc:postgresql://localhost:5432/postgres", "postgres", "password");
          Statement statement = connection.createStatement()) {
        statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
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
